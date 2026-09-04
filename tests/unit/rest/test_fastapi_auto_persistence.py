from __future__ import annotations

import pytest

from loom.core.backend import sqlalchemy as sqlalchemy_backend
from loom.core.config import ConfigContext
from loom.core.config.errors import ConfigError
from loom.core.di.container import LoomContainer
from loom.core.discovery.base import DiscoveryResult
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.dynamodb.uow import DynamoUnitOfWorkFactory
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from loom.rest.fastapi import auto
from loom.rest.fastapi.auto import (
    _AppConfig,
    _build_bootstrap,
    _discover_components,
    _load_persistence_config,
    _noop_lifespan,
    _resolve_persistence,
)


class PersistenceNoneRecord(BaseModel):
    __tablename__ = "persistence_none_records_fixture"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=50)


def _ctx(**sections: object) -> ConfigContext:
    return ConfigContext.from_dict(
        {
            "app": {"name": "demo"},
            "database": {"url": "sqlite+aiosqlite:///"},
            **sections,
        }
    )


def _no_models() -> DiscoveryResult:
    return DiscoveryResult(models=(), use_cases=(), interfaces=())


def _resolve(ctx: ConfigContext, discovered: DiscoveryResult) -> auto._PersistenceWiring:
    """Resolve persistence the way ``_build_bootstrap`` does: load config once."""
    return _resolve_persistence(ctx, _load_persistence_config(ctx), discovered)


def test_resolve_persistence_defaults_to_sqlalchemy() -> None:
    wiring = _resolve(_ctx(), _no_models())

    assert isinstance(wiring.uow_factory, SQLAlchemyUnitOfWorkFactory)
    assert callable(wiring.repo_registration_module)
    assert callable(wiring.lifespan_init)


def test_resolve_persistence_honours_explicit_sqlalchemy_backend() -> None:
    wiring = _resolve(
        _ctx(persistence={"backend": "sqlalchemy"}),
        _no_models(),
    )

    assert isinstance(wiring.uow_factory, SQLAlchemyUnitOfWorkFactory)


def test_resolve_persistence_rejects_unknown_backend() -> None:
    ctx = _ctx(persistence={"backend": "mongodb"})
    models = _no_models()
    with pytest.raises(ValueError, match="Unsupported persistence backend: 'mongodb'"):
        _resolve(ctx, models)


def test_resolve_persistence_selects_dynamodb_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Dummy credentials satisfy boto3's default chain against a local endpoint;
    # real credentials come from the task role on ECS.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    wiring = _resolve(
        _ctx(
            persistence={
                "backend": "dynamodb",
                "dynamodb": {
                    "region": "eu-west-1",
                    "table": "products",
                    "endpoint_url": "http://localhost:8000",
                },
            }
        ),
        _no_models(),
    )

    assert isinstance(wiring.uow_factory, DynamoUnitOfWorkFactory)
    assert callable(wiring.repo_registration_module)
    assert callable(wiring.lifespan_init)


def test_resolve_persistence_dynamodb_needs_no_database_section(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    ctx = ConfigContext.from_dict(
        {
            "app": {"name": "demo"},
            "persistence": {
                "backend": "dynamodb",
                "dynamodb": {"region": "eu-west-1", "table": "products"},
            },
        }
    )

    wiring = _resolve(ctx, _no_models())

    assert isinstance(wiring.uow_factory, DynamoUnitOfWorkFactory)


def test_resolve_persistence_dynamodb_requires_config_section() -> None:
    ctx = _ctx(persistence={"backend": "dynamodb"})
    models = _no_models()
    with pytest.raises(ConfigError, match="persistence.dynamodb"):
        _resolve(ctx, models)


def _agents_only() -> DiscoveryResult:
    return DiscoveryResult(models=(), use_cases=(), interfaces=(), agent_specs=("agents/*.yaml",))


def test_resolve_persistence_none_has_no_persistence_at_all() -> None:
    wiring = _resolve(_ctx(persistence={"backend": "none"}), _no_models())

    assert wiring.uow_factory is None
    assert wiring.lifespan_init is _noop_lifespan


def test_resolve_persistence_none_repo_module_registers_nothing() -> None:
    wiring = _resolve(_ctx(persistence={"backend": "none"}), _no_models())
    container = LoomContainer()
    before = dict(vars(container))

    assert wiring.repo_registration_module(container) is None
    assert vars(container) == before


def test_resolve_persistence_none_ignores_database_section() -> None:
    ctx = ConfigContext.from_dict({"app": {"name": "demo"}, "persistence": {"backend": "none"}})

    wiring = _resolve(ctx, _no_models())

    assert wiring.uow_factory is None


def test_discover_components_accepts_agents_only_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(auto, "_build_discovery_result", lambda _cfg: _agents_only())

    discovered = _discover_components(_AppConfig(name="demo"))

    assert discovered.agent_specs == ("agents/*.yaml",)


def test_discover_components_rejects_empty_result(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(auto, "_build_discovery_result", lambda _cfg: _no_models())

    app_cfg = _AppConfig(name="demo")

    with pytest.raises(RuntimeError, match="Nothing discovered") as exc_info:
        _discover_components(app_cfg)

    message = str(exc_info.value)
    assert "discovery.mode: manifest" in message
    assert "AGENTS" in message


def test_build_bootstrap_sqlalchemy_without_models_names_backend_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(auto, "_build_discovery_result", lambda _cfg: _agents_only())

    app_cfg = _AppConfig(name="demo")
    ctx = _ctx()

    with pytest.raises(RuntimeError, match="No BaseModel classes discovered") as exc_info:
        _build_bootstrap(app_cfg, ctx)

    assert "persistence.backend: none" in str(exc_info.value)


def test_build_bootstrap_none_with_models_builds_without_compiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with_models = DiscoveryResult(
        models=(PersistenceNoneRecord,), use_cases=(), interfaces=(), agent_specs=("agents/*.yaml",)
    )
    monkeypatch.setattr(auto, "_build_discovery_result", lambda _cfg: with_models)
    compiled: list[object] = []
    monkeypatch.setattr(sqlalchemy_backend, "compile_all", lambda *models: compiled.extend(models))

    runtime, wiring, discovered = _build_bootstrap(
        _AppConfig(name="demo"), _ctx(persistence={"backend": "none"})
    )

    assert compiled == []
    assert discovered.models == (PersistenceNoneRecord,)
    assert wiring.uow_factory is None
    assert runtime.executor is not None
