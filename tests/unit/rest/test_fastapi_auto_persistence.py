from __future__ import annotations

import pytest

from loom.core.config import ConfigContext
from loom.core.config.errors import ConfigError
from loom.core.discovery.base import DiscoveryResult
from loom.core.repository.dynamodb.uow import DynamoUnitOfWorkFactory
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from loom.rest.fastapi.auto import _resolve_persistence


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


def test_resolve_persistence_defaults_to_sqlalchemy() -> None:
    wiring = _resolve_persistence(_ctx(), _no_models())

    assert isinstance(wiring.uow_factory, SQLAlchemyUnitOfWorkFactory)
    assert wiring.requires_relational_models is True
    assert callable(wiring.repo_registration_module)
    assert callable(wiring.lifespan_init)


def test_resolve_persistence_honours_explicit_sqlalchemy_backend() -> None:
    wiring = _resolve_persistence(
        _ctx(persistence={"backend": "sqlalchemy"}),
        _no_models(),
    )

    assert isinstance(wiring.uow_factory, SQLAlchemyUnitOfWorkFactory)


def test_resolve_persistence_rejects_unknown_backend() -> None:
    ctx = _ctx(persistence={"backend": "mongodb"})
    models = _no_models()
    with pytest.raises(ValueError, match="Unsupported persistence backend: 'mongodb'"):
        _resolve_persistence(ctx, models)


def test_resolve_persistence_selects_dynamodb_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Dummy credentials satisfy boto3's default chain against a local endpoint;
    # real credentials come from the task role on ECS.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    wiring = _resolve_persistence(
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
    assert wiring.requires_relational_models is False
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

    wiring = _resolve_persistence(ctx, _no_models())

    assert isinstance(wiring.uow_factory, DynamoUnitOfWorkFactory)
    assert wiring.requires_relational_models is False


def test_resolve_persistence_dynamodb_requires_config_section() -> None:
    ctx = _ctx(persistence={"backend": "dynamodb"})
    models = _no_models()
    with pytest.raises(ConfigError, match="persistence.dynamodb"):
        _resolve_persistence(ctx, models)
