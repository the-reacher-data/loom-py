from __future__ import annotations

import pytest

from loom.core.config import ConfigContext
from loom.core.discovery.base import DiscoveryResult
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
    with pytest.raises(ValueError, match="Unsupported persistence backend: 'dynamodb'"):
        _resolve_persistence(_ctx(persistence={"backend": "dynamodb"}), _no_models())
