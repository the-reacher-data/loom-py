"""``SQLAlchemyBackend``: the readiness probe over the shared engine."""

from __future__ import annotations

import logging

import pytest

import loom.core.repository.sqlalchemy.backend as backend_module
from loom.core.config import ConfigContext
from loom.core.persistence import PersistenceWiring
from loom.core.repository.sqlalchemy.backend import SQLAlchemyBackend

_UNREACHABLE_URL = "sqlite+aiosqlite:////nonexistent/loom-readiness/store.db"


def _wiring(url: str) -> PersistenceWiring:
    ctx = ConfigContext.from_dict({"app": {"name": "demo"}, "database": {"url": url}})
    return SQLAlchemyBackend().build(ctx, ())


@pytest.mark.asyncio
async def test_readiness_is_true_when_the_database_answers() -> None:
    wiring = _wiring("sqlite+aiosqlite:///")

    assert wiring.readiness is not None
    async with wiring.lifespan_init():
        assert await wiring.readiness() is True


@pytest.mark.asyncio
async def test_readiness_is_false_when_the_database_is_unreachable(
    caplog: pytest.LogCaptureFixture,
) -> None:
    wiring = _wiring(_UNREACHABLE_URL)

    assert wiring.readiness is not None
    with caplog.at_level(logging.WARNING, logger=backend_module.__name__):
        assert await wiring.readiness() is False

    assert "readiness" in caplog.text
