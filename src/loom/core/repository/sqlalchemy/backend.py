"""The ``sqlalchemy`` persistence backend."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import ClassVar

import msgspec
from sqlalchemy import text

from loom.core.backend.sqlalchemy import compile_all, get_metadata, reset_registry
from loom.core.config import ConfigContext, ConfigKey
from loom.core.model import BaseModel
from loom.core.persistence.abc import PersistenceWiring
from loom.core.repository.sqlalchemy.registry import (
    build_sqlalchemy_repository_registration_module,
)
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory

_logger = logging.getLogger(__name__)


class _DatabaseConfig(msgspec.Struct, kw_only=True):
    url: str
    echo: bool | None = None
    pool_pre_ping: bool = True


class SQLAlchemyBackend:
    """Backend for ``persistence.backend: sqlalchemy``.

    Reads the ``database`` section, opens one engine shared by the unit of
    work and every repository, creates the compiled tables at startup and
    disposes the engine at shutdown.
    """

    name: ClassVar[str] = "sqlalchemy"

    def build(self, ctx: ConfigContext, models: Sequence[type[BaseModel]]) -> PersistenceWiring:
        """Build the SQLAlchemy wiring for the discovered models.

        Args:
            ctx: Configuration context holding the ``database`` section.
            models: Models whose repositories the DI module registers.

        Returns:
            The SQLAlchemy wiring.

        Raises:
            ConfigError: When the ``database`` section is missing or invalid.
        """
        db_cfg = ctx.section(ConfigKey.DATABASE, _DatabaseConfig)
        session_manager = _build_session_manager(db_cfg)
        return PersistenceWiring(
            uow_factory=SQLAlchemyUnitOfWorkFactory(session_manager),
            repo_registration_module=build_sqlalchemy_repository_registration_module(
                session_manager, models
            ),
            lifespan_init=lambda: _lifespan(session_manager),
            default_repository_type=RepositorySQLAlchemy,
            prepare_models=_prepare_models,
            readiness=lambda: _readiness(session_manager),
        )


def _build_session_manager(db_cfg: _DatabaseConfig) -> SessionManager:
    echo = db_cfg.echo if db_cfg.echo is not None else False
    return SessionManager(
        db_cfg.url,
        echo=echo,
        pool_pre_ping=db_cfg.pool_pre_ping,
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        connect_args={},
    )


def _prepare_models(models: Sequence[type[BaseModel]]) -> None:
    """Compile the discovered models into the shared SQLAlchemy registry."""
    if not models:
        _logger.warning(
            "no BaseModel classes discovered: the application starts with an empty "
            "relational schema. Declare your first model, or set "
            "persistence.backend: none if it never persists."
        )
    reset_registry()
    compile_all(*models)


async def _readiness(session_manager: SessionManager) -> bool:
    """Probe the database with ``SELECT 1``.

    Any failure is logged with its traceback and reported as not ready: the
    probe exists to be answered, never to raise.
    """
    try:
        async with session_manager.session() as session:
            await session.execute(text("SELECT 1"))
    except Exception:
        _logger.warning("sqlalchemy readiness probe failed", exc_info=True)
        return False
    return True


@asynccontextmanager
async def _lifespan(session_manager: SessionManager) -> AsyncIterator[None]:
    async with session_manager.engine.begin() as connection:
        await connection.run_sync(get_metadata().create_all)
    try:
        yield
    finally:
        await session_manager.dispose()
        reset_registry()


__all__ = ["SQLAlchemyBackend"]
