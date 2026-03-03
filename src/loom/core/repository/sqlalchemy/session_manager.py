from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from loom.core.logger import get_logger
from loom.core.tracing import get_trace_id


class SessionManager:
    """Async SQLAlchemy session manager with pooling support."""

    def __init__(
        self,
        url: str,
        *,
        echo: bool = False,
        pool_pre_ping: bool = True,
        pool_size: int | None = 10,
        max_overflow: int | None = 20,
        pool_timeout: int | None = 30,
        pool_recycle: int | None = 1800,
        connect_args: dict[str, object] | None = None,
        inject_trace_id: bool = True,
        **engine_kwargs: object,
    ) -> None:
        """Create a session manager backed by an async SQLAlchemy engine.

        Args:
            url: Database connection URL (e.g. ``"postgresql+asyncpg://..."``).
            echo: If ``True``, log all generated SQL statements.
            pool_pre_ping: Test connections before checkout to detect stale ones.
            pool_size: Number of permanent connections in the pool.
            max_overflow: Maximum additional connections beyond ``pool_size``.
            pool_timeout: Seconds to wait before raising on pool exhaustion.
            pool_recycle: Seconds after which a connection is recycled.
            connect_args: Extra keyword arguments passed to the DBAPI ``connect()`` call.
            inject_trace_id: When ``True``, prefixes every SQL statement with a
                ``/* trace_id=<id> */`` comment when a trace identifier is active
                in the current async context.  Visible in database slow-query logs
                and ``pg_stat_activity``.  Defaults to ``True``.
            **engine_kwargs: Additional keyword arguments forwarded to ``create_async_engine``.
        """
        engine_config: dict[str, object] = {
            "echo": echo,
            "pool_pre_ping": pool_pre_ping,
            **engine_kwargs,
        }
        if pool_size is not None:
            engine_config["pool_size"] = pool_size
        if max_overflow is not None:
            engine_config["max_overflow"] = max_overflow
        if pool_timeout is not None:
            engine_config["pool_timeout"] = pool_timeout
        if pool_recycle is not None:
            engine_config["pool_recycle"] = pool_recycle
        if connect_args is not None:
            engine_config["connect_args"] = connect_args

        self._log = get_logger(__name__).bind(module="session_manager")
        self._engine = create_async_engine(url, **engine_config)
        self._session_factory = async_sessionmaker(
            bind=self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        if inject_trace_id:
            _register_trace_id_listener(self._engine)
        self._log.info(
            "SessionManagerInitialized",
            backend=self._engine.url.get_backend_name(),
            driver=self._engine.url.get_driver_name(),
            inject_trace_id=inject_trace_id,
        )

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Yield a scoped async session that is automatically closed on exit.

        Yields:
            An ``AsyncSession`` bound to the managed engine.
        """
        self._log.debug("SessionScopeOpened")
        session = self._session_factory()
        try:
            yield session
        finally:
            await session.close()
            self._log.debug("SessionScopeClosed")

    async def dispose(self) -> None:
        """Dispose of the engine and release all pooled connections."""
        await self._engine.dispose()
        self._log.info("SessionManagerDisposed")

    @property
    def engine(self) -> AsyncEngine:
        """The underlying async SQLAlchemy engine."""
        return self._engine

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        """The configured async session factory bound to the engine."""
        return self._session_factory


def _register_trace_id_listener(async_engine: AsyncEngine) -> None:
    """Register a ``before_cursor_execute`` listener that injects trace comments.

    The listener is attached to the underlying sync engine so it fires for
    every SQL statement executed through the async engine.  When a
    trace-id is active in the current async context, the statement is
    prefixed with ``/* trace_id=<id> */``.

    Args:
        async_engine: The :class:`~sqlalchemy.ext.asyncio.AsyncEngine` whose
            sync engine will receive the listener.
    """

    @event.listens_for(async_engine.sync_engine, "before_cursor_execute", retval=True)
    def _inject(
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> tuple[str, Any]:
        tid = get_trace_id()
        if tid:
            statement = f"/* trace_id={tid} */ " + statement
        return statement, parameters
