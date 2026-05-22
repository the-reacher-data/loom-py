"""Resource lifecycle management for the Bytewax adapter."""

from __future__ import annotations

from collections.abc import Coroutine, Mapping
from typing import Any, Protocol, runtime_checkable

from loom.core.async_bridge import AsyncBridge
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.streaming.bytewax._operators import ResourceLifecycle, lifecycle_for
from loom.streaming.core._typing import StreamPayload
from loom.streaming.nodes._table.common import SqlAlchemyDatabaseConfig
from loom.streaming.nodes._with import With, WithAsync


@runtime_checkable
class _ResourceManagerProtocol(Protocol):
    """Manage per-node lifecycle objects during adapter wiring."""

    def manager_for(
        self,
        idx: int,
        node: With[StreamPayload, StreamPayload] | WithAsync[StreamPayload, StreamPayload],
    ) -> ResourceLifecycle:
        """Return the lifecycle manager for one scoped node."""
        ...

    def shutdown_all(self) -> None:
        """Shutdown all lifecycle managers."""
        ...


@runtime_checkable
class _SharedSessionManagerProtocol(Protocol):
    """Minimal SQLAlchemy session-manager contract cached by the adapter."""

    def dispose(self) -> Coroutine[Any, Any, None]:
        """Dispose pooled connections and release engine resources."""
        ...


@runtime_checkable
class _SessionManagerFactoryProtocol(Protocol):
    """Factory contract required to build SQLAlchemy session managers."""

    @classmethod
    def from_config(
        cls,
        config: Mapping[str, Any],
        *,
        inject_trace_id: bool = True,
        **engine_kwargs: object,
    ) -> _SharedSessionManagerProtocol:
        """Build a session manager from resolved configuration."""
        ...


class ResourceManager:
    """Own the resource lifecycle objects used by scoped nodes."""

    __slots__ = ("_bridge", "_managers", "_session_managers")

    def __init__(self, bridge: AsyncBridge | None) -> None:
        self._bridge = bridge
        self._managers: dict[int, ResourceLifecycle] = {}
        self._session_managers: dict[
            tuple[tuple[str, object], ...],
            _SharedSessionManagerProtocol,
        ] = {}

    def manager_for(
        self,
        idx: int,
        node: With[StreamPayload, StreamPayload] | WithAsync[StreamPayload, StreamPayload],
    ) -> ResourceLifecycle:
        """Get or create the lifecycle manager for one scoped node."""
        if idx not in self._managers:
            self._managers[idx] = lifecycle_for(node, bridge=self._bridge)
        return self._managers[idx]

    def session_manager_for(
        self,
        config: SqlAlchemyDatabaseConfig | Mapping[str, object],
    ) -> _SharedSessionManagerProtocol:
        """Get or create one shared SQLAlchemy session manager for a config."""
        if self._bridge is None:
            raise RuntimeError("SQLAlchemy session managers require an async bridge.")
        resolved: Mapping[str, object]
        if isinstance(config, SqlAlchemyDatabaseConfig):
            resolved = config.to_session_manager_config()
        else:
            resolved = config
        key = _session_manager_key(resolved)
        if key not in self._session_managers:
            self._session_managers[key] = SessionManager.from_config(resolved)
        return self._session_managers[key]

    def shutdown_all(self) -> None:
        """Shutdown all managed resources and any associated bridge."""
        errors: list[Exception] = []
        for manager in self._managers.values():
            try:
                manager.shutdown()
            except Exception as exc:
                errors.append(exc)
        bridge = self._bridge
        for session_manager in self._session_managers.values():
            if bridge is None:
                break
            try:
                bridge.run(session_manager.dispose())
            except Exception as exc:
                errors.append(exc)
        self._managers.clear()
        self._session_managers.clear()
        if self._bridge is not None:
            try:
                self._bridge.shutdown()
            except Exception as exc:
                errors.append(exc)
            finally:
                self._bridge = None
        if errors:
            raise ExceptionGroup("shutdown errors", errors)


def _freeze_mapping(mapping: Mapping[str, object]) -> tuple[tuple[str, object], ...]:
    """Convert a mapping into a hashable key for resource caching."""
    return tuple(sorted((key, _freeze_value(value)) for key, value in mapping.items()))


def _freeze_value(value: object) -> object:
    if isinstance(value, Mapping):
        return _freeze_mapping(value)
    if isinstance(value, list):
        return tuple(_freeze_value(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze_value(item) for item in value)
    if isinstance(value, set):
        return tuple(sorted((_freeze_value(item) for item in value), key=repr))
    return value


def _session_manager_key(config: Mapping[str, object]) -> tuple[tuple[str, object], ...]:
    """Extract the connection settings that define a shared session manager."""
    return _freeze_mapping(
        {
            "url": config.get("url"),
            "echo": config.get("echo", False),
            "pool_pre_ping": config.get("pool_pre_ping", True),
            "pool_size": config.get("pool_size", 10),
            "max_overflow": config.get("max_overflow", 20),
            "pool_timeout": config.get("pool_timeout", 30),
            "pool_recycle": config.get("pool_recycle", 1800),
            "connect_args": config.get("connect_args") or {},
        }
    )


__all__ = ["ResourceManager", "_ResourceManagerProtocol"]
