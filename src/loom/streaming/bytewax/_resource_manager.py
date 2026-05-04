"""Resource lifecycle management for the Bytewax adapter."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from loom.core.async_bridge import AsyncBridge
from loom.streaming.bytewax._operators import ResourceLifecycle, lifecycle_for
from loom.streaming.core._typing import StreamPayload
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


class ResourceManager:
    """Own the resource lifecycle objects used by scoped nodes."""

    __slots__ = ("_bridge", "_managers")

    def __init__(self, bridge: AsyncBridge | None) -> None:
        self._bridge = bridge
        self._managers: dict[int, ResourceLifecycle] = {}

    def manager_for(
        self,
        idx: int,
        node: With[StreamPayload, StreamPayload] | WithAsync[StreamPayload, StreamPayload],
    ) -> ResourceLifecycle:
        """Get or create the lifecycle manager for one scoped node."""
        if idx not in self._managers:
            self._managers[idx] = lifecycle_for(node, bridge=self._bridge)
        return self._managers[idx]

    def shutdown_all(self) -> None:
        """Shutdown all managed resources and any associated bridge."""
        errors: list[Exception] = []
        for manager in self._managers.values():
            try:
                manager.shutdown()
            except Exception as exc:
                errors.append(exc)
        if self._bridge is not None:
            try:
                self._bridge.shutdown()
            except Exception as exc:
                errors.append(exc)
            finally:
                self._bridge = None
        if errors:
            raise ExceptionGroup("shutdown errors", errors)


__all__ = ["ResourceManager", "_ResourceManagerProtocol"]
