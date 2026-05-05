"""LifecycleObserver protocol — the single contract for all observers."""

from __future__ import annotations

from typing import Protocol

from loom.core.observability.event import LifecycleEvent


class LifecycleObserver(Protocol):
    """Observer that receives lifecycle events from an ``ObservabilityRuntime``.

    Implementors must be safe to call from any thread. Raising inside
    ``on_event`` is allowed — ``ObservabilityRuntime.emit`` isolates failures
    per-observer so one broken observer never interrupts the others.

    Example::

        class MyObserver:
            def on_event(self, event: LifecycleEvent) -> None:
                if event.kind is EventKind.ERROR:
                    alert(event.error)
    """

    def on_event(self, event: LifecycleEvent) -> None:
        """Handle one lifecycle event.

        Args:
            event: Immutable lifecycle event from the runtime.
        """
        ...


__all__ = ["LifecycleObserver"]
