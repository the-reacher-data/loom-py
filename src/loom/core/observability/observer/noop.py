"""No-op lifecycle observer for tests and disabled observability."""

from __future__ import annotations

from loom.core.observability.event import LifecycleEvent


class NoopObserver:
    """Lifecycle observer that discards every event.

    Used as the fallback when no backends are enabled and as the default
    observer in test fixtures that do not need observability assertions.
    """

    def on_event(self, event: LifecycleEvent) -> None:
        """Discard the event.

        Args:
            event: Lifecycle event — ignored.
        """


__all__ = ["NoopObserver"]
