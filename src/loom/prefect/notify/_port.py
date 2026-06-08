"""Notifier protocol — single method, strategy-friendly."""

from __future__ import annotations

from typing import Protocol

from loom.prefect.notify._event import NotifyEvent


class Notifier(Protocol):
    """Sink that receives a :class:`NotifyEvent` on terminal flow states."""

    def notify(self, event: NotifyEvent) -> None:
        """Deliver *event* to the underlying transport.

        Implementations must never raise; transport errors are swallowed
        and logged so a notification glitch never aborts a flow.
        """
        ...


__all__ = ["Notifier"]
