"""Flow run summary context variable.

The flow body writes a one-line summary here before returning or raising;
the Prefect ``on_failure`` / ``on_completion`` hook reads it to enrich
the notification message.

Using a ``ContextVar`` keeps each concurrent flow run isolated without
any global state.
"""

from __future__ import annotations

from contextvars import ContextVar

_run_summary: ContextVar[str | None] = ContextVar("_loom_run_summary", default=None)


def set_run_summary(text: str) -> None:
    """Store *text* as the summary for the current flow run context."""
    _run_summary.set(text)


def get_run_summary() -> str | None:
    """Return the summary stored for the current flow run, or ``None``."""
    return _run_summary.get()


__all__ = ["get_run_summary", "set_run_summary"]
