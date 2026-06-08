"""Run an awaitable from sync code, safe under an active event loop."""

from __future__ import annotations

from typing import Any, TypeVar

from prefect.utilities.asyncutils import run_coro_as_sync

T = TypeVar("T")


def run_sync(coro: Any) -> Any:
    """Run *coro* to completion from sync code via Prefect's runner."""
    return run_coro_as_sync(coro)


__all__ = ["run_sync"]
