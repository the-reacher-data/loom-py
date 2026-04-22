"""Persistent asyncio event loop for synchronous runtimes.

Provides a background event-loop thread so that synchronous workers
(Celery prefork, Bytewax operators) can execute async code without
creating/destroying a loop per task.

Lifecycle::

    worker = AsyncBridge()
    result = worker.run(some_coroutine())
    worker.shutdown()

Thread safety:
    :meth:`run` is safe to call from multiple threads - it schedules
    coroutines via ``run_coroutine_threadsafe``.  :meth:`shutdown` joins
    the background thread; call it once at process exit.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Coroutine
from typing import Any, TypeVar

T = TypeVar("T")


class AsyncBridge:
    """Per-worker persistent asyncio event loop on a daemon thread.

    Args:
        thread_name: Name for the background thread.
        shutdown_timeout: Seconds to wait when joining the thread.
    """

    __slots__ = ("_loop", "_thread", "_shutdown_timeout")

    def __init__(
        self,
        *,
        thread_name: str = "loom-async-bridge",
        shutdown_timeout: float = 10.0,
    ) -> None:
        self._shutdown_timeout = shutdown_timeout
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever,
            name=thread_name,
            daemon=True,
        )
        self._thread.start()

    def run(self, coro: Coroutine[Any, Any, T], *, timeout: float | None = None) -> T:
        """Submit *coro* to the background loop and block until done.

        Args:
            coro: Awaitable coroutine.
            timeout: Maximum seconds to wait.  ``None`` waits indefinitely.

        Returns:
            The coroutine's return value.

        Raises:
            RuntimeError: If the bridge has been shut down.
            TimeoutError: If *timeout* elapses before completion.
            Exception: Any exception raised by *coro*.
        """
        if self._loop.is_closed():
            _close_coro(coro)
            raise RuntimeError("AsyncBridge has been shut down.")
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout=timeout)

    @property
    def is_alive(self) -> bool:
        """Whether the background loop is running."""
        return self._thread.is_alive() and not self._loop.is_closed()

    def shutdown(self) -> None:
        """Stop the background loop and join the thread.

        Idempotent - safe to call multiple times.
        """
        if self._loop.is_closed():
            return
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=self._shutdown_timeout)
        self._loop.close()


def _close_coro(coro: object) -> None:
    """Close an unawaited coroutine to avoid ResourceWarning."""
    close = getattr(coro, "close", None)
    if callable(close):
        close()


AsyncWorker = AsyncBridge

__all__ = ["AsyncBridge", "AsyncWorker"]
