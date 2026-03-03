"""Persistent asyncio event loop for Celery prefork worker processes.

Each Celery worker process gets one :class:`WorkerEventLoop` that runs a
single asyncio loop on a background daemon thread.  All coroutine
submissions from the Celery task thread block until the coroutine
completes and then return the result (or re-raise the exception).

This avoids creating and tearing down an event loop on every task
execution, which would invalidate SQLAlchemy's async connection pool and
force a full TCP handshake per task.

Lifecycle::

    # In worker_process_init signal handler (child process):
    WorkerEventLoop.initialize()

    # Inside a Celery task function:
    result = WorkerEventLoop.run(some_coroutine(arg))

    # In worker_process_shutdown signal handler:
    WorkerEventLoop.shutdown(timeout=10.0)

Thread safety:
    :meth:`initialize` and :meth:`shutdown` are protected by a
    ``threading.Lock``.  :meth:`run` is safe to call concurrently from
    multiple task threads because ``asyncio.run_coroutine_threadsafe``
    schedules the coroutine onto the event loop without blocking the lock.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any, TypeVar

T = TypeVar("T")


class WorkerEventLoop:
    """Per-process persistent asyncio event loop for Celery workers.

    A class-level singleton: one loop per OS process.  After ``fork()``,
    each child process starts with ``_loop = None`` and must call
    :meth:`initialize` (typically from the ``worker_process_init``
    Celery signal) before submitting coroutines.

    Attributes:
        _loop: The running asyncio event loop, or ``None`` before
            :meth:`initialize` or after :meth:`shutdown`.
        _thread: Daemon thread driving ``_loop.run_forever()``.
        _lock: Mutex guarding :meth:`initialize` / :meth:`shutdown`.

    Example::

        # bootstrap (signal handler, child process):
        WorkerEventLoop.initialize()

        # inside task:
        result = WorkerEventLoop.run(my_coroutine())

        # teardown (signal handler):
        WorkerEventLoop.shutdown()
    """

    _loop: asyncio.AbstractEventLoop | None = None
    _thread: threading.Thread | None = None
    _lock: threading.Lock = threading.Lock()

    @classmethod
    def initialize(cls) -> None:
        """Start the background event loop in a daemon thread.

        Idempotent: calling :meth:`initialize` when the loop is already
        running is a no-op.

        Raises:
            RuntimeError: Should never happen under normal operation — only
                if the background thread fails to start.
        """
        with cls._lock:
            if cls._loop is not None:
                return
            loop = asyncio.new_event_loop()
            thread = threading.Thread(
                target=loop.run_forever,
                name="loom-worker-loop",
                daemon=True,
            )
            thread.start()
            cls._loop = loop
            cls._thread = thread

    @classmethod
    def run(cls, coro: Any) -> Any:
        """Submit *coro* to the background loop and block until done.

        The calling thread (Celery task thread) blocks on a
        ``concurrent.futures.Future`` until the coroutine finishes.
        Exceptions raised inside the coroutine are re-raised in the
        calling thread.

        Args:
            coro: An awaitable coroutine to execute on the background loop.

        Returns:
            Whatever the coroutine returns.

        Raises:
            RuntimeError: If :meth:`initialize` has not been called first.
            Exception: Any exception raised by *coro* is propagated.
        """
        if cls._loop is None:
            raise RuntimeError(
                "WorkerEventLoop is not initialized. "
                "Call WorkerEventLoop.initialize() from the "
                "worker_process_init signal handler before running tasks."
            )
        future = asyncio.run_coroutine_threadsafe(coro, cls._loop)
        return future.result()

    @classmethod
    def shutdown(cls, *, timeout: float = 10.0) -> None:
        """Stop the background loop and close it.

        Signals ``loop.stop()``, waits for the thread to finish (up to
        *timeout* seconds), then closes the loop.  Idempotent: safe to
        call when already shut down.

        Args:
            timeout: Maximum seconds to wait for the background thread to
                join.  The thread is a daemon so the process will not hang
                if this times out, but pending coroutines may be abandoned.
        """
        with cls._lock:
            if cls._loop is None:
                return
            cls._loop.call_soon_threadsafe(cls._loop.stop)
            if cls._thread is not None:
                cls._thread.join(timeout=timeout)
            cls._loop.close()
            cls._loop = None
            cls._thread = None
