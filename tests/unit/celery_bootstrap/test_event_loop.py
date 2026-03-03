"""Unit tests for WorkerEventLoop (Piece 7 — persistent per-process loop)."""

from __future__ import annotations

import asyncio
import threading

import pytest

from loom.celery.event_loop import WorkerEventLoop

# ---------------------------------------------------------------------------
# Fixture: always reset class state after each test
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_worker_loop() -> None:
    """Ensure WorkerEventLoop is shut down after every test."""
    yield
    WorkerEventLoop.shutdown()


# ---------------------------------------------------------------------------
# initialize()
# ---------------------------------------------------------------------------


class TestInitialize:
    def test_sets_loop_and_thread(self) -> None:
        WorkerEventLoop.initialize()
        assert WorkerEventLoop._loop is not None
        assert WorkerEventLoop._thread is not None

    def test_loop_is_running(self) -> None:
        WorkerEventLoop.initialize()
        assert WorkerEventLoop._loop is not None
        assert WorkerEventLoop._loop.is_running()

    def test_thread_is_alive(self) -> None:
        WorkerEventLoop.initialize()
        assert WorkerEventLoop._thread is not None
        assert WorkerEventLoop._thread.is_alive()

    def test_thread_is_daemon(self) -> None:
        WorkerEventLoop.initialize()
        assert WorkerEventLoop._thread is not None
        assert WorkerEventLoop._thread.daemon is True

    def test_idempotent_second_call_no_op(self) -> None:
        WorkerEventLoop.initialize()
        first_loop = WorkerEventLoop._loop
        WorkerEventLoop.initialize()
        assert WorkerEventLoop._loop is first_loop

    def test_idempotent_concurrent_calls(self) -> None:
        """Two threads calling initialize() simultaneously must not create two loops."""
        errors: list[Exception] = []

        def _init() -> None:
            try:
                WorkerEventLoop.initialize()
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=_init) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        assert not errors
        # Only one loop should exist
        assert WorkerEventLoop._loop is not None


# ---------------------------------------------------------------------------
# run()
# ---------------------------------------------------------------------------


class TestRun:
    def test_raises_when_not_initialized(self) -> None:
        with pytest.raises(RuntimeError, match="not initialized"):
            WorkerEventLoop.run(asyncio.sleep(0))

    def test_returns_coroutine_result(self) -> None:
        WorkerEventLoop.initialize()

        async def _add(a: int, b: int) -> int:
            return a + b

        result = WorkerEventLoop.run(_add(3, 4))
        assert result == 7

    def test_reraises_coroutine_exception(self) -> None:
        WorkerEventLoop.initialize()

        async def _boom() -> None:
            raise ValueError("from coroutine")

        with pytest.raises(ValueError, match="from coroutine"):
            WorkerEventLoop.run(_boom())

    def test_concurrent_submissions_all_complete(self) -> None:
        WorkerEventLoop.initialize()
        results: list[int] = []
        errors: list[Exception] = []

        async def _compute(n: int) -> int:
            await asyncio.sleep(0)
            return n * 2

        def _submit(n: int) -> None:
            try:
                results.append(WorkerEventLoop.run(_compute(n)))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=_submit, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors
        assert sorted(results) == [i * 2 for i in range(10)]


# ---------------------------------------------------------------------------
# shutdown()
# ---------------------------------------------------------------------------


class TestShutdown:
    def test_clears_loop_and_thread(self) -> None:
        WorkerEventLoop.initialize()
        WorkerEventLoop.shutdown()
        assert WorkerEventLoop._loop is None
        assert WorkerEventLoop._thread is None

    def test_idempotent_when_already_shut_down(self) -> None:
        WorkerEventLoop.shutdown()  # must not raise
        WorkerEventLoop.shutdown()  # second call also safe

    def test_run_raises_after_shutdown(self) -> None:
        WorkerEventLoop.initialize()
        WorkerEventLoop.shutdown()

        with pytest.raises(RuntimeError, match="not initialized"):
            WorkerEventLoop.run(asyncio.sleep(0))

    def test_reinitialize_after_shutdown(self) -> None:
        WorkerEventLoop.initialize()
        first_loop = WorkerEventLoop._loop
        WorkerEventLoop.shutdown()
        WorkerEventLoop.initialize()
        # A fresh loop is created
        assert WorkerEventLoop._loop is not None
        assert WorkerEventLoop._loop is not first_loop

    def test_loop_stops_after_shutdown(self) -> None:
        WorkerEventLoop.initialize()
        loop = WorkerEventLoop._loop
        WorkerEventLoop.shutdown()
        assert loop is not None
        assert not loop.is_running()
