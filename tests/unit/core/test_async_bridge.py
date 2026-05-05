"""Unit tests for AsyncBridge — anyio blocking portal wrapper."""

from __future__ import annotations

import asyncio

import pytest

from loom.core.async_bridge import AsyncBridge


async def _return_value(x: int) -> int:
    await asyncio.sleep(0)
    return x


async def _raise_error() -> None:
    raise ValueError("boom")


class TestAsyncBridgeLifecycle:
    def test_bridge_starts_alive(self) -> None:
        bridge = AsyncBridge()
        try:
            assert bridge.is_alive is True
        finally:
            bridge.shutdown()

    def test_bridge_reports_dead_after_shutdown(self) -> None:
        bridge = AsyncBridge()
        bridge.shutdown()

        assert bridge.is_alive is False

    def test_shutdown_is_idempotent(self) -> None:
        bridge = AsyncBridge()
        bridge.shutdown()
        bridge.shutdown()  # must not raise


class TestAsyncBridgeRun:
    def test_run_returns_coroutine_result(self) -> None:
        bridge = AsyncBridge()
        try:
            result = bridge.run(_return_value(42))
        finally:
            bridge.shutdown()

        assert result == 42

    def test_run_propagates_exception(self) -> None:
        bridge = AsyncBridge()
        try:
            with pytest.raises(ValueError, match="boom"):
                bridge.run(_raise_error())
        finally:
            bridge.shutdown()

    def test_run_raises_after_shutdown(self) -> None:
        bridge = AsyncBridge()
        bridge.shutdown()

        with pytest.raises(RuntimeError, match="shut down"):
            bridge.run(_return_value(1))

    def test_run_with_timeout_raises_on_slow_coro(self) -> None:
        import anyio

        async def _slow() -> int:
            await anyio.sleep(60)
            return 0

        bridge = AsyncBridge()
        try:
            with pytest.raises(TimeoutError):
                bridge.run(_slow(), timeout=0.05)
        finally:
            bridge.shutdown()


class TestAsyncBridgeShutdownTimeout:
    def test_shutdown_timeout_ms_stored(self) -> None:
        bridge = AsyncBridge(shutdown_timeout_ms=2000)
        try:
            assert bridge._shutdown_timeout_ms == 2000
        finally:
            bridge.shutdown()

    def test_shutdown_with_timeout_completes_when_no_inflight_tasks(self) -> None:
        bridge = AsyncBridge(shutdown_timeout_ms=5000)
        bridge.run(_return_value(1))
        bridge.shutdown()  # must not hang

        assert bridge.is_alive is False


class TestAsyncBridgeBackend:
    def test_asyncio_backend_is_default(self) -> None:
        bridge = AsyncBridge()
        try:
            assert bridge._backend == "asyncio"
        finally:
            bridge.shutdown()

    def test_explicit_asyncio_backend(self) -> None:
        bridge = AsyncBridge(backend="asyncio")
        try:
            result = bridge.run(_return_value(7))
        finally:
            bridge.shutdown()

        assert result == 7
