"""Unit tests for BytewaxRuntimeConfig async backend settings."""

from __future__ import annotations

import pytest

pytest.importorskip("bytewax")

from loom.streaming.bytewax.runner import BytewaxRuntimeConfig, _build_backend_options


class TestBytewaxRuntimeConfigDefaults:
    def test_async_backend_defaults_to_asyncio(self) -> None:
        cfg = BytewaxRuntimeConfig()

        assert cfg.async_backend == "asyncio"

    def test_use_uvloop_defaults_to_false(self) -> None:
        cfg = BytewaxRuntimeConfig()

        assert cfg.use_uvloop is False

    def test_custom_backend_is_stored(self) -> None:
        cfg = BytewaxRuntimeConfig(async_backend="trio")

        assert cfg.async_backend == "trio"

    def test_use_uvloop_can_be_enabled(self) -> None:
        cfg = BytewaxRuntimeConfig(use_uvloop=True)

        assert cfg.use_uvloop is True


class TestForceShutdownTimeout:
    def test_force_shutdown_timeout_ms_defaults_to_none(self) -> None:
        cfg = BytewaxRuntimeConfig()

        assert cfg.force_shutdown_timeout_ms is None

    def test_force_shutdown_timeout_ms_can_be_set(self) -> None:
        cfg = BytewaxRuntimeConfig(force_shutdown_timeout_ms=5000)

        assert cfg.force_shutdown_timeout_ms == 5000


class TestBuildBackendOptions:
    def test_asyncio_without_uvloop_returns_empty(self) -> None:
        opts = _build_backend_options("asyncio", use_uvloop=False)

        assert opts == {}

    def test_trio_with_use_uvloop_flag_returns_empty(self) -> None:
        opts = _build_backend_options("trio", use_uvloop=True)

        assert opts == {}

    def test_asyncio_with_uvloop_adds_loop_factory(self) -> None:
        import sys

        if sys.platform == "win32":
            pytest.skip("uvloop not available on Windows")

        import uvloop

        opts = _build_backend_options("asyncio", use_uvloop=True)

        assert opts.get("loop_factory") is uvloop.new_event_loop
