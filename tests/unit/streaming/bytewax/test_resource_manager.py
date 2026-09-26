"""Tests for Bytewax resource caching."""

from __future__ import annotations

import asyncio
from typing import cast

import pytest

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.core.async_bridge import AsyncBridge  # noqa: E402
from loom.core.repository.sqlalchemy.session_manager import SessionManager  # noqa: E402
from loom.streaming.bytewax._resource_manager import ResourceManager  # noqa: E402


class _FakeManager:
    async def dispose(self) -> None:
        await asyncio.sleep(0)
        return None


def test_session_manager_for_reuses_same_config(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    def _from_config(cls: type[SessionManager], config: dict[str, object]) -> _FakeManager:
        del cls
        calls.append(dict(config))
        return _FakeManager()

    monkeypatch.setattr(SessionManager, "from_config", classmethod(_from_config))

    manager = ResourceManager(cast(AsyncBridge, object()))
    first = manager.session_manager_for(
        {
            "url": "sqlite+aiosqlite:///:memory:",
            "connect_args": {"timeout": 1},
            "pool_pre_ping": True,
            "table": "orders",
            "chunk_size": 100,
        }
    )
    second = manager.session_manager_for(
        {
            "pool_pre_ping": True,
            "connect_args": {"timeout": 1},
            "url": "sqlite+aiosqlite:///:memory:",
            "table": "customers",
            "chunk_size": 500,
        }
    )

    assert first is second
    assert len(calls) == 1
