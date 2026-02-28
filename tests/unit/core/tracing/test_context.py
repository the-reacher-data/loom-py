"""Unit tests for loom.core.tracing.context."""

from __future__ import annotations

import asyncio

import pytest

from loom.core.tracing import generate_trace_id, get_trace_id, reset_trace_id, set_trace_id


class TestGetTraceId:
    def test_returns_none_by_default(self) -> None:
        assert get_trace_id() is None

    def test_returns_set_value(self) -> None:
        token = set_trace_id("abc123")
        try:
            assert get_trace_id() == "abc123"
        finally:
            reset_trace_id(token)

    def test_returns_none_after_reset(self) -> None:
        token = set_trace_id("abc123")
        reset_trace_id(token)
        assert get_trace_id() is None


class TestSetTraceId:
    def test_returns_token(self) -> None:
        token = set_trace_id("x")
        try:
            assert token is not None
        finally:
            reset_trace_id(token)

    def test_overwrites_previous_value(self) -> None:
        t1 = set_trace_id("first")
        try:
            t2 = set_trace_id("second")
            try:
                assert get_trace_id() == "second"
            finally:
                reset_trace_id(t2)
        finally:
            reset_trace_id(t1)


class TestResetTraceId:
    def test_restores_none_when_was_none(self) -> None:
        token = set_trace_id("tid")
        reset_trace_id(token)
        assert get_trace_id() is None

    def test_restores_previous_value(self) -> None:
        outer = set_trace_id("outer")
        try:
            inner = set_trace_id("inner")
            reset_trace_id(inner)
            assert get_trace_id() == "outer"
        finally:
            reset_trace_id(outer)


class TestGenerateTraceId:
    def test_returns_32_char_string(self) -> None:
        tid = generate_trace_id()
        assert isinstance(tid, str)
        assert len(tid) == 32

    def test_returns_lowercase_hex(self) -> None:
        tid = generate_trace_id()
        assert all(c in "0123456789abcdef" for c in tid)

    def test_each_call_unique(self) -> None:
        ids = {generate_trace_id() for _ in range(100)}
        assert len(ids) == 100


class TestAsyncIsolation:
    @pytest.mark.asyncio
    async def test_trace_id_isolated_between_async_tasks(self) -> None:
        results: dict[str, str | None] = {}

        async def task_a() -> None:
            token = set_trace_id("task-a")
            await asyncio.sleep(0)
            results["a"] = get_trace_id()
            reset_trace_id(token)

        async def task_b() -> None:
            token = set_trace_id("task-b")
            await asyncio.sleep(0)
            results["b"] = get_trace_id()
            reset_trace_id(token)

        await asyncio.gather(task_a(), task_b())
        assert results["a"] == "task-a"
        assert results["b"] == "task-b"

    @pytest.mark.asyncio
    async def test_child_task_inherits_parent_trace_id(self) -> None:
        """ContextVar copies propagate into child tasks at creation time."""
        token = set_trace_id("parent-tid")
        try:
            seen: list[str | None] = []

            async def child() -> None:
                seen.append(get_trace_id())

            task = asyncio.create_task(child())
            await task
            assert seen == ["parent-tid"]
        finally:
            reset_trace_id(token)
