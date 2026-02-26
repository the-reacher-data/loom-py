"""Tests for trace_id enrichment in RuntimeExecutor."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.events import EventKind, RuntimeEvent
from loom.core.engine.executor import RuntimeExecutor
from loom.core.tracing import reset_trace_id, set_trace_id
from loom.core.use_case.use_case import UseCase


class _UC(UseCase[object, str]):
    async def execute(self) -> str:
        return "ok"


def _make_executor(events: list[RuntimeEvent]) -> RuntimeExecutor:
    class _Adapter:
        def on_event(self, event: RuntimeEvent) -> None:
            events.append(event)

    compiler = UseCaseCompiler()
    compiler.compile(_UC)
    return RuntimeExecutor(compiler, metrics=_Adapter())


class TestExecutorTraceIdEnrichment:
    @pytest.mark.asyncio
    async def test_exec_start_carries_trace_id_when_set(self) -> None:
        events: list[RuntimeEvent] = []
        executor = _make_executor(events)

        token = set_trace_id("trace-abc")
        try:
            await executor.execute(_UC())
        finally:
            reset_trace_id(token)

        start_events = [e for e in events if e.kind == EventKind.EXEC_START]
        assert start_events[0].trace_id == "trace-abc"

    @pytest.mark.asyncio
    async def test_exec_done_carries_trace_id_when_set(self) -> None:
        events: list[RuntimeEvent] = []
        executor = _make_executor(events)

        token = set_trace_id("trace-xyz")
        try:
            await executor.execute(_UC())
        finally:
            reset_trace_id(token)

        done_events = [e for e in events if e.kind == EventKind.EXEC_DONE]
        assert done_events[0].trace_id == "trace-xyz"

    @pytest.mark.asyncio
    async def test_trace_id_is_none_when_not_set(self) -> None:
        events: list[RuntimeEvent] = []
        executor = _make_executor(events)

        await executor.execute(_UC())

        for ev in events:
            assert ev.trace_id is None

    @pytest.mark.asyncio
    async def test_logger_bind_called_with_trace_id(self) -> None:
        compiler = UseCaseCompiler()
        compiler.compile(_UC)

        mock_logger = MagicMock()
        bound_logger = MagicMock()
        mock_logger.bind.return_value = bound_logger

        executor = RuntimeExecutor(compiler, logger=mock_logger)

        token = set_trace_id("bind-test")
        try:
            await executor.execute(_UC())
        finally:
            reset_trace_id(token)

        mock_logger.bind.assert_called_once_with(trace_id="bind-test")

    @pytest.mark.asyncio
    async def test_logger_not_bound_when_trace_id_absent(self) -> None:
        compiler = UseCaseCompiler()
        compiler.compile(_UC)

        mock_logger = MagicMock()
        executor = RuntimeExecutor(compiler, logger=mock_logger)

        await executor.execute(_UC())

        mock_logger.bind.assert_not_called()
