"""Tests for RuntimeExecutor UoW lifecycle and pending-dispatch wiring.

Verifies that:
- flush_pending_dispatches() is called after a successful commit.
- clear_pending_dispatches() is called after a rollback.
- Dispatches registered during a failed execution are never flushed.
- Nested executions (reusing an existing UoW) do NOT trigger flush/clear.
- read_only=True (call-site) skips UoW entirely.
- UseCase.read_only = True (class-level) also skips UoW via plan.
- CompiledRoute.read_only propagates to the executor handler (GET routes).
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.job.context import add_pending_dispatch
from loom.core.uow.abc import UnitOfWork
from loom.core.use_case.use_case import UseCase

# ---------------------------------------------------------------------------
# Minimal UoW stub
# ---------------------------------------------------------------------------


class _StubUoW:
    """Fake UnitOfWork that records begin/commit/rollback calls."""

    def __init__(self) -> None:
        self.begun = False
        self.committed = False
        self.rolled_back = False

    async def begin(self) -> None:
        await asyncio.sleep(0)
        self.begun = True

    async def commit(self) -> None:
        await asyncio.sleep(0)
        self.committed = True

    async def rollback(self) -> None:
        await asyncio.sleep(0)
        self.rolled_back = True

    async def __aenter__(self) -> _StubUoW:
        await self.begin()
        return self

    async def __aexit__(self, *args: object) -> None:
        if args[0] is None:
            await self.commit()
        else:
            await self.rollback()


class _StubUoWFactory:
    def __init__(self, uow: _StubUoW) -> None:
        self._uow = uow

    def create(self) -> UnitOfWork:
        return self._uow  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# UseCase fixtures
# ---------------------------------------------------------------------------


class _OkUseCase(UseCase[Any, str]):
    async def execute(self) -> str:
        return "ok"


class _FailingUseCase(UseCase[Any, str]):
    async def execute(self) -> str:
        raise ValueError("boom")


class _CancellingUseCase(UseCase[Any, str]):
    """Models a use case cut by cancellation while it awaits."""

    async def execute(self) -> str:
        raise asyncio.CancelledError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_executor(uow: _StubUoW) -> RuntimeExecutor:
    compiler = UseCaseCompiler()
    factory = _StubUoWFactory(uow)
    return RuntimeExecutor(compiler, uow_factory=factory)


# ---------------------------------------------------------------------------
# Tests — successful path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flush_called_after_successful_commit() -> None:
    uow = _StubUoW()
    executor = _make_executor(uow)
    uc = _OkUseCase()

    with (
        patch(
            "loom.core.engine.executor.flush_pending_dispatches", new_callable=AsyncMock
        ) as mock_flush,
        patch("loom.core.engine.executor.clear_pending_dispatches") as mock_clear,
    ):
        await executor.execute(uc)

    mock_flush.assert_awaited_once()
    mock_clear.assert_not_called()


@pytest.mark.asyncio
async def test_begin_and_commit_called_on_success() -> None:
    uow = _StubUoW()
    executor = _make_executor(uow)

    await executor.execute(_OkUseCase())

    assert uow.begun
    assert uow.committed
    assert not uow.rolled_back


# ---------------------------------------------------------------------------
# Tests — failure path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_clear_called_after_rollback() -> None:
    uow = _StubUoW()
    executor = _make_executor(uow)

    use_case = _FailingUseCase()
    with (
        patch(
            "loom.core.engine.executor.flush_pending_dispatches", new_callable=AsyncMock
        ) as mock_flush,
        patch("loom.core.engine.executor.clear_pending_dispatches") as mock_clear,
        pytest.raises(ValueError, match="boom"),
    ):
        await executor.execute(use_case)

    mock_clear.assert_called_once()
    mock_flush.assert_not_awaited()


@pytest.mark.asyncio
async def test_rollback_called_on_failure() -> None:
    uow = _StubUoW()
    executor = _make_executor(uow)

    use_case = _FailingUseCase()
    with pytest.raises(ValueError):
        await executor.execute(use_case)

    assert uow.begun
    assert uow.rolled_back
    assert not uow.committed


@pytest.mark.asyncio
async def test_hace_rollback_cuando_la_ejecucion_se_cancela() -> None:
    """A cancellation is not an ``Exception``, yet the begun transaction must close."""
    uow = _StubUoW()
    executor = _make_executor(uow)
    use_case = _CancellingUseCase()

    with (
        patch("loom.core.engine.executor.clear_pending_dispatches") as mock_clear,
        pytest.raises(asyncio.CancelledError),
    ):
        await executor.execute(use_case)

    assert uow.begun
    assert uow.rolled_back
    assert not uow.committed
    mock_clear.assert_called_once()


@pytest.mark.asyncio
async def test_completa_el_rollback_cuando_llega_una_segunda_cancelacion() -> None:
    """A second cancel while rolling back must not cut the rollback short."""
    release = asyncio.Event()
    gate = asyncio.Event()

    class _RollbackGatedUoW(_StubUoW):
        async def rollback(self) -> None:
            await release.wait()
            self.rolled_back = True

    class _WaitingUseCase(UseCase[Any, str]):
        async def execute(self) -> str:
            await gate.wait()
            return "unreachable"

    uow = _RollbackGatedUoW()
    executor = _make_executor(uow)
    run = asyncio.ensure_future(executor.execute(_WaitingUseCase()))
    await asyncio.sleep(0)
    run.cancel()  # cuts the use case: the rollback starts and blocks on ``release``
    await asyncio.sleep(0)
    run.cancel()  # arrives while the rollback is in flight
    with pytest.raises(asyncio.CancelledError):
        await run
    assert not uow.rolled_back

    release.set()
    await asyncio.sleep(0)

    assert uow.rolled_back


@pytest.mark.asyncio
async def test_dispatches_registered_during_failed_execution_are_cleared() -> None:
    """Dispatches added inside a failing UseCase must be discarded."""
    uow = _StubUoW()
    executor = _make_executor(uow)
    flushed: list[str] = []

    class _DispatchAndFail(UseCase[Any, None]):
        async def execute(self) -> None:  # type: ignore[override]
            add_pending_dispatch(lambda: flushed.append("ran"))
            raise RuntimeError("fail")

    use_case = _DispatchAndFail()
    with pytest.raises(RuntimeError):
        await executor.execute(use_case)

    assert flushed == [], "dispatch must not run after rollback"


# ---------------------------------------------------------------------------
# Tests — no UoW
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_flush_or_clear_without_uow_factory() -> None:
    """When no uow_factory is configured, neither flush nor clear is called."""
    compiler = UseCaseCompiler()
    executor = RuntimeExecutor(compiler)

    with (
        patch(
            "loom.core.engine.executor.flush_pending_dispatches", new_callable=AsyncMock
        ) as mock_flush,
        patch("loom.core.engine.executor.clear_pending_dispatches") as mock_clear,
    ):
        await executor.execute(_OkUseCase())

    mock_flush.assert_not_awaited()
    mock_clear.assert_not_called()


# ---------------------------------------------------------------------------
# Tests — nested execution reuses outer UoW
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Tests — read_only skips UoW
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_read_only_call_site_skips_uow() -> None:
    """Passing read_only=True at call-site bypasses begin/commit even with a factory."""
    uow = _StubUoW()
    executor = _make_executor(uow)

    result = await executor.execute(_OkUseCase(), read_only=True)

    assert result == "ok"
    assert not uow.begun
    assert not uow.committed
    assert not uow.rolled_back


@pytest.mark.asyncio
async def test_read_only_class_flag_skips_uow() -> None:
    """UseCase.read_only = True causes the plan to mark it read-only."""

    class _ReadOnlyUseCase(UseCase[Any, str]):
        read_only = True

        async def execute(self) -> str:
            return "read"

    uow = _StubUoW()
    executor = _make_executor(uow)

    result = await executor.execute(_ReadOnlyUseCase())

    assert result == "read"
    assert not uow.begun


@pytest.mark.asyncio
async def test_read_only_false_still_opens_uow() -> None:
    """Default (read_only=False) must still open the UoW as before."""
    uow = _StubUoW()
    executor = _make_executor(uow)

    await executor.execute(_OkUseCase(), read_only=False)

    assert uow.begun
    assert uow.committed


@pytest.mark.asyncio
async def test_nested_execution_does_not_flush_independently() -> None:
    """Nested executor.execute() calls share the outer UoW and do not flush."""
    uow = _StubUoW()
    executor = _make_executor(uow)

    class _Outer(UseCase[Any, str]):
        async def execute(self) -> str:
            # Simulate inner call within the same async context
            inner = _OkUseCase()
            return await executor.execute(inner)

    with patch(
        "loom.core.engine.executor.flush_pending_dispatches", new_callable=AsyncMock
    ) as mock_flush:
        await executor.execute(_Outer())

    # flush is called exactly once (for the outer UoW owner, not the inner)
    mock_flush.assert_awaited_once()
