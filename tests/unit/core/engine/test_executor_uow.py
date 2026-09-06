"""Tests for RuntimeExecutor UoW lifecycle and pending-dispatch wiring.

Verifies that:
- dispatches queued during an execution run after the UoW closed (commit).
- dispatches queued during a failed execution are discarded, never run.
- nested executions (reusing an existing UoW) enqueue on the outer channel
  and never drain on their own.
- without a UoW the dispatches run at the end of the execution.
- read_only=True (call-site) skips UoW entirely.
- UseCase.read_only = True (class-level) also skips UoW via plan.

FR-015: the pins on ``flush_pending_dispatches`` / ``clear_pending_dispatches``
being called by the executor were replaced by observed dispatches, since the
executor now drains its own post-commit channel and no longer imports the
job context helpers.  The executor-level double-cancellation test was removed:
the cancellation shield moved into the adapters (lead decision 11), and the
behaviour is pinned by
``tests/unit/core/uow/test_sqlalchemy_uow.py::test_second_cancellation_during_rollback_still_rolls_back_and_closes``
and
``tests/unit/core/repository/mongo/test_uow.py::TestTransactional::test_second_cancellation_during_abort_still_aborts_and_ends``.
"""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar

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

    transactional: ClassVar[bool] = True

    def __init__(self) -> None:
        self.begun = False
        self.committed = False
        self.rolled_back = False
        self.closed = 0

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

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        try:
            if exc_type is None:
                await self.commit()
            else:
                await self.rollback()
        finally:
            self.closed += 1


class _StubUoWFactory:
    def __init__(self, uow: _StubUoW) -> None:
        self._uow = uow

    def create(self) -> UnitOfWork:
        return self._uow


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


class _Dispatching(UseCase[Any, str]):
    """Queues one dispatch that records whether the UoW had closed when it ran."""

    def __init__(self, uow: _StubUoW, ran: list[str]) -> None:
        self._uow = uow
        self._ran = ran

    async def execute(self) -> str:
        add_pending_dispatch(lambda: self._ran.append(f"closed={self._uow.closed}"))
        return "ok"


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
async def test_dispatch_runs_after_the_uow_closed() -> None:
    uow = _StubUoW()
    executor = _make_executor(uow)
    ran: list[str] = []

    await executor.execute(_Dispatching(uow, ran))

    assert ran == ["closed=1"]
    assert uow.committed


@pytest.mark.asyncio
async def test_begin_and_commit_called_on_success() -> None:
    uow = _StubUoW()
    executor = _make_executor(uow)

    await executor.execute(_OkUseCase())

    assert uow.begun
    assert uow.committed
    assert not uow.rolled_back
    assert uow.closed == 1


# ---------------------------------------------------------------------------
# Tests — failure path
# ---------------------------------------------------------------------------


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
    assert uow.closed == 1


@pytest.mark.asyncio
async def test_hace_rollback_cuando_la_ejecucion_se_cancela() -> None:
    """A cancellation is not an ``Exception``, yet the begun transaction must close."""
    uow = _StubUoW()
    executor = _make_executor(uow)
    use_case = _CancellingUseCase()

    with pytest.raises(asyncio.CancelledError):
        await executor.execute(use_case)

    assert uow.begun
    assert uow.rolled_back
    assert not uow.committed
    assert uow.closed == 1


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
    await executor.execute(_OkUseCase())

    assert flushed == [], "dispatch must not run after rollback, nor in a later execution"


@pytest.mark.asyncio
async def test_dispatches_registered_during_cancelled_execution_are_cleared() -> None:
    uow = _StubUoW()
    executor = _make_executor(uow)
    flushed: list[str] = []

    class _DispatchAndCancel(UseCase[Any, None]):
        async def execute(self) -> None:  # type: ignore[override]
            add_pending_dispatch(lambda: flushed.append("ran"))
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await executor.execute(_DispatchAndCancel())
    await executor.execute(_OkUseCase())

    assert flushed == []


# ---------------------------------------------------------------------------
# Tests — no UoW
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_runs_at_the_end_without_uow_factory() -> None:
    """FR-003: without a UoW the dispatches run at the end of the execution."""
    compiler = UseCaseCompiler()
    executor = RuntimeExecutor(compiler)
    ran: list[str] = []

    class _DispatchNoUoW(UseCase[Any, str]):
        async def execute(self) -> str:
            add_pending_dispatch(lambda: ran.append("ran"))
            ran.append("executed")
            return "ok"

    await executor.execute(_DispatchNoUoW())

    assert ran == ["executed", "ran"]


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


# ---------------------------------------------------------------------------
# Tests — nested execution reuses outer UoW
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_nested_execution_does_not_flush_independently() -> None:
    """Nested executor.execute() calls share the outer UoW and its channel."""
    uow = _StubUoW()
    executor = _make_executor(uow)
    ran: list[str] = []

    class _Outer(UseCase[Any, str]):
        async def execute(self) -> str:
            inner = await executor.execute(_Dispatching(uow, ran))
            ran.append(f"inner-returned closed={uow.closed}")
            return inner

    await executor.execute(_Outer())

    # The inner dispatch ran once, after the outer UoW closed, not when the inner returned.
    assert ran == ["inner-returned closed=0", "closed=1"]
    assert uow.closed == 1
