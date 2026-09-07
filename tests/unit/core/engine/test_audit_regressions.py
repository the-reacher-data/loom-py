"""Regression tests for the external audit findings F01, F02 and F07.

The executor owns one lifecycle: ``EXEC_START`` before ``__aenter__``, the
unit of work driven only through its context-manager protocol, one terminal
event after the unit of work closed, and post-commit actions drained once
the transaction is out of the context.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.events import EventKind
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.post_commit import PostCommitError, active_channel
from loom.core.job.context import add_pending_dispatch
from loom.core.repository.sqlalchemy.transactional import get_active_session
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from loom.core.uow.abc import UnitOfWork
from loom.core.use_case.use_case import UseCase

from ._lifecycle_doubles import (
    Boom,
    Broker,
    Dispatching,
    DispatchThenFail,
    Log,
    Metrics,
    Ok,
    cancel_mid_flight,
    context_is_clean,
)

# ---------------------------------------------------------------------------
# Doubles
# ---------------------------------------------------------------------------


class _StubUoW:
    """Unit of work following the adapter contract: commit failure rolls back."""

    transactional: ClassVar[bool] = True

    def __init__(
        self,
        log: Log,
        *,
        commit_raises: Exception | None = None,
        begin_raises: Exception | None = None,
    ) -> None:
        self._log = log
        self._commit_raises = commit_raises
        self._begin_raises = begin_raises

    async def begin(self) -> None:
        self._log("uow.begin")
        if self._begin_raises is not None:
            raise self._begin_raises

    async def commit(self) -> None:
        self._log("uow.commit")
        if self._commit_raises is not None:
            raise self._commit_raises

    async def rollback(self) -> None:
        self._log("uow.rollback")

    async def __aenter__(self) -> _StubUoW:
        await self.begin()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        try:
            if exc_type is None:
                await self._commit_or_rollback()
            else:
                await self.rollback()
        finally:
            self._log("uow.closed")

    async def _commit_or_rollback(self) -> None:
        try:
            await self.commit()
        except Exception:
            await self.rollback()
            raise


class _StubUoWFactory:
    def __init__(self, log: Log, **options: Any) -> None:
        self._log = log
        self._options = options
        self.created: list[_StubUoW] = []

    def create(self) -> UnitOfWork:
        uow = _StubUoW(self._log, **self._options)
        self.created.append(uow)
        return uow


class _FakeSession:
    def __init__(self, log: Log) -> None:
        self._log = log

    async def commit(self) -> None:
        self._log("session.commit")

    async def rollback(self) -> None:
        self._log("session.rollback")


class _FakeSessionCM:
    def __init__(self, log: Log) -> None:
        self._log = log
        self.session = _FakeSession(log)
        self.exits = 0

    async def __aenter__(self) -> _FakeSession:
        self._log("session_cm.enter")
        return self.session

    async def __aexit__(self, *args: object) -> None:
        self._log("session_cm.exit")
        self.exits += 1


class _FakeSessionManager:
    def __init__(self, log: Log) -> None:
        self._log = log
        self.cms: list[_FakeSessionCM] = []

    def session(self) -> _FakeSessionCM:
        cm = _FakeSessionCM(self._log)
        self.cms.append(cm)
        return cm


def _executor(
    factory: _StubUoWFactory | SQLAlchemyUnitOfWorkFactory | None = None,
    metrics: Metrics | None = None,
) -> RuntimeExecutor:
    return RuntimeExecutor(UseCaseCompiler(), uow_factory=factory, metrics=metrics)


# ---------------------------------------------------------------------------
# F01 — the SQLAlchemy session context exits exactly once and nothing leaks
# ---------------------------------------------------------------------------


class TestF01SessionClosed:
    def _sqlalchemy_executor(self, log: Log) -> tuple[RuntimeExecutor, _FakeSessionManager]:
        manager = _FakeSessionManager(log)
        # The fake stands in for the real SessionManager.
        factory = SQLAlchemyUnitOfWorkFactory(manager)  # type: ignore[arg-type]
        return _executor(factory), manager

    async def test_commit_path_exits_the_session_once_and_unbinds_everything(self) -> None:
        log = Log()
        executor, manager = self._sqlalchemy_executor(log)

        await executor.execute(Ok(), params={"value": "x"})

        assert [cm.exits for cm in manager.cms] == [1]
        assert log.entries == ["session_cm.enter", "session.commit", "session_cm.exit"]
        assert context_is_clean()

    async def test_rollback_path_exits_the_session_once_and_unbinds_everything(self) -> None:
        log = Log()
        executor, manager = self._sqlalchemy_executor(log)

        with pytest.raises(RuntimeError, match="boom"):
            await executor.execute(Boom(), params={"value": "x"})

        assert [cm.exits for cm in manager.cms] == [1]
        assert log.entries == ["session_cm.enter", "session.rollback", "session_cm.exit"]
        assert context_is_clean()

    async def test_cancellation_path_exits_the_session_once_and_unbinds_everything(
        self,
    ) -> None:
        log = Log()
        executor, manager = self._sqlalchemy_executor(log)

        await cancel_mid_flight(executor)

        assert [cm.exits for cm in manager.cms] == [1]
        assert log.entries == ["session_cm.enter", "session.rollback", "session_cm.exit"]
        assert context_is_clean()

    async def test_a_later_execution_opens_its_own_session(self) -> None:
        log = Log()
        executor, manager = self._sqlalchemy_executor(log)
        await executor.execute(Ok(), params={"value": "x"})

        await executor.execute(Ok(), params={"value": "y"})

        assert [cm.exits for cm in manager.cms] == [1, 1]
        assert get_active_session() is None


# ---------------------------------------------------------------------------
# F02 — dispatches run after the close, in every execution shape
# ---------------------------------------------------------------------------


class TestF02DispatchAfterClose:
    async def test_without_unit_of_work_the_dispatch_is_sent_at_the_end(self) -> None:
        log = Log()
        broker = Broker(log)

        await _executor().execute(Dispatching(broker), params={"value": "job-A"})

        assert broker.sent == ["job-A"]
        assert log.entries == ["dispatch.queued(job-A)", "broker.send(job-A) uow=False"]
        assert active_channel() is None

    async def test_read_only_execution_sends_at_the_end_and_leaks_nothing(self) -> None:
        log = Log()
        broker = Broker(log)
        executor = _executor(_StubUoWFactory(log))

        await executor.execute(Dispatching(broker), params={"value": "job-B"}, read_only=True)
        assert broker.sent == ["job-B"]

        await _executor(_StubUoWFactory(Log())).execute(Ok(), params={"value": "unrelated"})
        assert broker.sent == ["job-B"]

    async def test_under_a_unit_of_work_the_send_happens_after_the_close(self) -> None:
        log = Log()
        broker = Broker(log)
        executor = _executor(_StubUoWFactory(log))

        await executor.execute(Dispatching(broker), params={"value": "job-C"})

        assert log.entries == [
            "uow.begin",
            "dispatch.queued(job-C)",
            "uow.commit",
            "uow.closed",
            "broker.send(job-C) uow=False",
        ]

    async def test_failed_execution_discards_and_never_leaks_into_a_later_one(self) -> None:
        log = Log()
        broker = Broker(log)
        executor = _executor(_StubUoWFactory(log))

        with pytest.raises(RuntimeError, match="after dispatch"):
            await executor.execute(DispatchThenFail(broker), params={"value": "job-D"})
        await executor.execute(Ok(), params={"value": "later"})

        assert broker.sent == []
        assert "broker.send(job-D) uow=False" not in log.entries

    async def test_broker_failure_after_commit_is_a_post_commit_error(self) -> None:
        log = Log()
        broker = Broker(log)
        broker.failing.add("job-E")
        factory = _StubUoWFactory(log)
        metrics = Metrics(log)
        executor = _executor(factory, metrics)

        with pytest.raises(PostCommitError) as info:
            await executor.execute(Dispatching(broker), params={"value": "job-E"})

        assert info.value.committed is True
        assert [type(failure) for failure in info.value.failures] == [ConnectionError]
        assert "uow.rollback" not in log.entries
        assert metrics.kinds() == [EventKind.EXEC_START, EventKind.EXEC_DONE]
        assert log.entries.index("event.EXEC_DONE") < log.entries.index(
            "broker.send(job-E) uow=False"
        )


# ---------------------------------------------------------------------------
# F07 — the terminal event reflects the transaction outcome
# ---------------------------------------------------------------------------


class TestF07TerminalEvent:
    async def test_commit_failure_emits_exec_error_and_rolls_back_once(self) -> None:
        log = Log()
        metrics = Metrics(log)
        factory = _StubUoWFactory(log, commit_raises=RuntimeError("commit failed"))
        executor = _executor(factory, metrics)

        with pytest.raises(RuntimeError, match="commit failed"):
            await executor.execute(Ok(), params={"value": "x"})

        assert metrics.kinds() == [EventKind.EXEC_START, EventKind.EXEC_ERROR]
        error = metrics.only(EventKind.EXEC_ERROR)
        assert error.error_kind == "commit"
        assert error.pipeline_ms is not None
        assert error.commit_ms is not None
        assert error.duration_ms is not None
        assert log.entries.count("uow.rollback") == 1
        assert log.entries.index("uow.closed") < log.entries.index("event.EXEC_ERROR")

    async def test_begin_failure_emits_exec_start_then_exec_error(self) -> None:
        log = Log()
        metrics = Metrics(log)
        factory = _StubUoWFactory(log, begin_raises=ConnectionError("no database"))
        executor = _executor(factory, metrics)

        with pytest.raises(ConnectionError, match="no database"):
            await executor.execute(Ok(), params={"value": "x"})

        assert metrics.kinds() == [EventKind.EXEC_START, EventKind.EXEC_ERROR]
        assert metrics.only(EventKind.EXEC_ERROR).error_kind == "begin"
        assert log.entries == ["event.EXEC_START", "uow.begin", "event.EXEC_ERROR"]
        assert context_is_clean()

    async def test_business_failure_emits_exec_error_after_the_rollback(self) -> None:
        log = Log()
        metrics = Metrics(log)
        executor = _executor(_StubUoWFactory(log), metrics)

        with pytest.raises(RuntimeError, match="boom"):
            await executor.execute(Boom(), params={"value": "x"})

        error = metrics.only(EventKind.EXEC_ERROR)
        assert error.error_kind == "business"
        assert error.commit_ms is None
        assert log.entries == [
            "event.EXEC_START",
            "uow.begin",
            "uow.rollback",
            "uow.closed",
            "event.EXEC_ERROR",
        ]

    async def test_cancellation_emits_exec_error_with_error_kind_cancelled(self) -> None:
        log = Log()
        metrics = Metrics(log)
        executor = _executor(_StubUoWFactory(log), metrics)

        await cancel_mid_flight(executor)

        assert metrics.kinds() == [EventKind.EXEC_START, EventKind.EXEC_ERROR]
        assert metrics.only(EventKind.EXEC_ERROR).error_kind == "cancelled"
        assert "uow.rollback" in log.entries
        assert context_is_clean()

    async def test_exec_done_is_emitted_after_the_close_with_timings(self) -> None:
        log = Log()
        metrics = Metrics(log)
        executor = _executor(_StubUoWFactory(log), metrics)

        await executor.execute(Ok(), params={"value": "x"})

        assert log.entries == [
            "event.EXEC_START",
            "uow.begin",
            "uow.commit",
            "uow.closed",
            "event.EXEC_DONE",
        ]
        done = metrics.only(EventKind.EXEC_DONE)
        assert done.pipeline_ms is not None
        assert done.commit_ms is not None
        assert done.duration_ms is not None
        assert done.duration_ms >= done.pipeline_ms

    async def test_without_unit_of_work_exec_done_carries_no_commit_time(self) -> None:
        metrics = Metrics()

        await _executor(metrics=metrics).execute(Ok(), params={"value": "x"})

        done = metrics.only(EventKind.EXEC_DONE)
        assert done.pipeline_ms is not None
        assert done.commit_ms is None


# ---------------------------------------------------------------------------
# Nested executions — channel ownership follows unit-of-work ownership
# ---------------------------------------------------------------------------


class TestNestedExecutions:
    async def test_inner_joins_the_outer_unit_of_work_and_channel(self) -> None:
        log = Log()
        broker = Broker(log)
        factory = _StubUoWFactory(log)
        executor = _executor(factory)

        class _Outer(UseCase[Any, str]):
            async def execute(self, value: str) -> str:
                inner = await executor.execute(Dispatching(broker), params={"value": "inner"})
                broker.dispatch("outer")
                return inner

        await executor.execute(_Outer(), params={"value": "x"})

        assert len(factory.created) == 1
        assert log.entries == [
            "uow.begin",
            "dispatch.queued(inner)",
            "dispatch.queued(outer)",
            "uow.commit",
            "uow.closed",
            "broker.send(inner) uow=False",
            "broker.send(outer) uow=False",
        ]

    async def test_inner_with_its_own_unit_of_work_drains_right_after_its_close(self) -> None:
        log = Log()
        broker = Broker(log)
        factory = _StubUoWFactory(log)
        executor = _executor(factory)

        class _ReadOnlyOuter(UseCase[Any, str]):
            read_only = True

            async def execute(self, value: str) -> str:
                await executor.execute(Dispatching(broker), params={"value": "inner"})
                log("outer.after_inner")
                raise RuntimeError("outer fails after the inner committed")

        with pytest.raises(RuntimeError, match="outer fails"):
            await executor.execute(_ReadOnlyOuter(), params={"value": "x"})

        assert broker.sent == ["inner"]
        assert log.entries == [
            "uow.begin",
            "dispatch.queued(inner)",
            "uow.commit",
            "uow.closed",
            "broker.send(inner) uow=False",
            "outer.after_inner",
        ]

    async def test_an_action_that_executes_a_use_case_opens_its_own_lifecycle(self) -> None:
        log = Log()
        factory = _StubUoWFactory(log)
        executor = _executor(factory)

        class _FromAction(UseCase[Any, str]):
            async def execute(self, value: str) -> str:
                async def run_again() -> None:
                    await executor.execute(Ok(), params={"value": "again"})

                add_pending_dispatch(run_again)
                return value

        await executor.execute(_FromAction(), params={"value": "x"})

        assert len(factory.created) == 2
        assert log.entries == ["uow.begin", "uow.commit", "uow.closed"] * 2
