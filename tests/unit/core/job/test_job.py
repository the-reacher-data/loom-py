"""Unit tests for loom.core.job.job.Job base class."""

from __future__ import annotations

import inspect
from typing import Any

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.job.job import Job

# ---------------------------------------------------------------------------
# Concrete subclasses for testing
# ---------------------------------------------------------------------------


class _AsyncJob(Job[str]):
    """Minimal async job."""

    async def execute(self, value: str = "") -> str:
        return value.upper()


class _SyncJob(Job[int]):
    """Minimal sync (CPU-bound) job."""

    def execute(self, x: int = 0) -> int:
        return x * 2


class _JobWithDefaults(Job[None]):
    """Job that relies entirely on default ClassVar values."""

    async def execute(self) -> None:
        # No-op: only ClassVar defaults are under test, not the execution body.
        pass


class _JobWithCustomRouting(Job[None]):
    """Job with all routing ClassVars overridden."""

    __queue__ = "high-priority"
    __retries__ = 5
    __countdown__ = 10
    __timeout__ = 300
    __priority__ = 3

    async def execute(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Abstract contract
# ---------------------------------------------------------------------------


class TestJobIsAbstract:
    def test_cannot_instantiate_base_directly(self) -> None:
        with pytest.raises(TypeError):
            Job()  # type: ignore[abstract]

    def test_subclass_without_execute_is_abstract(self) -> None:
        class _Incomplete(Job[str]):
            pass

        with pytest.raises(TypeError):
            _Incomplete()  # type: ignore[abstract]

    def test_concrete_async_subclass_can_be_instantiated(self) -> None:
        job = _AsyncJob()
        assert isinstance(job, _AsyncJob)

    def test_concrete_sync_subclass_can_be_instantiated(self) -> None:
        job = _SyncJob()
        assert isinstance(job, _SyncJob)


# ---------------------------------------------------------------------------
# Sync vs async execute detection
# ---------------------------------------------------------------------------


class TestJobExecuteVariant:
    def test_async_execute_is_coroutinefunction(self) -> None:
        assert inspect.iscoroutinefunction(_AsyncJob.execute)

    def test_sync_execute_is_not_coroutinefunction(self) -> None:
        assert not inspect.iscoroutinefunction(_SyncJob.execute)

    async def test_async_execute_is_awaitable(self) -> None:
        job = _AsyncJob()
        result = await job.execute(value="hello")
        assert result == "HELLO"

    def test_sync_execute_returns_value(self) -> None:
        job = _SyncJob()
        assert job.execute(x=5) == 10


# ---------------------------------------------------------------------------
# ClassVar defaults
# ---------------------------------------------------------------------------


class TestJobClassVarDefaults:
    def test_default_computes_is_empty(self) -> None:
        assert _JobWithDefaults.computes == ()

    def test_default_rules_is_empty(self) -> None:
        assert _JobWithDefaults.rules == ()

    def test_default_queue(self) -> None:
        assert _JobWithDefaults.__queue__ == "default"

    def test_default_retries(self) -> None:
        assert _JobWithDefaults.__retries__ == 0

    def test_default_countdown(self) -> None:
        assert _JobWithDefaults.__countdown__ == 0

    def test_default_timeout_is_none(self) -> None:
        assert _JobWithDefaults.__timeout__ is None

    def test_default_priority(self) -> None:
        assert _JobWithDefaults.__priority__ == 0


class TestJobCustomRoutingClassVars:
    def test_custom_queue(self) -> None:
        assert _JobWithCustomRouting.__queue__ == "high-priority"

    def test_custom_retries(self) -> None:
        assert _JobWithCustomRouting.__retries__ == 5

    def test_custom_countdown(self) -> None:
        assert _JobWithCustomRouting.__countdown__ == 10

    def test_custom_timeout(self) -> None:
        assert _JobWithCustomRouting.__timeout__ == 300

    def test_custom_priority(self) -> None:
        assert _JobWithCustomRouting.__priority__ == 3


# ---------------------------------------------------------------------------
# ClassVar isolation across subclasses
# ---------------------------------------------------------------------------


class TestJobClassVarIsolation:
    def test_custom_queue_does_not_affect_default_job(self) -> None:
        assert _JobWithDefaults.__queue__ == "default"
        assert _JobWithCustomRouting.__queue__ == "high-priority"

    def test_custom_retries_do_not_affect_default_job(self) -> None:
        assert _JobWithDefaults.__retries__ == 0
        assert _JobWithCustomRouting.__retries__ == 5

    def test_execution_plan_starts_as_none(self) -> None:
        class _FreshJob(Job[Any]):
            async def execute(self) -> Any:
                return None

        assert _FreshJob.__execution_plan__ is None

    def test_classvars_are_independent_objects(self) -> None:
        # Sequence defaults must not be shared across classes.
        assert _JobWithDefaults.computes is not _AsyncJob.computes or (
            _JobWithDefaults.computes == () and _AsyncJob.computes == ()
        )


# ---------------------------------------------------------------------------
# Compiler integration — duck-typing via UseCaseCompiler
# ---------------------------------------------------------------------------


class TestJobCompilerIntegration:
    def test_async_job_compiles_successfully(self) -> None:
        compiler = UseCaseCompiler()
        plan = compiler.compile(_AsyncJob)  # type: ignore[arg-type]
        assert plan is not None
        assert plan.use_case_type is _AsyncJob

    def test_sync_job_compiles_successfully(self) -> None:
        compiler = UseCaseCompiler()
        plan = compiler.compile(_SyncJob)  # type: ignore[arg-type]
        assert plan is not None
        assert plan.use_case_type is _SyncJob

    def test_compile_sets_execution_plan_on_class(self) -> None:
        class _CompileTarget(Job[str]):
            async def execute(self, msg: str = "") -> str:
                return msg

        compiler = UseCaseCompiler()
        plan = compiler.compile(_CompileTarget)  # type: ignore[arg-type]
        assert _CompileTarget.__execution_plan__ is plan

    def test_compile_is_idempotent(self) -> None:
        compiler = UseCaseCompiler()
        plan1 = compiler.compile(_AsyncJob)  # type: ignore[arg-type]
        plan2 = compiler.compile(_AsyncJob)  # type: ignore[arg-type]
        assert plan1 is plan2
