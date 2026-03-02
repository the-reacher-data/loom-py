from __future__ import annotations

import pytest

from loom.core.job.job import Job
from loom.core.use_case.compute import ComputeFn
from loom.core.use_case.rule import RuleFn

# ---------------------------------------------------------------------------
# Concrete subclasses used across tests
# ---------------------------------------------------------------------------


class AsyncJob(Job[int]):
    """Minimal async job for testing."""

    async def execute(self, value: int) -> int:  # type: ignore[override]
        return value * 2


class SyncJob(Job[str]):
    """Minimal sync job for testing."""

    def execute(self, text: str) -> str:  # type: ignore[override]
        return text.upper()


class CustomQueueJob(Job[None]):
    __queue__ = "heavy"
    __retries__ = 5
    __countdown__ = 10
    __timeout__ = 600
    __priority__ = 3

    async def execute(self) -> None:  # type: ignore[override]
        pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_job_is_abstract() -> None:
    with pytest.raises(TypeError):
        Job()  # type: ignore[abstract]


def test_async_job_is_instantiable() -> None:
    job = AsyncJob()
    assert isinstance(job, Job)


def test_sync_job_is_instantiable() -> None:
    job = SyncJob()
    assert isinstance(job, Job)


def test_default_classvars_on_async_job() -> None:
    assert AsyncJob.__queue__ == "default"
    assert AsyncJob.__retries__ == 0
    assert AsyncJob.__countdown__ == 0
    assert AsyncJob.__timeout__ is None
    assert AsyncJob.__priority__ == 0


def test_custom_classvars_on_subclass() -> None:
    assert CustomQueueJob.__queue__ == "heavy"
    assert CustomQueueJob.__retries__ == 5
    assert CustomQueueJob.__countdown__ == 10
    assert CustomQueueJob.__timeout__ == 600
    assert CustomQueueJob.__priority__ == 3


def test_custom_classvars_do_not_affect_sibling_jobs() -> None:
    assert AsyncJob.__queue__ == "default"
    assert SyncJob.__queue__ == "default"


def test_execution_plan_defaults_to_none() -> None:
    assert AsyncJob.__execution_plan__ is None
    assert SyncJob.__execution_plan__ is None


def test_computes_defaults_to_empty_tuple() -> None:
    assert AsyncJob.computes == ()


def test_rules_defaults_to_empty_tuple() -> None:
    assert AsyncJob.rules == ()


def test_computes_and_rules_overridable_per_subclass() -> None:
    dummy_compute: ComputeFn[None] = object()  # type: ignore[assignment]
    dummy_rule: RuleFn = object()  # type: ignore[assignment]

    class RichJob(Job[None]):
        computes = (dummy_compute,)
        rules = (dummy_rule,)

        async def execute(self) -> None:  # type: ignore[override]
            pass

    assert RichJob.computes == (dummy_compute,)
    assert RichJob.rules == (dummy_rule,)
    assert AsyncJob.computes == ()
    assert AsyncJob.rules == ()
