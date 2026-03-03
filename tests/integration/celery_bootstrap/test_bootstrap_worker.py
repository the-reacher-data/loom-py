"""Integration tests for bootstrap_worker() (Piece 8)."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import yaml
from celery import Celery  # type: ignore[import-untyped]

import loom.celery.bootstrap as boot
import loom.celery.runner as _runner
from loom.celery.bootstrap import WorkerBootstrapResult, bootstrap_worker
from loom.core.job.job import Job

# ---------------------------------------------------------------------------
# Jobs used in tests
# ---------------------------------------------------------------------------


class _DoubleSyncJob(Job[int]):
    """Simple sync job with no infrastructure dependencies."""

    __queue__ = "default"
    __retries__ = 0
    __countdown__ = 0
    __timeout__ = None
    __priority__ = 0

    def execute(self, value: int = 0) -> int:  # type: ignore[override]
        return value * 2


class _UpperJob(Job[str]):
    """Sync job that upper-cases a string — no dependencies."""

    __queue__ = "default"
    __retries__ = 0
    __countdown__ = 0
    __timeout__ = None
    __priority__ = 0

    def execute(self, text: str = "") -> str:  # type: ignore[override]
        return text.upper()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def worker_config(tmp_path: Path) -> str:
    """Write a minimal worker YAML with task_always_eager enabled."""
    cfg: dict[str, Any] = {
        "celery": {
            "broker_url": "memory://",
            "result_backend": "cache+memory://",
            "task_always_eager": True,
        },
        "database": {"url": "sqlite+aiosqlite:///test_worker.db"},
    }
    config_file = tmp_path / "worker.yaml"
    config_file.write_text(yaml.dump(cfg))
    return str(config_file)


@pytest.fixture(autouse=True)
def reset_worker_session() -> Iterator[None]:
    """Reset module-level _worker_session around each test."""
    boot._worker_session.session_manager = None
    boot._worker_session.uow_factory = None
    yield
    # Only dispose real SessionManagers — mocks return non-coroutines from dispose()
    sm = boot._worker_session.session_manager
    if sm is not None and asyncio.iscoroutinefunction(sm.dispose):
        asyncio.run(sm.dispose())
    boot._worker_session.session_manager = None
    boot._worker_session.uow_factory = None


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestBootstrapWorkerResult:
    def test_returns_worker_bootstrap_result(self, worker_config: str) -> None:
        result = bootstrap_worker(worker_config, jobs=[_DoubleSyncJob])
        assert isinstance(result, WorkerBootstrapResult)

    def test_celery_app_is_celery_instance(self, worker_config: str) -> None:
        result = bootstrap_worker(worker_config, jobs=[_DoubleSyncJob])
        assert isinstance(result.celery_app, Celery)

    def test_celery_app_task_always_eager_is_set(self, worker_config: str) -> None:
        result = bootstrap_worker(worker_config, jobs=[_DoubleSyncJob])
        assert result.celery_app.conf.task_always_eager is True

    def test_result_is_frozen(self, worker_config: str) -> None:
        result = bootstrap_worker(worker_config, jobs=[_DoubleSyncJob])
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            result.container = None  # type: ignore[misc]

    def test_job_task_registered(self, worker_config: str) -> None:
        result = bootstrap_worker(worker_config, jobs=[_DoubleSyncJob])
        task_name = f"loom.job.{_DoubleSyncJob.__qualname__}"
        assert task_name in result.celery_app.tasks

    def test_multiple_jobs_all_registered(self, worker_config: str) -> None:
        result = bootstrap_worker(worker_config, jobs=[_DoubleSyncJob, _UpperJob])
        assert f"loom.job.{_DoubleSyncJob.__qualname__}" in result.celery_app.tasks
        assert f"loom.job.{_UpperJob.__qualname__}" in result.celery_app.tasks


class TestBootstrapWorkerTaskExecution:
    def test_sync_task_executes_with_task_always_eager(self, worker_config: str) -> None:
        """A registered sync task runs inline when task_always_eager is True."""
        result = bootstrap_worker(worker_config, jobs=[_DoubleSyncJob])
        task_name = f"loom.job.{_DoubleSyncJob.__qualname__}"
        task = result.celery_app.tasks[task_name]

        eager_result = task.apply(kwargs={"payload": {"value": 5}})
        assert eager_result.result == 10

    def test_second_sync_task_executes_correctly(self, worker_config: str) -> None:
        result = bootstrap_worker(worker_config, jobs=[_UpperJob])
        task_name = f"loom.job.{_UpperJob.__qualname__}"
        task = result.celery_app.tasks[task_name]

        eager_result = task.apply(kwargs={"payload": {"text": "hello"}})
        assert eager_result.result == "HELLO"


class TestBootstrapWorkerJobConfigOverride:
    def test_job_config_override_applied_before_task_registration(self, tmp_path: Path) -> None:
        """Retries override from YAML is visible to _make_job_task at registration.

        Querying celery_app.tasks[name].max_retries is unreliable when multiple
        Celery instances share the global task registry across tests.  Instead,
        spy on _make_job_task to capture job_type.__retries__ at call time.
        """
        original_retries = _DoubleSyncJob.__retries__
        captured_retries: list[int] = []

        def _spy(celery_app: Any, job_type: Any, *args: Any, **kwargs: Any) -> Any:
            captured_retries.append(job_type.__retries__)
            return _runner._make_job_task(celery_app, job_type, *args, **kwargs)

        try:
            cfg: dict[str, Any] = {
                "celery": {
                    "broker_url": "memory://",
                    "result_backend": "cache+memory://",
                    "task_always_eager": True,
                },
                "database": {"url": "sqlite+aiosqlite:///test_worker.db"},
                "jobs": {"_DoubleSyncJob": {"retries": 3}},
            }
            config_file = tmp_path / "worker_with_override.yaml"
            config_file.write_text(yaml.dump(cfg))

            with patch.object(boot, "_make_job_task", side_effect=_spy):
                bootstrap_worker(str(config_file), jobs=[_DoubleSyncJob])

            assert captured_retries == [3], (
                f"_make_job_task saw __retries__={captured_retries}; expected [3]"
            )
            assert _DoubleSyncJob.__retries__ == 3
        finally:
            _DoubleSyncJob.__retries__ = original_retries  # type: ignore[assignment]
