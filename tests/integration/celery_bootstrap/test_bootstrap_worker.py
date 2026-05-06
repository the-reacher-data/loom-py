"""Integration tests for bootstrap_worker() (Piece 8)."""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import yaml  # type: ignore[import-untyped]
from celery import Celery  # type: ignore[import-untyped]

import loom.celery.bootstrap as boot
import loom.celery.runner as _runner
from loom.celery.bootstrap import WorkerBootstrapResult, bootstrap_worker
from loom.celery.constants import TASK_JOB_PREFIX
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

    def execute(self, value: int = 0) -> int:
        return value * 2


class _UpperJob(Job[str]):
    """Sync job that upper-cases a string — no dependencies."""

    __queue__ = "default"
    __retries__ = 0
    __countdown__ = 0
    __timeout__ = None
    __priority__ = 0

    def execute(self, text: str = "") -> str:
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
        "database": {"url": f"sqlite+aiosqlite:///{tmp_path / 'test_worker.db'}"},
    }
    config_file = tmp_path / "worker.yaml"
    config_file.write_text(yaml.dump(cfg))
    return str(config_file)


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
            from typing import Any as _Any

            cast_result: _Any = result
            cast_result.container = None

    def test_job_task_registered(self, worker_config: str) -> None:
        result = bootstrap_worker(worker_config, jobs=[_DoubleSyncJob])
        task_name = f"{TASK_JOB_PREFIX}.{_DoubleSyncJob.__qualname__}"
        assert task_name in result.celery_app.tasks

    def test_multiple_jobs_all_registered(self, worker_config: str) -> None:
        result = bootstrap_worker(worker_config, jobs=[_DoubleSyncJob, _UpperJob])
        assert f"{TASK_JOB_PREFIX}.{_DoubleSyncJob.__qualname__}" in result.celery_app.tasks
        assert f"{TASK_JOB_PREFIX}.{_UpperJob.__qualname__}" in result.celery_app.tasks

    def test_celery_conf_has_json_serializer(self, worker_config: str) -> None:
        result = bootstrap_worker(worker_config, jobs=[_DoubleSyncJob])
        assert result.celery_app.conf.task_serializer == "json"
        assert result.celery_app.conf.result_serializer == "json"


class TestBootstrapWorkerTaskExecution:
    def test_sync_task_executes_with_task_always_eager(self, worker_config: str) -> None:
        """A registered sync task runs inline when task_always_eager is True.

        Primitive parameters (no ``Input()`` marker) must be passed via
        ``params``, not ``payload``.  ``payload`` is reserved for
        ``Input()`` command construction by the executor.
        """
        result = bootstrap_worker(worker_config, jobs=[_DoubleSyncJob])
        task_name = f"{TASK_JOB_PREFIX}.{_DoubleSyncJob.__qualname__}"
        task = result.celery_app.tasks[task_name]

        eager_result = task.apply(kwargs={"params": {"value": 5}})
        assert eager_result.result == 10

    def test_second_sync_task_executes_correctly(self, worker_config: str) -> None:
        result = bootstrap_worker(worker_config, jobs=[_UpperJob])
        task_name = f"{TASK_JOB_PREFIX}.{_UpperJob.__qualname__}"
        task = result.celery_app.tasks[task_name]

        eager_result = task.apply(kwargs={"params": {"text": "hello"}})
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
                "database": {"url": f"sqlite+aiosqlite:///{tmp_path / 'test_worker.db'}"},
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
            _DoubleSyncJob.__retries__ = original_retries
