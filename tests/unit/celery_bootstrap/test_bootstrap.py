"""Unit tests for loom.celery.bootstrap (Piece 8)."""

from __future__ import annotations

import dataclasses
import sys
import types
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

import loom.celery.bootstrap as boot
from loom.celery.bootstrap import (
    WorkerBootstrapResult,
    WorkerManifest,
    _apply_job_config_if_present,
    _connect_worker_signals,
)
from loom.celery.constants import TASK_CALLBACK_ERROR_PREFIX, TASK_CALLBACK_PREFIX, TASK_JOB_PREFIX
from loom.core.job.job import Job
from loom.core.model import BaseModel, ColumnField

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _SyncJob(Job[int]):
    __queue__ = "default"
    __retries__ = 0
    __countdown__ = 0
    __timeout__ = None
    __priority__ = 0

    def execute(self, value: int = 0) -> int:  # type: ignore[override]
        return value * 2


class _DiscoveredModel(BaseModel):
    __tablename__ = "discovered_model_bootstrap_test"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=120)


class _ObservedCallback:
    def on_success(self, job_id: str, result: Any, **context: Any) -> None:
        del job_id, result, context

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: Any) -> None:
        del job_id, exc_type, exc_msg, context


def _make_raw_config(extra: dict[str, Any] | None = None) -> Any:
    """Build a minimal omegaconf DictConfig for bootstrap_worker tests."""
    from omegaconf import OmegaConf

    base = {
        "celery": {
            "broker_url": "memory://",
            "result_backend": "cache+memory://",
            "task_always_eager": True,
        },
        "database": {"url": "sqlite+aiosqlite:///test.db"},
    }
    if extra:
        base.update(extra)
    return OmegaConf.create(base)


# ---------------------------------------------------------------------------
# _apply_job_config_if_present
# ---------------------------------------------------------------------------


class TestApplyJobConfigIfPresent:
    def test_applies_override_when_section_exists(self) -> None:
        raw = _make_raw_config(extra={"jobs": {"_SyncJob": {"queue": "override", "retries": 5}}})
        original_queue = _SyncJob.__queue__
        original_retries = _SyncJob.__retries__
        try:
            _apply_job_config_if_present(raw, _SyncJob)
            assert _SyncJob.__queue__ == "override"
            assert _SyncJob.__retries__ == 5
        finally:
            _SyncJob.__queue__ = original_queue  # type: ignore[assignment]
            _SyncJob.__retries__ = original_retries  # type: ignore[assignment]

    def test_does_not_raise_when_section_absent(self) -> None:
        raw = _make_raw_config()
        # Must not raise even though jobs._SyncJob section is missing
        _apply_job_config_if_present(raw, _SyncJob)

    def test_classvar_unchanged_when_section_absent(self) -> None:
        raw = _make_raw_config()
        original = _SyncJob.__queue__
        _apply_job_config_if_present(raw, _SyncJob)
        assert _SyncJob.__queue__ == original


# ---------------------------------------------------------------------------
# WorkerBootstrapResult — immutability
# ---------------------------------------------------------------------------


class TestWorkerBootstrapResult:
    def test_frozen_prevents_mutation(self) -> None:
        result = WorkerBootstrapResult(
            container=MagicMock(),
            factory=MagicMock(),
            celery_app=MagicMock(),
        )
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            result.container = MagicMock()  # type: ignore[misc]


# ---------------------------------------------------------------------------
# bootstrap_worker — task registration
# ---------------------------------------------------------------------------


class TestBootstrapWorkerTaskRegistration:
    def _run(
        self,
        tmp_path: Any,
        extra_cfg: dict[str, Any] | None = None,
        *,
        jobs: tuple[type[Job[Any]], ...] | None = (_SyncJob,),
    ) -> WorkerBootstrapResult:
        """Write a minimal YAML config and call bootstrap_worker."""
        import yaml

        cfg: dict[str, Any] = {
            "celery": {
                "broker_url": "memory://",
                "result_backend": "cache+memory://",
                "task_always_eager": True,
            },
            "database": {"url": "sqlite+aiosqlite:///test.db"},
        }
        if extra_cfg:
            cfg.update(extra_cfg)

        config_path = tmp_path / "worker.yaml"
        config_path.write_text(yaml.dump(cfg))

        from loom.celery.bootstrap import bootstrap_worker

        kwargs: dict[str, Any] = {}
        if jobs is not None:
            kwargs["jobs"] = list(jobs)
        return bootstrap_worker(str(config_path), **kwargs)

    def test_task_registered_with_correct_name(self, tmp_path: Any) -> None:
        result = self._run(tmp_path)
        expected = f"{TASK_JOB_PREFIX}.{_SyncJob.__qualname__}"
        assert expected in result.celery_app.tasks

    def test_celery_app_is_configured(self, tmp_path: Any) -> None:
        result = self._run(tmp_path)
        assert result.celery_app.conf.task_always_eager is True

    def test_returns_valid_result_type(self, tmp_path: Any) -> None:
        result = self._run(tmp_path)
        assert isinstance(result, WorkerBootstrapResult)

    def test_applies_job_config_before_registration(self, tmp_path: Any) -> None:
        """JobConfig overrides must be applied before task registration.

        Verifies ordering by spying on _make_job_task and capturing the value
        of job_type.__retries__ at the moment of registration.
        """
        import loom.celery.runner as _runner

        original_retries = _SyncJob.__retries__
        captured_retries: list[int] = []

        def _spy(celery_app: Any, job_type: Any, *args: Any, **kwargs: Any) -> Any:
            captured_retries.append(job_type.__retries__)
            return _runner._make_job_task(celery_app, job_type, *args, **kwargs)

        try:
            with patch.object(boot, "_make_job_task", side_effect=_spy):
                self._run(tmp_path, extra_cfg={"jobs": {"_SyncJob": {"retries": 7}}})
            assert captured_retries == [7], (
                f"_make_job_task saw __retries__={captured_retries}; expected [7]"
            )
            assert _SyncJob.__retries__ == 7
        finally:
            _SyncJob.__retries__ = original_retries  # type: ignore[assignment]

    def test_pure_job_bootstrap_without_database_section(self, tmp_path: Any) -> None:
        """Database config is optional for jobs that do not access repositories."""
        import yaml

        cfg: dict[str, Any] = {
            "celery": {
                "broker_url": "memory://",
                "result_backend": "cache+memory://",
                "task_always_eager": True,
            }
        }
        config_path = tmp_path / "worker_no_db.yaml"
        config_path.write_text(yaml.dump(cfg))

        with patch.object(boot, "_connect_worker_signals") as mock_signals:
            result = boot.bootstrap_worker(str(config_path), jobs=[_SyncJob])

        assert isinstance(result, WorkerBootstrapResult)
        mock_signals.assert_not_called()

    def test_discovers_jobs_from_modules_when_jobs_not_passed(self, tmp_path: Any) -> None:
        cfg = {
            "app": {
                "discovery": {
                    "mode": "modules",
                    "modules": {"include": ["tests.unit.celery_bootstrap.test_bootstrap"]},
                }
            }
        }
        result = self._run(tmp_path, extra_cfg=cfg)
        expected = f"{TASK_JOB_PREFIX}.{_SyncJob.__qualname__}"
        assert expected in result.celery_app.tasks

    def test_discovers_jobs_from_manifest_when_jobs_not_passed(self, tmp_path: Any) -> None:
        module_name = "tests.unit.celery_bootstrap._manifest_jobs_for_test"
        module = types.ModuleType(module_name)
        module.MANIFEST = WorkerManifest(jobs=[_SyncJob])
        sys.modules[module_name] = module

        cfg = {"app": {"discovery": {"mode": "manifest", "manifest": {"module": module_name}}}}
        try:
            result = self._run(tmp_path, extra_cfg=cfg)
        finally:
            sys.modules.pop(module_name, None)

        expected = f"{TASK_JOB_PREFIX}.{_SyncJob.__qualname__}"
        assert expected in result.celery_app.tasks

    def test_discovers_jobs_from_manifest_lists_when_manifest_struct_missing(
        self, tmp_path: Any
    ) -> None:
        module_name = "tests.unit.celery_bootstrap._manifest_lists_for_test"
        module = types.ModuleType(module_name)
        module.JOBS = [_SyncJob]
        module.CALLBACKS = [_ObservedCallback]
        sys.modules[module_name] = module

        cfg = {"app": {"discovery": {"mode": "manifest", "manifest": {"module": module_name}}}}
        try:
            result = self._run(tmp_path, extra_cfg=cfg)
        finally:
            sys.modules.pop(module_name, None)

        assert f"{TASK_JOB_PREFIX}.{_SyncJob.__qualname__}" in result.celery_app.tasks
        assert f"{TASK_CALLBACK_PREFIX}.{_ObservedCallback.__qualname__}" in result.celery_app.tasks
        assert (
            f"{TASK_CALLBACK_ERROR_PREFIX}.{_ObservedCallback.__qualname__}"
            in result.celery_app.tasks
        )

    def test_discovers_callbacks_from_modules_when_jobs_not_passed(self, tmp_path: Any) -> None:
        cfg = {
            "app": {
                "discovery": {
                    "mode": "modules",
                    "modules": {"include": ["tests.unit.celery_bootstrap.test_bootstrap"]},
                }
            }
        }
        result = self._run(tmp_path, extra_cfg=cfg, jobs=None)
        assert f"{TASK_JOB_PREFIX}.{_SyncJob.__qualname__}" in result.celery_app.tasks
        assert f"{TASK_CALLBACK_PREFIX}.{_ObservedCallback.__qualname__}" in result.celery_app.tasks
        assert (
            f"{TASK_CALLBACK_ERROR_PREFIX}.{_ObservedCallback.__qualname__}"
            in result.celery_app.tasks
        )

    def test_registers_repo_mapping_for_discovered_models(self, tmp_path: Any) -> None:
        cfg = {
            "app": {
                "discovery": {
                    "mode": "modules",
                    "modules": {"include": ["tests.unit.celery_bootstrap.test_bootstrap"]},
                }
            }
        }
        result = self._run(tmp_path, extra_cfg=cfg, jobs=None)
        mapped_names = {model.__name__ for model in result.container._repo_bindings}
        assert "_DiscoveredModel" in mapped_names

    def test_raises_when_no_jobs_and_no_discovery(self, tmp_path: Any) -> None:
        with pytest.raises(RuntimeError):
            self._run(tmp_path, jobs=None)


# ---------------------------------------------------------------------------
# Celery worker signals
# ---------------------------------------------------------------------------


@pytest.fixture()
def isolated_celery_signals() -> Any:
    """Save and restore Celery worker signal receivers around each test.

    ``_connect_worker_signals`` registers closures with ``weak=False``.
    Without isolation, receivers accumulate across tests and assertions
    like ``assert_called_once()`` see multiple calls (one per handler).
    """
    from celery.signals import (  # type: ignore[import-untyped]
        worker_process_init,
        worker_process_shutdown,
    )

    saved_init = list(worker_process_init.receivers)
    saved_shutdown = list(worker_process_shutdown.receivers)
    # Start each test with a clean slate so accumulated handlers from prior
    # bootstrap_worker() calls do not inflate assertion call counts.
    worker_process_init.receivers[:] = []
    worker_process_shutdown.receivers[:] = []
    yield
    worker_process_init.receivers[:] = saved_init
    worker_process_shutdown.receivers[:] = saved_shutdown


class TestWorkerSignals:
    def test_on_init_calls_worker_event_loop_initialize(self, isolated_celery_signals: Any) -> None:
        from celery.signals import worker_process_init  # type: ignore[import-untyped]

        mock_sm = MagicMock()

        with patch.object(boot, "WorkerEventLoop") as mock_loop:
            _connect_worker_signals(mock_sm)
            worker_process_init.send(sender=None)

        mock_loop.initialize.assert_called_once()

    def test_on_shutdown_disposes_session_manager_via_loop(
        self, isolated_celery_signals: Any
    ) -> None:
        from celery.signals import (  # type: ignore[import-untyped]
            worker_process_init,
            worker_process_shutdown,
        )

        mock_sm = MagicMock()
        dispose_coro = MagicMock()
        mock_sm.dispose.return_value = dispose_coro

        with patch.object(boot, "WorkerEventLoop") as mock_loop:
            mock_loop.is_initialized.return_value = True
            _connect_worker_signals(mock_sm)
            worker_process_init.send(sender=None)
            worker_process_shutdown.send(sender=None)

        mock_loop.run.assert_called_once_with(dispose_coro)

    def test_on_shutdown_calls_worker_event_loop_shutdown(
        self, isolated_celery_signals: Any
    ) -> None:
        from celery.signals import (  # type: ignore[import-untyped]
            worker_process_init,
            worker_process_shutdown,
        )

        mock_sm = MagicMock()

        with patch.object(boot, "WorkerEventLoop") as mock_loop:
            mock_loop.is_initialized.return_value = True
            _connect_worker_signals(mock_sm)
            worker_process_init.send(sender=None)
            worker_process_shutdown.send(sender=None)

        mock_loop.shutdown.assert_called_once()

    def test_on_shutdown_skips_dispose_when_loop_not_initialized(
        self, isolated_celery_signals: Any
    ) -> None:
        from celery.signals import worker_process_shutdown  # type: ignore[import-untyped]

        mock_sm = MagicMock()

        with patch.object(boot, "WorkerEventLoop") as mock_loop:
            mock_loop.is_initialized.return_value = False
            _connect_worker_signals(mock_sm)
            worker_process_shutdown.send(sender=None)

        mock_sm.dispose.assert_not_called()
        mock_loop.run.assert_not_called()
        mock_loop.shutdown.assert_called_once()

    def test_on_shutdown_always_calls_shutdown_when_dispose_fails(
        self, isolated_celery_signals: Any
    ) -> None:
        from celery.signals import worker_process_shutdown  # type: ignore[import-untyped]

        mock_sm = MagicMock()
        dispose_coro = MagicMock()
        mock_sm.dispose.return_value = dispose_coro

        with patch.object(boot, "WorkerEventLoop") as mock_loop:
            mock_loop.is_initialized.return_value = True
            mock_loop.run.side_effect = RuntimeError("loop stopped")
            _connect_worker_signals(mock_sm)
            worker_process_shutdown.send(sender=None)

        mock_loop.run.assert_called_once_with(dispose_coro)
        dispose_coro.close.assert_called_once()
        mock_loop.shutdown.assert_called_once()
