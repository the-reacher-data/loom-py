"""Unit tests for loom.celery.bootstrap (Piece 8)."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

import loom.celery.bootstrap as boot
from loom.celery.bootstrap import (
    WorkerBootstrapResult,
    _apply_job_config_if_present,
    _connect_worker_signals,
    _DeferredUoWFactory,
)
from loom.core.job.job import Job

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
# Fixture: reset module-level _worker_session before/after each test
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_worker_session() -> Iterator[None]:
    """Ensure _worker_session is clean before and after each test."""
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
# _DeferredUoWFactory
# ---------------------------------------------------------------------------


class TestDeferredUoWFactory:
    def test_create_raises_when_not_initialized(self) -> None:
        factory = _DeferredUoWFactory()
        with pytest.raises(RuntimeError, match="Worker not initialized"):
            factory.create()

    def test_create_delegates_to_worker_session_factory(self) -> None:
        mock_uow = MagicMock()
        mock_factory = MagicMock()
        mock_factory.create.return_value = mock_uow

        boot._worker_session.uow_factory = mock_factory  # type: ignore[assignment]
        try:
            result = _DeferredUoWFactory().create()
            assert result is mock_uow
            mock_factory.create.assert_called_once()
        finally:
            boot._worker_session.uow_factory = None


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
    def _run(self, tmp_path: Any, extra_cfg: dict[str, Any] | None = None) -> WorkerBootstrapResult:
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

        return bootstrap_worker(str(config_path), jobs=[_SyncJob])

    def test_task_registered_with_correct_name(self, tmp_path: Any) -> None:
        result = self._run(tmp_path)
        expected = f"loom.job.{_SyncJob.__qualname__}"
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
        of job_type.__retries__ at the moment of registration.  Querying
        celery_app.tasks[name].max_retries is unreliable across tests because
        Celery maintains a global task registry shared between Celery instances.
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


# ---------------------------------------------------------------------------
# Celery worker signals
# ---------------------------------------------------------------------------


class TestWorkerSignals:
    def test_on_init_creates_session_manager_and_uow_factory(self) -> None:
        from celery.signals import worker_process_init  # type: ignore[import-untyped]

        with (
            patch.object(boot, "SessionManager") as mock_sm_cls,
            patch.object(boot, "SQLAlchemyUnitOfWorkFactory") as mock_factory_cls,
        ):
            mock_sm_instance = MagicMock()
            mock_sm_cls.return_value = mock_sm_instance
            mock_factory_cls.return_value = MagicMock()

            _connect_worker_signals("sqlite+aiosqlite:///test.db", False, True)
            worker_process_init.send(sender=None)

        assert boot._worker_session.session_manager is not None
        assert boot._worker_session.uow_factory is not None

    def test_on_shutdown_clears_worker_session(self) -> None:
        from celery.signals import (  # type: ignore[import-untyped]
            worker_process_init,
            worker_process_shutdown,
        )

        with (
            patch.object(boot, "SessionManager") as mock_sm_cls,
            patch.object(boot, "SQLAlchemyUnitOfWorkFactory") as mock_factory_cls,
            patch.object(boot, "asyncio") as mock_asyncio,
        ):
            mock_sm_instance = MagicMock()
            mock_sm_cls.return_value = mock_sm_instance
            mock_factory_cls.return_value = MagicMock()
            mock_asyncio.run = MagicMock()

            _connect_worker_signals("sqlite+aiosqlite:///test.db", False, True)
            worker_process_init.send(sender=None)

            assert boot._worker_session.session_manager is not None

            worker_process_shutdown.send(sender=None)

        assert boot._worker_session.session_manager is None
        assert boot._worker_session.uow_factory is None

    def test_on_shutdown_calls_asyncio_run_with_dispose(self) -> None:
        from celery.signals import (  # type: ignore[import-untyped]
            worker_process_init,
            worker_process_shutdown,
        )

        with (
            patch.object(boot, "SessionManager") as mock_sm_cls,
            patch.object(boot, "SQLAlchemyUnitOfWorkFactory") as mock_factory_cls,
            patch.object(boot, "asyncio") as mock_asyncio,
        ):
            mock_sm_instance = MagicMock()
            mock_sm_cls.return_value = mock_sm_instance
            mock_factory_cls.return_value = MagicMock()
            mock_asyncio.run = MagicMock()

            _connect_worker_signals("sqlite+aiosqlite:///test.db", False, True)
            worker_process_init.send(sender=None)
            worker_process_shutdown.send(sender=None)

        mock_asyncio.run.assert_called_once()
