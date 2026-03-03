"""Unit tests for Celery runner factories (Piece 7)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from loom.celery.runner import (
    _install_trace,
    _make_callback_error_task,
    _make_callback_task,
    _make_job_task,
    _resolve_error_info,
    _run_job,
    _uninstall_trace,
)
from loom.core.job.job import Job

# ---------------------------------------------------------------------------
# Test fixtures / helpers
# ---------------------------------------------------------------------------


class _SyncJob(Job[int]):
    __queue__ = "default"
    __retries__ = 2
    __countdown__ = 0
    __timeout__ = None
    __priority__ = 0

    def execute(self, value: int = 0) -> int:  # type: ignore[override]
        return value * 2


class _AsyncJob(Job[str]):
    __queue__ = "heavy"
    __retries__ = 1
    __countdown__ = 0
    __timeout__ = 300
    __priority__ = 0

    async def execute(self, msg: str = "") -> str:  # type: ignore[override]
        return msg.upper()


class _SyncCallback:
    def on_success(self, job_id: str, result: object, **ctx: object) -> None:
        pass  # intentional no-op stub

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **ctx: object) -> None:
        pass  # intentional no-op stub


class _AsyncCallback:
    async def on_success(self, job_id: str, result: object, **ctx: object) -> None:
        pass  # intentional no-op stub

    async def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **ctx: object) -> None:
        pass  # intentional no-op stub


def _mock_celery_app() -> MagicMock:
    app = MagicMock()
    app.task = MagicMock(side_effect=lambda **kw: lambda fn: fn)
    app.conf.task_always_eager = False
    return app


def _mock_factory(instance: object) -> MagicMock:
    factory = MagicMock()
    factory.build = MagicMock(return_value=instance)
    return factory


# ---------------------------------------------------------------------------
# Trace ID lifecycle
# ---------------------------------------------------------------------------


class TestTraceLifecycle:
    def test_install_returns_none_when_no_trace_id(self) -> None:
        token = _install_trace(None)
        assert token is None

    def test_install_returns_token_when_trace_id_given(self) -> None:
        token = _install_trace("abc123")
        assert token is not None
        _uninstall_trace(token)

    def test_uninstall_none_token_is_safe(self) -> None:
        _uninstall_trace(None)  # must not raise


# ---------------------------------------------------------------------------
# _run_job
# ---------------------------------------------------------------------------


class TestRunJob:
    async def test_returns_executor_result(self) -> None:
        instance = MagicMock()
        executor = MagicMock()
        executor.execute = AsyncMock(return_value="done")
        result = await _run_job(instance, payload={}, params=None, executor=executor)
        assert result == "done"

    async def test_flushes_pending_dispatches_on_success(self) -> None:
        instance = MagicMock()
        executor = MagicMock()
        executor.execute = AsyncMock(return_value=None)
        with patch(
            "loom.celery.runner.flush_pending_dispatches", new_callable=AsyncMock
        ) as mock_flush:
            await _run_job(instance, payload={}, params=None, executor=executor)
            mock_flush.assert_awaited_once()

    async def test_clears_pending_dispatches_on_failure(self) -> None:
        instance = MagicMock()
        executor = MagicMock()
        executor.execute = AsyncMock(side_effect=ValueError("boom"))
        with patch("loom.celery.runner.clear_pending_dispatches") as mock_clear:
            with pytest.raises(ValueError):
                await _run_job(instance, payload={}, params=None, executor=executor)
            mock_clear.assert_called_once()

    async def test_does_not_flush_on_failure(self) -> None:
        instance = MagicMock()
        executor = MagicMock()
        executor.execute = AsyncMock(side_effect=RuntimeError("fail"))
        with patch(
            "loom.celery.runner.flush_pending_dispatches", new_callable=AsyncMock
        ) as mock_flush:
            with pytest.raises(RuntimeError):
                await _run_job(instance, payload={}, params=None, executor=executor)
            mock_flush.assert_not_awaited()


# ---------------------------------------------------------------------------
# _make_job_task — task registration
# ---------------------------------------------------------------------------


class TestMakeJobTaskRegistration:
    def test_task_registered_with_correct_name_sync(self) -> None:
        app = MagicMock()
        registered_kwargs: dict = {}
        app.task = MagicMock(
            side_effect=lambda **kw: registered_kwargs.update(kw) or (lambda fn: fn)
        )
        _make_job_task(app, _SyncJob, MagicMock(), MagicMock())
        assert registered_kwargs["name"] == f"loom.job.{_SyncJob.__qualname__}"

    def test_task_registered_with_correct_name_async(self) -> None:
        app = MagicMock()
        registered_kwargs: dict = {}
        app.task = MagicMock(
            side_effect=lambda **kw: registered_kwargs.update(kw) or (lambda fn: fn)
        )
        _make_job_task(app, _AsyncJob, MagicMock(), MagicMock())
        assert registered_kwargs["name"] == f"loom.job.{_AsyncJob.__qualname__}"

    def test_acks_late_is_true(self) -> None:
        app = MagicMock()
        registered_kwargs: dict = {}
        app.task = MagicMock(
            side_effect=lambda **kw: registered_kwargs.update(kw) or (lambda fn: fn)
        )
        _make_job_task(app, _SyncJob, MagicMock(), MagicMock())
        assert registered_kwargs["acks_late"] is True

    def test_reject_on_worker_lost_is_true(self) -> None:
        app = MagicMock()
        registered_kwargs: dict = {}
        app.task = MagicMock(
            side_effect=lambda **kw: registered_kwargs.update(kw) or (lambda fn: fn)
        )
        _make_job_task(app, _SyncJob, MagicMock(), MagicMock())
        assert registered_kwargs["reject_on_worker_lost"] is True

    def test_max_retries_uses_job_classvar(self) -> None:
        app = MagicMock()
        registered_kwargs: dict = {}
        app.task = MagicMock(
            side_effect=lambda **kw: registered_kwargs.update(kw) or (lambda fn: fn)
        )
        _make_job_task(app, _SyncJob, MagicMock(), MagicMock())
        assert registered_kwargs["max_retries"] == _SyncJob.__retries__

    def test_soft_time_limit_uses_job_classvar(self) -> None:
        app = MagicMock()
        registered_kwargs: dict = {}
        app.task = MagicMock(
            side_effect=lambda **kw: registered_kwargs.update(kw) or (lambda fn: fn)
        )
        _make_job_task(app, _AsyncJob, MagicMock(), MagicMock())
        assert registered_kwargs["soft_time_limit"] == _AsyncJob.__timeout__


# ---------------------------------------------------------------------------
# _make_job_task — execution paths
# ---------------------------------------------------------------------------


def _mock_self(retries: int = 0, max_retries: int = 0) -> MagicMock:
    """Return a mock representing Celery's bound task self."""
    ms = MagicMock()
    ms.request.retries = retries
    ms.request.is_eager = False
    ms.max_retries = max_retries
    ms.app.conf.task_always_eager = False
    return ms


class TestMakeJobTaskExecution:
    def test_job_task_calls_worker_event_loop_run(self) -> None:
        """Every job (sync or async) is submitted to WorkerEventLoop.run()."""
        instance = _SyncJob()
        factory = _mock_factory(instance)
        task_fn = _make_job_task(_mock_celery_app(), _SyncJob, factory, MagicMock())
        with patch("loom.celery.runner.WorkerEventLoop") as mock_loop:
            mock_loop.run = MagicMock(return_value=10)
            task_fn(_mock_self(), payload={"value": 5})
            mock_loop.run.assert_called_once()
            _, kwargs = mock_loop.run.call_args
            assert kwargs["timeout"] is None

    def test_async_job_also_calls_worker_event_loop_run(self) -> None:
        instance = MagicMock()
        factory = _mock_factory(instance)
        executor = MagicMock()
        task_fn = _make_job_task(_mock_celery_app(), _AsyncJob, factory, executor)
        with patch("loom.celery.runner.WorkerEventLoop") as mock_loop:
            mock_loop.run = MagicMock(return_value="ok")
            task_fn(_mock_self(), payload={"msg": "hello"})
            mock_loop.run.assert_called_once()
            _, kwargs = mock_loop.run.call_args
            assert kwargs["timeout"] == pytest.approx(300.0)

    def test_task_returns_loop_run_result(self) -> None:
        instance = _SyncJob()
        factory = _mock_factory(instance)
        task_fn = _make_job_task(_mock_celery_app(), _SyncJob, factory, MagicMock())
        with patch("loom.celery.runner.WorkerEventLoop") as mock_loop:
            mock_loop.run = MagicMock(return_value=42)
            mock_loop.is_initialized = MagicMock(return_value=True)
            result = task_fn(_mock_self(), payload={"value": 4})
        assert result == 42

    def test_job_task_uses_asyncio_run_when_eager_and_loop_not_initialized(self) -> None:
        instance = _SyncJob()
        factory = _mock_factory(instance)
        task_fn = _make_job_task(_mock_celery_app(), _SyncJob, factory, MagicMock())
        task_self = _mock_self()
        task_self.request.is_eager = True

        with (
            patch("loom.celery.runner.WorkerEventLoop") as mock_loop,
            patch("loom.celery.runner.asyncio.run", return_value=10) as mock_asyncio_run,
        ):
            mock_loop.is_initialized = MagicMock(return_value=False)
            result = task_fn(task_self, payload={"value": 5})

        assert result == 10
        mock_loop.run.assert_not_called()
        mock_asyncio_run.assert_called_once()


# ---------------------------------------------------------------------------
# _make_job_task — retry logic
# ---------------------------------------------------------------------------


class TestMakeJobTaskRetry:
    def test_retries_when_below_max(self) -> None:
        instance = MagicMock()
        factory = _mock_factory(instance)

        mock_self = MagicMock()
        mock_self.request.retries = 0
        mock_self.max_retries = 2

        class RetryError(Exception):
            pass

        mock_self.retry = MagicMock(side_effect=RetryError)

        task_fn = _make_job_task(_mock_celery_app(), _SyncJob, factory, MagicMock())
        raw_fn = task_fn if callable(task_fn) else task_fn.__func__
        with patch("loom.celery.runner.WorkerEventLoop") as mock_loop:
            mock_loop.run = MagicMock(side_effect=ValueError("err"))
            with pytest.raises(RetryError):
                raw_fn(mock_self, payload={"value": 1})
        mock_self.retry.assert_called_once()

    def test_retry_countdown_uses_exponential_backoff(self) -> None:
        instance = MagicMock()
        factory = _mock_factory(instance)

        mock_self = MagicMock()
        mock_self.request.retries = 2  # third attempt → backoff^2
        mock_self.max_retries = 5

        class RetryError(Exception):
            pass

        mock_self.retry = MagicMock(side_effect=RetryError)

        task_fn = _make_job_task(_mock_celery_app(), _SyncJob, factory, MagicMock(), backoff=3)
        raw_fn = task_fn if callable(task_fn) else task_fn.__func__
        with patch("loom.celery.runner.WorkerEventLoop") as mock_loop:
            mock_loop.run = MagicMock(side_effect=ValueError("err"))
            with pytest.raises(RetryError):
                raw_fn(mock_self, payload={"value": 1})
        _, kwargs = mock_self.retry.call_args
        # backoff=3, retries=2 → countdown = 3**2 = 9
        assert kwargs["countdown"] == 9

    def test_raises_original_exc_when_retries_exhausted(self) -> None:
        instance = MagicMock()
        factory = _mock_factory(instance)

        mock_self = MagicMock()
        mock_self.request.retries = 2
        mock_self.max_retries = 2

        task_fn = _make_job_task(_mock_celery_app(), _SyncJob, factory, MagicMock())
        raw_fn = task_fn if callable(task_fn) else task_fn.__func__
        with patch("loom.celery.runner.WorkerEventLoop") as mock_loop:
            mock_loop.run = MagicMock(side_effect=ValueError("final"))
            with pytest.raises(ValueError, match="final"):
                raw_fn(mock_self, payload={"value": 1})
        mock_self.retry.assert_not_called()


# ---------------------------------------------------------------------------
# _resolve_error_info
# ---------------------------------------------------------------------------


class TestResolveErrorInfo:
    def test_returns_qualname_and_message_for_exception(self) -> None:
        mock_result = MagicMock()
        mock_result.result = ValueError("something went wrong")
        with patch("loom.celery.runner.AsyncResult", return_value=mock_result):
            exc_type, exc_msg = _resolve_error_info("some-task-id")
        assert exc_type == "ValueError"
        assert exc_msg == "something went wrong"

    def test_returns_unknown_when_result_is_not_exception(self) -> None:
        mock_result = MagicMock()
        mock_result.result = None
        with patch("loom.celery.runner.AsyncResult", return_value=mock_result):
            exc_type, exc_msg = _resolve_error_info("some-task-id")
        assert exc_type == "Unknown"
        assert exc_msg == ""


# ---------------------------------------------------------------------------
# _make_callback_task (on_success)
# ---------------------------------------------------------------------------


class TestMakeCallbackTask:
    def test_task_registered_with_correct_name(self) -> None:
        app = MagicMock()
        registered_name: list[str] = []
        app.task = MagicMock(
            side_effect=lambda **kw: registered_name.append(kw["name"]) or (lambda fn: fn)
        )
        _make_callback_task(app, _SyncCallback, MagicMock())
        task_name = next(iter(registered_name), None)
        assert task_name == f"loom.callback.{_SyncCallback.__qualname__}"

    def test_sync_on_success_called_with_result_and_job_id(self) -> None:
        cb = MagicMock(spec=_SyncCallback)
        factory = _mock_factory(cb)
        task_fn = _make_callback_task(_mock_celery_app(), _SyncCallback, factory)
        task_fn("result_value", job_id="uuid-1", context={})
        cb.on_success.assert_called_once_with(job_id="uuid-1", result="result_value")

    def test_sync_on_success_forwards_context_as_kwargs(self) -> None:
        cb = MagicMock(spec=_SyncCallback)
        factory = _mock_factory(cb)
        task_fn = _make_callback_task(_mock_celery_app(), _SyncCallback, factory)
        task_fn("r", job_id="uuid-2", context={"order_id": 99})
        _, kwargs = cb.on_success.call_args
        assert kwargs["order_id"] == 99

    def test_async_on_success_is_run_via_worker_event_loop(self) -> None:
        cb = MagicMock()
        cb.on_success = AsyncMock()
        factory = _mock_factory(cb)
        task_fn = _make_callback_task(_mock_celery_app(), _AsyncCallback, factory)
        with patch("loom.celery.runner.WorkerEventLoop") as mock_loop:
            mock_loop.run = MagicMock(return_value=None)
            mock_loop.is_initialized = MagicMock(return_value=True)
            task_fn("r", job_id="x", context={})
            mock_loop.run.assert_called_once()

    def test_async_on_success_uses_asyncio_run_when_eager_without_loop(self) -> None:
        cb = MagicMock()
        cb.on_success = AsyncMock()
        app = _mock_celery_app()
        app.conf.task_always_eager = True
        factory = _mock_factory(cb)
        task_fn = _make_callback_task(app, _AsyncCallback, factory)
        with (
            patch("loom.celery.runner.WorkerEventLoop") as mock_loop,
            patch("loom.celery.runner.asyncio.run", return_value=None) as mock_asyncio_run,
        ):
            mock_loop.is_initialized = MagicMock(return_value=False)
            task_fn("r", job_id="x", context={})
            mock_loop.run.assert_not_called()
            mock_asyncio_run.assert_called_once()


# ---------------------------------------------------------------------------
# _make_callback_error_task (on_failure)
# ---------------------------------------------------------------------------


class TestMakeCallbackErrorTask:
    def test_task_registered_with_correct_name(self) -> None:
        app = MagicMock()
        registered_name: list[str] = []
        app.task = MagicMock(
            side_effect=lambda **kw: registered_name.append(kw["name"]) or (lambda fn: fn)
        )
        _make_callback_error_task(app, _SyncCallback, MagicMock())
        task_name = next(iter(registered_name), None)
        assert task_name == f"loom.callback_error.{_SyncCallback.__qualname__}"

    def test_on_failure_called_with_exc_info_from_backend(self) -> None:
        cb = MagicMock(spec=_SyncCallback)
        factory = _mock_factory(cb)
        task_fn = _make_callback_error_task(_mock_celery_app(), _SyncCallback, factory)
        with patch(
            "loom.celery.runner._resolve_error_info",
            return_value=("ValueError", "bad input"),
        ):
            task_fn(job_id="uuid-3", context={})
        cb.on_failure.assert_called_once_with(
            job_id="uuid-3", exc_type="ValueError", exc_msg="bad input"
        )

    def test_on_failure_forwards_context_as_kwargs(self) -> None:
        cb = MagicMock(spec=_SyncCallback)
        factory = _mock_factory(cb)
        task_fn = _make_callback_error_task(_mock_celery_app(), _SyncCallback, factory)
        with patch("loom.celery.runner._resolve_error_info", return_value=("E", "m")):
            task_fn(job_id="uuid-4", context={"user_id": 7})
        _, kwargs = cb.on_failure.call_args
        assert kwargs["user_id"] == 7

    def test_async_on_failure_is_run_via_worker_event_loop(self) -> None:
        cb = MagicMock()
        cb.on_failure = AsyncMock()
        factory = _mock_factory(cb)
        task_fn = _make_callback_error_task(_mock_celery_app(), _AsyncCallback, factory)
        with (
            patch("loom.celery.runner._resolve_error_info", return_value=("E", "m")),
            patch("loom.celery.runner.WorkerEventLoop") as mock_loop,
        ):
            mock_loop.run = MagicMock()
            task_fn(job_id="uuid-5", context={})
            mock_loop.run.assert_called_once()
