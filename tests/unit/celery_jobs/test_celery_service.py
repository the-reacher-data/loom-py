"""Unit tests for CeleryJobService."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from loom.celery.service import CeleryJobService, _build_failure_link, _build_success_link
from loom.core.job.context import clear_pending_dispatches, flush_pending_dispatches
from loom.core.job.handle import JobGroup, JobHandle
from loom.core.job.job import Job

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _EmailJob(Job[None]):
    __queue__ = "email"
    __retries__ = 2
    __countdown__ = 0
    __timeout__ = None
    __priority__ = 0

    def execute(self) -> None:  # type: ignore[override]
        pass


class _HeavyJob(Job[int]):
    __queue__ = "heavy"
    __retries__ = 0
    __countdown__ = 10
    __timeout__ = 300
    __priority__ = 5

    def execute(self) -> int:  # type: ignore[override]
        return 42


class _SuccessCallback:
    def on_success(self, job_id: str, result: object, **ctx: object) -> None:
        pass

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **ctx: object) -> None:
        pass


class _FailureCallback:
    def on_success(self, job_id: str, result: object, **ctx: object) -> None:
        pass

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **ctx: object) -> None:
        pass


def _make_service() -> tuple[CeleryJobService, MagicMock]:
    mock_app = MagicMock()
    service = CeleryJobService(mock_app)
    return service, mock_app


@pytest.fixture(autouse=True)
def _clean_pending() -> pytest.FixtureRequest:  # type: ignore[return]
    """Isolate the pending-dispatch ContextVar for every test.

    Runs before and after each test (sync or async) to prevent leakage
    between tests that dispatch without flushing.
    """
    clear_pending_dispatches()
    yield  # type: ignore[misc]
    clear_pending_dispatches()


# ---------------------------------------------------------------------------
# dispatch() — deferred send
# ---------------------------------------------------------------------------


class TestDispatchDeferred:
    def test_dispatch_does_not_call_send_task_immediately(self) -> None:
        service, mock_app = _make_service()
        service.dispatch(_EmailJob, payload={"email": "a@b.com"})
        mock_app.send_task.assert_not_called()

    async def test_send_task_called_after_flush(self) -> None:
        service, mock_app = _make_service()
        service.dispatch(_EmailJob, payload={"email": "a@b.com"})
        await flush_pending_dispatches()
        mock_app.send_task.assert_called_once()

    async def test_send_task_receives_correct_task_name(self) -> None:
        service, mock_app = _make_service()
        service.dispatch(_EmailJob)
        await flush_pending_dispatches()
        assert mock_app.send_task.call_args.args[0] == "loom.job._EmailJob"

    async def test_send_task_receives_payload_in_kwargs(self) -> None:
        service, mock_app = _make_service()
        service.dispatch(_EmailJob, payload={"email": "x@y.com"})
        await flush_pending_dispatches()
        sent_kwargs = mock_app.send_task.call_args.kwargs["kwargs"]
        assert sent_kwargs["payload"] == {"email": "x@y.com"}

    async def test_send_task_receives_params_in_kwargs(self) -> None:
        service, mock_app = _make_service()
        service.dispatch(_EmailJob, params={"user_id": 7})
        await flush_pending_dispatches()
        sent_kwargs = mock_app.send_task.call_args.kwargs["kwargs"]
        assert sent_kwargs["params"] == {"user_id": 7}


# ---------------------------------------------------------------------------
# dispatch() — JobHandle identity
# ---------------------------------------------------------------------------


class TestDispatchHandleIdentity:
    async def test_handle_job_id_matches_send_task_task_id(self) -> None:
        service, mock_app = _make_service()
        handle = service.dispatch(_EmailJob)
        await flush_pending_dispatches()
        sent_task_id = mock_app.send_task.call_args.kwargs["task_id"]
        assert handle.job_id == sent_task_id

    def test_handle_queue_uses_job_classvar_by_default(self) -> None:
        service, _ = _make_service()
        handle = service.dispatch(_EmailJob)
        assert handle.queue == "email"

    async def test_handle_queue_respects_override(self) -> None:
        service, mock_app = _make_service()
        handle = service.dispatch(_EmailJob, queue="priority")
        assert handle.queue == "priority"
        await flush_pending_dispatches()
        assert mock_app.send_task.call_args.kwargs["queue"] == "priority"

    def test_handle_is_jobhandle_instance(self) -> None:
        service, _ = _make_service()
        handle = service.dispatch(_EmailJob)
        assert isinstance(handle, JobHandle)

    def test_each_dispatch_generates_unique_job_id(self) -> None:
        service, _ = _make_service()
        h1 = service.dispatch(_EmailJob)
        h2 = service.dispatch(_EmailJob)
        assert h1.job_id != h2.job_id


# ---------------------------------------------------------------------------
# dispatch() — routing ClassVar defaults
# ---------------------------------------------------------------------------


class TestDispatchRoutingDefaults:
    async def test_countdown_uses_job_classvar(self) -> None:
        service, mock_app = _make_service()
        service.dispatch(_HeavyJob)
        await flush_pending_dispatches()
        assert mock_app.send_task.call_args.kwargs["countdown"] == 10

    async def test_countdown_override_takes_precedence(self) -> None:
        service, mock_app = _make_service()
        service.dispatch(_HeavyJob, countdown=0)
        await flush_pending_dispatches()
        assert mock_app.send_task.call_args.kwargs["countdown"] == 0

    async def test_priority_uses_job_classvar(self) -> None:
        service, mock_app = _make_service()
        service.dispatch(_HeavyJob)
        await flush_pending_dispatches()
        assert mock_app.send_task.call_args.kwargs["priority"] == 5

    async def test_priority_override_takes_precedence(self) -> None:
        service, mock_app = _make_service()
        service.dispatch(_HeavyJob, priority=1)
        await flush_pending_dispatches()
        assert mock_app.send_task.call_args.kwargs["priority"] == 1

    async def test_soft_time_limit_uses_job_classvar(self) -> None:
        service, mock_app = _make_service()
        service.dispatch(_HeavyJob)
        await flush_pending_dispatches()
        assert mock_app.send_task.call_args.kwargs["soft_time_limit"] == 300

    async def test_soft_time_limit_none_when_no_timeout(self) -> None:
        service, mock_app = _make_service()
        service.dispatch(_EmailJob)
        await flush_pending_dispatches()
        assert mock_app.send_task.call_args.kwargs["soft_time_limit"] is None


# ---------------------------------------------------------------------------
# dispatch() — callbacks (link / link_error)
# ---------------------------------------------------------------------------


class TestDispatchCallbacks:
    async def test_no_callbacks_sends_link_none(self) -> None:
        service, mock_app = _make_service()
        service.dispatch(_EmailJob)
        await flush_pending_dispatches()
        assert mock_app.send_task.call_args.kwargs["link"] is None
        assert mock_app.send_task.call_args.kwargs["link_error"] is None

    async def test_on_success_produces_non_none_link(self) -> None:
        service, mock_app = _make_service()
        service.dispatch(_EmailJob, on_success=_SuccessCallback)
        await flush_pending_dispatches()
        assert mock_app.send_task.call_args.kwargs["link"] is not None

    async def test_on_failure_produces_non_none_link_error(self) -> None:
        service, mock_app = _make_service()
        service.dispatch(_EmailJob, on_failure=_FailureCallback)
        await flush_pending_dispatches()
        assert mock_app.send_task.call_args.kwargs["link_error"] is not None


# ---------------------------------------------------------------------------
# _build_success_link / _build_failure_link
# ---------------------------------------------------------------------------


class TestLinkBuilders:
    def test_success_link_uses_callback_qualname(self) -> None:
        mock_app = MagicMock()
        _build_success_link(mock_app, _SuccessCallback, {}, "uuid-123")
        name_arg = mock_app.signature.call_args.args[0]
        assert name_arg == "loom.callback._SuccessCallback"

    def test_success_link_is_not_immutable(self) -> None:
        mock_app = MagicMock()
        _build_success_link(mock_app, _SuccessCallback, {}, "uuid-123")
        kwargs = mock_app.signature.call_args.kwargs
        assert kwargs["immutable"] is False

    def test_failure_link_uses_callback_error_prefix(self) -> None:
        mock_app = MagicMock()
        _build_failure_link(mock_app, _FailureCallback, {}, "uuid-456")
        name_arg = mock_app.signature.call_args.args[0]
        assert name_arg == "loom.callback_error._FailureCallback"

    def test_failure_link_is_immutable(self) -> None:
        mock_app = MagicMock()
        _build_failure_link(mock_app, _FailureCallback, {}, "uuid-456")
        kwargs = mock_app.signature.call_args.kwargs
        assert kwargs["immutable"] is True

    def test_link_kwargs_contain_job_id(self) -> None:
        mock_app = MagicMock()
        _build_success_link(mock_app, _SuccessCallback, {"key": "val"}, "my-uuid")
        kwargs = mock_app.signature.call_args.kwargs["kwargs"]
        assert kwargs["job_id"] == "my-uuid"

    def test_link_kwargs_contain_context(self) -> None:
        mock_app = MagicMock()
        _build_success_link(mock_app, _SuccessCallback, {"order_id": 99}, "uuid")
        kwargs = mock_app.signature.call_args.kwargs["kwargs"]
        assert kwargs["context"] == {"order_id": 99}


# ---------------------------------------------------------------------------
# dispatch_parallel()
# ---------------------------------------------------------------------------


class TestDispatchParallel:
    def test_returns_jobgroup(self) -> None:
        service, _ = _make_service()
        group = service.dispatch_parallel([(_EmailJob, {"a": 1}), (_HeavyJob, {"b": 2})])
        assert isinstance(group, JobGroup)

    def test_group_has_one_handle_per_job(self) -> None:
        service, _ = _make_service()
        group = service.dispatch_parallel([(_EmailJob, {}), (_HeavyJob, {})])
        assert len(group.handles) == 2

    async def test_send_task_called_once_per_job_after_flush(self) -> None:
        service, mock_app = _make_service()
        service.dispatch_parallel([(_EmailJob, {}), (_HeavyJob, {})])
        await flush_pending_dispatches()
        assert mock_app.send_task.call_count == 2

    async def test_send_task_called_with_correct_task_names(self) -> None:
        service, mock_app = _make_service()
        service.dispatch_parallel([(_EmailJob, {}), (_HeavyJob, {})])
        await flush_pending_dispatches()
        names = {c.args[0] for c in mock_app.send_task.call_args_list}
        assert names == {"loom.job._EmailJob", "loom.job._HeavyJob"}

    def test_handles_have_unique_job_ids(self) -> None:
        service, _ = _make_service()
        group = service.dispatch_parallel([(_EmailJob, {}), (_EmailJob, {})])
        ids = [h.job_id for h in group.handles]
        assert len(set(ids)) == 2

    def test_does_not_call_send_task_before_flush(self) -> None:
        service, mock_app = _make_service()
        service.dispatch_parallel([(_EmailJob, {}), (_HeavyJob, {})])
        mock_app.send_task.assert_not_called()


# ---------------------------------------------------------------------------
# run() — not supported
# ---------------------------------------------------------------------------


class TestRunNotSupported:
    async def test_run_raises_not_implemented(self) -> None:
        service, _ = _make_service()
        with pytest.raises(NotImplementedError):
            await service.run(_EmailJob)

    async def test_run_error_message_mentions_dispatch(self) -> None:
        service, _ = _make_service()
        with pytest.raises(NotImplementedError, match="dispatch"):
            await service.run(_EmailJob)


# ---------------------------------------------------------------------------
# Pending queue isolation
# ---------------------------------------------------------------------------


class TestPendingQueueIsolation:
    async def test_clear_prevents_send_task_after_rollback(self) -> None:
        service, mock_app = _make_service()
        service.dispatch(_EmailJob)
        clear_pending_dispatches()
        await flush_pending_dispatches()
        mock_app.send_task.assert_not_called()

    async def test_flush_empties_queue(self) -> None:
        service, mock_app = _make_service()
        service.dispatch(_EmailJob)
        await flush_pending_dispatches()
        mock_app.reset_mock()
        await flush_pending_dispatches()
        mock_app.send_task.assert_not_called()
