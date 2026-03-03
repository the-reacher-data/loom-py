"""Celery task factories for Job and Callback execution on the worker side.

Each ``_make_*`` function registers one Celery task against a given
``celery_app`` and returns the task object.  They are called once during
worker bootstrap (see :func:`~loom.celery.bootstrap.bootstrap_worker`)
and never again.

All jobs — both sync and async ``execute()`` — are executed through the
:class:`~loom.core.engine.executor.RuntimeExecutor` via
:class:`~loom.celery.event_loop.WorkerEventLoop`.  This gives every job
access to the Unit of Work, injection markers (``Input()``, ``Load()``,
etc.), and the ability to dispatch further jobs post-commit.  It also
keeps a single, persistent asyncio event loop per worker process so
SQLAlchemy's async connection pool is reused across tasks.

Callbacks follow the same pattern: async ``on_success`` / ``on_failure``
methods are submitted to :class:`~loom.celery.event_loop.WorkerEventLoop`;
sync methods are called directly from the task thread.
"""

from __future__ import annotations

import asyncio
import time
from contextvars import Token
from typing import TYPE_CHECKING, Any

from celery import Celery  # type: ignore[import-untyped]
from celery.result import AsyncResult  # type: ignore[import-untyped]

from loom.celery.event_loop import WorkerEventLoop
from loom.core.engine.events import EventKind, RuntimeEvent
from loom.core.job.context import clear_pending_dispatches, flush_pending_dispatches
from loom.core.tracing import reset_trace_id, set_trace_id

if TYPE_CHECKING:
    from loom.core.engine.executor import RuntimeExecutor
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.job.job import Job
    from loom.core.use_case.factory import UseCaseFactory


# ---------------------------------------------------------------------------
# Metrics helpers
# ---------------------------------------------------------------------------


def _emit(metrics: MetricsAdapter | None, event: RuntimeEvent) -> None:
    """Forward *event* to *metrics* when an adapter is configured."""
    if metrics is not None:
        metrics.on_event(event)


def _job_event(
    kind: EventKind,
    use_case_name: str,
    trace_id: str | None,
    **kwargs: Any,
) -> RuntimeEvent:
    """Build a :class:`~loom.core.engine.events.RuntimeEvent` for a job lifecycle step."""
    return RuntimeEvent(kind=kind, use_case_name=use_case_name, trace_id=trace_id, **kwargs)


# ---------------------------------------------------------------------------
# Trace ID context guard
# ---------------------------------------------------------------------------


def _install_trace(trace_id: str | None) -> Token[str | None] | None:
    """Install *trace_id* into the current context and return the token.

    Returns ``None`` when *trace_id* is absent so the caller can skip the
    matching :func:`reset_trace_id` call.
    """
    return set_trace_id(trace_id) if trace_id else None


def _uninstall_trace(token: Token[str | None] | None) -> None:
    """Restore the trace context to its prior value using *token*."""
    if token is not None:
        reset_trace_id(token)


def _is_eager_request(task_self: Any) -> bool:
    request = getattr(task_self, "request", None)
    is_eager = getattr(request, "is_eager", None)
    if isinstance(is_eager, bool):
        return is_eager

    app = getattr(task_self, "app", None)
    conf = getattr(app, "conf", None)
    return bool(getattr(conf, "task_always_eager", False))


def _run_coroutine(
    coro: Any,
    *,
    timeout: float | None = None,
    eager_fallback: bool,
) -> Any:
    if WorkerEventLoop.is_initialized():
        return WorkerEventLoop.run(coro, timeout=timeout)

    if eager_fallback:
        if timeout is None:
            return asyncio.run(coro)

        async def _with_timeout() -> Any:
            return await asyncio.wait_for(coro, timeout=timeout)

        return asyncio.run(_with_timeout())

    close = getattr(coro, "close", None)
    if callable(close):
        close()
    raise RuntimeError(
        "WorkerEventLoop is not initialized and eager fallback is disabled. "
        "Use a Celery worker process or enable task_always_eager."
    )


# ---------------------------------------------------------------------------
# Job execution helper
# ---------------------------------------------------------------------------


async def _run_job(
    instance: Job[Any],
    *,
    payload: dict[str, Any],
    params: dict[str, Any] | None,
    executor: RuntimeExecutor,
) -> Any:
    """Execute a Job through the executor and flush pending dispatches.

    The executor opens a UoW, runs the compiled execution plan (handling
    both sync and async ``execute()`` methods), and commits.  Pending
    dispatches (jobs enqueued during execution) are flushed on success
    and discarded on failure so downstream tasks are never sent for a
    rolled-back transaction.

    Args:
        instance: Constructed Job instance.
        payload: Raw payload dict for command construction.
        params: Optional primitive params.
        executor: RuntimeExecutor that drives the ExecutionPlan.

    Returns:
        The value returned by ``execute()``.
    """
    try:
        result = await executor.execute(instance, params=params, payload=payload)
        await flush_pending_dispatches()
        return result
    except Exception:
        clear_pending_dispatches()
        raise


# ---------------------------------------------------------------------------
# Job runner factory
# ---------------------------------------------------------------------------


def _make_job_task(
    celery_app: Celery,
    job_type: type[Job[Any]],
    factory: UseCaseFactory,
    executor: RuntimeExecutor,
    metrics: MetricsAdapter | None = None,
    backoff: int = 2,
) -> Any:
    """Register and return a Celery task that executes *job_type* on the worker.

    The returned task is bound (``bind=True``) so it can access retry
    context via ``self``.  Exponential back-off uses ``backoff ** retries``
    seconds (default ``2 ** 0 = 1``, ``2 ** 1 = 2``, …).

    Both sync and async ``execute()`` methods are driven by the
    :class:`~loom.core.engine.executor.RuntimeExecutor` via
    :class:`~loom.celery.event_loop.WorkerEventLoop`, giving every job
    access to the full framework (UoW, injection markers, dispatch).

    Args:
        celery_app: Celery application to register the task on.
        job_type: Concrete :class:`~loom.core.job.job.Job` subclass.
        factory: Used to build the Job instance via DI.
        executor: RuntimeExecutor driving the compiled ExecutionPlan.
        metrics: Optional metrics adapter.  Events emitted in Piece 9.
        backoff: Base for exponential retry delay (seconds).  Defaults to 2.

    Returns:
        The registered Celery task object.
    """

    timeout_value = job_type.__timeout__
    run_timeout = float(timeout_value) if timeout_value is not None and timeout_value > 0 else None

    @celery_app.task(  # type: ignore[untyped-decorator]
        name=f"loom.job.{job_type.__qualname__}",
        bind=True,
        acks_late=True,
        reject_on_worker_lost=True,
        max_retries=job_type.__retries__,
        soft_time_limit=job_type.__timeout__,
    )
    def _job_task(
        self: Any,
        *,
        payload: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        trace_id: str | None = None,
    ) -> Any:
        name = job_type.__qualname__
        token = _install_trace(trace_id)
        _emit(metrics, _job_event(EventKind.JOB_STARTED, name, trace_id))
        t0 = time.monotonic()
        try:
            instance = factory.build(job_type)
            result = _run_coroutine(
                _run_job(
                    instance,
                    payload=payload or {},
                    params=params,
                    executor=executor,
                ),
                timeout=run_timeout,
                eager_fallback=_is_eager_request(self),
            )
            _emit(
                metrics,
                _job_event(
                    EventKind.JOB_SUCCEEDED,
                    name,
                    trace_id,
                    duration_ms=(time.monotonic() - t0) * 1000,
                    status="success",
                ),
            )
            return result
        except Exception as exc:
            if self.request.retries < self.max_retries:
                countdown = backoff**self.request.retries
                _emit(
                    metrics,
                    _job_event(
                        EventKind.JOB_RETRYING, name, trace_id, status="retrying", error=exc
                    ),
                )
                raise self.retry(exc=exc, countdown=countdown) from exc
            _emit(
                metrics,
                _job_event(
                    EventKind.JOB_EXHAUSTED,
                    name,
                    trace_id,
                    duration_ms=(time.monotonic() - t0) * 1000,
                    status="exhausted",
                    error=exc,
                ),
            )
            raise
        finally:
            _uninstall_trace(token)

    return _job_task


# ---------------------------------------------------------------------------
# Callback runner factories
# ---------------------------------------------------------------------------


def _make_callback_task(
    celery_app: Celery,
    callback_type: type[Any],
    factory: UseCaseFactory,
) -> Any:
    """Register and return a Celery task for the ``on_success`` callback.

    Celery passes the parent job's return value as the first positional
    argument (``result``) because the link signature uses ``immutable=False``.
    The callback receives ``job_id`` and ``context`` via kwargs.

    Async ``on_success`` methods are submitted to
    :class:`~loom.celery.event_loop.WorkerEventLoop`.

    Args:
        celery_app: Celery application to register the task on.
        callback_type: Concrete :class:`~loom.core.job.callback.JobCallback`
            subclass.
        factory: Used to build the callback instance via DI.

    Returns:
        The registered Celery task object.
    """
    import inspect

    @celery_app.task(name=f"loom.callback.{callback_type.__qualname__}")  # type: ignore[untyped-decorator]
    def _callback_task(
        result: Any,
        *,
        job_id: str,
        context: dict[str, Any],
        trace_id: str | None = None,
    ) -> None:
        token = _install_trace(trace_id)
        try:
            cb = factory.build(callback_type)
            if inspect.iscoroutinefunction(cb.on_success):
                _run_coroutine(
                    cb.on_success(job_id=job_id, result=result, **context),
                    eager_fallback=bool(getattr(celery_app.conf, "task_always_eager", False)),
                )
            else:
                cb.on_success(job_id=job_id, result=result, **context)
        finally:
            _uninstall_trace(token)

    return _callback_task


def _resolve_error_info(job_id: str) -> tuple[str, str]:
    """Look up the exception that caused a job to fail via the result backend.

    Falls back to empty strings if the backend does not hold the result or
    the stored value is not an exception (e.g. backend not configured).

    Args:
        job_id: Celery task UUID of the *failed* job.

    Returns:
        A ``(exc_type, exc_msg)`` tuple ready to forward to ``on_failure``.
    """
    exc = AsyncResult(job_id).result
    if isinstance(exc, Exception):
        return type(exc).__qualname__, str(exc)
    return "Unknown", ""


def _make_callback_error_task(
    celery_app: Celery,
    callback_type: type[Any],
    factory: UseCaseFactory,
) -> Any:
    """Register and return a Celery task for the ``on_failure`` callback.

    The link was registered with ``immutable=True`` so Celery does not
    prepend additional positional arguments.  Exception details are
    retrieved from the result backend via :func:`_resolve_error_info`; a
    configured backend is therefore required for full ``exc_type`` /
    ``exc_msg`` propagation.

    Async ``on_failure`` methods are submitted to
    :class:`~loom.celery.event_loop.WorkerEventLoop`.

    Args:
        celery_app: Celery application to register the task on.
        callback_type: Concrete :class:`~loom.core.job.callback.JobCallback`
            subclass.
        factory: Used to build the callback instance via DI.

    Returns:
        The registered Celery task object.
    """
    import inspect

    @celery_app.task(name=f"loom.callback_error.{callback_type.__qualname__}")  # type: ignore[untyped-decorator]
    def _callback_error_task(
        *,
        job_id: str,
        context: dict[str, Any],
        trace_id: str | None = None,
    ) -> None:
        token = _install_trace(trace_id)
        try:
            exc_type, exc_msg = _resolve_error_info(job_id)
            cb = factory.build(callback_type)
            if inspect.iscoroutinefunction(cb.on_failure):
                _run_coroutine(
                    cb.on_failure(
                        job_id=job_id,
                        exc_type=exc_type,
                        exc_msg=exc_msg,
                        **context,
                    ),
                    eager_fallback=bool(getattr(celery_app.conf, "task_always_eager", False)),
                )
            else:
                cb.on_failure(job_id=job_id, exc_type=exc_type, exc_msg=exc_msg, **context)
        finally:
            _uninstall_trace(token)

    return _callback_error_task
