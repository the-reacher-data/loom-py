"""Celery task factories for Job and Callback execution on the worker side.

Each ``_make_*`` function registers one Celery task against a given
``celery_app`` and returns the task object.  They are called once during
worker bootstrap (see :func:`~loom.celery.bootstrap.bootstrap_worker`)
and never again.

Async jobs are executed via :func:`asyncio.run` so the Celery worker
(typically using the ``prefork`` pool) can run them without a persistent
event loop.  Sync jobs are executed inline without spinning up an event loop.

Callbacks use the same async-detection logic: if ``on_success`` /
``on_failure`` is an ``async def``, it is awaited inside ``asyncio.run``;
otherwise it is called directly.
"""

from __future__ import annotations

import asyncio
import inspect
from contextvars import Token
from typing import TYPE_CHECKING, Any

from celery import Celery  # type: ignore[import-untyped]
from celery.result import AsyncResult  # type: ignore[import-untyped]

from loom.core.job.context import clear_pending_dispatches, flush_pending_dispatches
from loom.core.tracing import reset_trace_id, set_trace_id

if TYPE_CHECKING:
    from loom.core.engine.executor import RuntimeExecutor
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.job.job import Job
    from loom.core.uow.abc import UnitOfWorkFactory
    from loom.core.use_case.factory import UseCaseFactory


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


# ---------------------------------------------------------------------------
# Sync and async job execution helpers
# ---------------------------------------------------------------------------


def _run_sync_job(
    instance: Job[Any],
    *,
    payload: dict[str, Any],
    params: dict[str, Any] | None,
) -> Any:
    """Execute a synchronous Job directly without an event loop.

    The combined ``payload`` and ``params`` dicts are forwarded as kwargs to
    ``execute()``.  Sync Jobs should declare a flat, primitive signature and
    avoid framework injection markers (``Input()``, ``Load()``, etc.).

    Args:
        instance: Constructed Job instance.
        payload: Raw payload dict forwarded as kwargs.
        params: Optional primitive params forwarded as kwargs.

    Returns:
        The value returned by ``instance.execute()``.
    """
    kwargs: dict[str, Any] = {**(payload or {}), **(params or {})}
    return instance.execute(**kwargs)


async def _run_async_job(
    instance: Job[Any],
    *,
    payload: dict[str, Any],
    params: dict[str, Any] | None,
    executor: RuntimeExecutor,
) -> Any:
    """Execute an async Job through the executor and flush pending dispatches.

    The executor opens a UoW, runs the compiled execution plan, and commits.
    Pending dispatches (jobs enqueued during execution) are flushed on success
    and discarded on failure so downstream tasks are not sent for a rolled-back
    transaction.

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
    uow_factory: UnitOfWorkFactory,
    executor: RuntimeExecutor,
    metrics: MetricsAdapter | None = None,
    backoff: int = 2,
) -> Any:
    """Register and return a Celery task that executes *job_type* on the worker.

    The returned task is bound (``bind=True``) so it can access retry context
    via ``self``.  Exponential back-off uses ``backoff ** retries`` seconds
    (default ``2 ** 0 = 1``, ``2 ** 1 = 2``, …).

    Async ``execute()`` methods are driven by :func:`asyncio.run` + the
    RuntimeExecutor (which manages the UoW).  Sync ``execute()`` methods
    are called inline without an event loop.

    Args:
        celery_app: Celery application to register the task on.
        job_type: Concrete :class:`~loom.core.job.job.Job` subclass.
        factory: Used to build the Job instance via DI.
        uow_factory: Passed to the executor for UoW lifecycle management.
        executor: RuntimeExecutor driving the compiled ExecutionPlan.
        metrics: Optional metrics adapter.  Events emitted in Piece 9.
        backoff: Base for exponential retry delay (seconds).  Defaults to 2.

    Returns:
        The registered Celery task object.
    """
    is_async = inspect.iscoroutinefunction(job_type.execute)

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
        token = _install_trace(trace_id)
        # TODO(piece-9): emit JOB_STARTED via metrics
        try:
            instance = factory.build(job_type)
            if is_async:
                result = asyncio.run(
                    _run_async_job(
                        instance,
                        payload=payload or {},
                        params=params,
                        executor=executor,
                    )
                )
            else:
                result = _run_sync_job(instance, payload=payload or {}, params=params)
            # TODO(piece-9): emit JOB_SUCCEEDED via metrics
            return result
        except Exception as exc:
            if self.request.retries < self.max_retries:
                countdown = backoff**self.request.retries
                # TODO(piece-9): emit JOB_RETRYING via metrics
                raise self.retry(exc=exc, countdown=countdown) from exc
            # TODO(piece-9): emit JOB_EXHAUSTED via metrics
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
    uow_factory: UnitOfWorkFactory,
) -> Any:
    """Register and return a Celery task for the ``on_success`` callback.

    Celery passes the parent job's return value as the first positional
    argument (``result``) because the link signature uses ``immutable=False``.
    The callback receives ``job_id`` and ``context`` via kwargs.

    Async ``on_success`` methods are run inside :func:`asyncio.run`.

    Args:
        celery_app: Celery application to register the task on.
        callback_type: Concrete :class:`~loom.core.job.callback.JobCallback`
            subclass.
        factory: Used to build the callback instance via DI.
        uow_factory: Reserved for async callbacks that need a UoW.

    Returns:
        The registered Celery task object.
    """

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
                asyncio.run(cb.on_success(job_id=job_id, result=result, **context))
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

    The link was registered with ``immutable=True`` so Celery does not prepend
    additional positional arguments.  Exception details are retrieved from the
    result backend via :func:`_resolve_error_info`; a configured backend is
    therefore required for full ``exc_type`` / ``exc_msg`` propagation.

    Async ``on_failure`` methods are run inside :func:`asyncio.run`.

    Args:
        celery_app: Celery application to register the task on.
        callback_type: Concrete :class:`~loom.core.job.callback.JobCallback`
            subclass.
        factory: Used to build the callback instance via DI.

    Returns:
        The registered Celery task object.
    """

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
                asyncio.run(
                    cb.on_failure(
                        job_id=job_id,
                        exc_type=exc_type,
                        exc_msg=exc_msg,
                        **context,
                    )
                )
            else:
                cb.on_failure(job_id=job_id, exc_type=exc_type, exc_msg=exc_msg, **context)
        finally:
            _uninstall_trace(token)

    return _callback_error_task
