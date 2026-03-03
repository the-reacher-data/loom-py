"""CeleryJobService — Celery-backed implementation of the JobService protocol.

Dispatches Jobs to the broker post-commit by registering a sync callable
into the pending-dispatch queue managed by
:mod:`loom.core.job.context`.  No broker call is made inside
:meth:`~CeleryJobService.dispatch`; the actual ``send_task`` is deferred
and executed only after the Unit of Work commits successfully.

Usage::

    from loom.celery.service import CeleryJobService

    service = CeleryJobService(celery_app)

    class NotifyUsersUseCase(UseCase[User, None]):
        def __init__(self, jobs: JobService) -> None:
            self._jobs = jobs

        async def execute(self, cmd: NotifyCmd = Input()) -> None:
            self._jobs.dispatch(SendEmailJob, payload={"email": cmd.email})
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, TypeVar

from celery import Celery  # type: ignore[import-untyped]

from loom.core.job.context import add_pending_dispatch
from loom.core.job.handle import JobGroup, JobHandle
from loom.core.job.job import Job
from loom.core.tracing import get_trace_id

if TYPE_CHECKING:
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.job.callback import JobCallback

ResultT = TypeVar("ResultT")


# ---------------------------------------------------------------------------
# Pending send — replaces closures with a callable dataclass
# ---------------------------------------------------------------------------


@dataclass
class _PendingCeleryDispatch:
    """Deferred ``send_task`` call executed after UoW commits.

    Avoids nested function definitions (closures) by capturing all send
    context as explicit dataclass fields.  The instance is registered in
    the pending queue and called by
    :func:`~loom.core.job.context.flush_pending_dispatches`.
    """

    celery_app: Celery
    task_name: str
    task_id: str
    payload: dict[str, Any]
    params: dict[str, Any] | None
    trace_id: str | None
    queue: str
    countdown: int
    priority: int
    eta: datetime | None
    soft_time_limit: int | None
    link: Any | None
    link_error: Any | None

    def __call__(self) -> None:
        """Invoke ``send_task`` synchronously. Called by flush."""
        self.celery_app.send_task(
            self.task_name,
            kwargs={
                "payload": self.payload,
                "params": self.params,
                "trace_id": self.trace_id,
            },
            task_id=self.task_id,
            queue=self.queue,
            countdown=self.countdown,
            priority=self.priority,
            eta=self.eta,
            soft_time_limit=self.soft_time_limit,
            link=self.link,
            link_error=self.link_error,
        )


# ---------------------------------------------------------------------------
# Link builders — module-level, single responsibility each
# ---------------------------------------------------------------------------


def _build_success_link(
    celery_app: Celery,
    callback_type: type[Any],
    payload: dict[str, Any],
    task_id: str,
    trace_id: str | None = None,
) -> Any:
    """Build a Celery success-callback signature.

    The signature is attached as ``link`` on the parent task so Celery
    passes the task return value to the callback as its first argument.

    Args:
        celery_app: Celery application used to create the signature.
        callback_type: Callback class whose registered task name is used.
        payload: Dispatch payload forwarded as execution context.
        task_id: Parent task UUID for correlation.
        trace_id: Distributed trace ID propagated to the callback worker.

    Returns:
        Celery ``Signature`` with ``immutable=False``.
    """
    return celery_app.signature(
        f"loom.callback.{callback_type.__qualname__}",
        kwargs={"job_id": task_id, "context": payload, "trace_id": trace_id},
        immutable=False,
    )


def _build_failure_link(
    celery_app: Celery,
    callback_type: type[Any],
    payload: dict[str, Any],
    task_id: str,
    trace_id: str | None = None,
) -> Any:
    """Build a Celery failure-callback signature.

    The signature is attached as ``link_error`` on the parent task.
    Celery does not pass the task return value to error callbacks, so
    ``immutable=True`` is required.

    Args:
        celery_app: Celery application used to create the signature.
        callback_type: Callback class whose registered task name is used.
        payload: Dispatch payload forwarded as execution context.
        task_id: Parent task UUID for correlation.
        trace_id: Distributed trace ID propagated to the callback worker.

    Returns:
        Celery ``Signature`` with ``immutable=True``.
    """
    return celery_app.signature(
        f"loom.callback_error.{callback_type.__qualname__}",
        kwargs={"job_id": task_id, "context": payload, "trace_id": trace_id},
        immutable=True,
    )


# ---------------------------------------------------------------------------
# CeleryJobService
# ---------------------------------------------------------------------------


class CeleryJobService:
    """Celery-backed implementation of the ``JobService`` protocol.

    All dispatch calls are deferred until the current Unit of Work commits.
    Tasks are registered in the async context's pending queue via
    :func:`~loom.core.job.context.add_pending_dispatch` and flushed
    synchronously by
    :class:`~loom.core.engine.executor.RuntimeExecutor` after a successful
    commit.

    Callbacks are wired as native Celery ``link`` / ``link_error``
    signatures so the broker manages delivery without framework
    intervention after dispatch.

    .. note::
        :meth:`run` is not supported in Celery mode.  Use
        :class:`~loom.core.job.service.InlineJobService` for synchronous
        execution in tests or local mode.

    Args:
        celery_app: Configured Celery application instance.
        metrics: Optional metrics adapter.  Receives job lifecycle events
            once the new ``EventKind`` values from Piece 9 are wired in.

    Example::

        service = CeleryJobService(celery_app)
        handle = service.dispatch(SendEmailJob, payload={"user_id": 1})
        # actual send_task happens after UoW commit via flush
    """

    def __init__(
        self,
        celery_app: Celery,
        metrics: MetricsAdapter | None = None,
    ) -> None:
        self._app = celery_app
        self._metrics = metrics

    # ------------------------------------------------------------------
    # JobService protocol
    # ------------------------------------------------------------------

    async def run(
        self,
        job_type: type[Job[ResultT]],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> ResultT:
        """Not supported in Celery mode.

        Args:
            job_type: Unused.
            params: Unused.
            payload: Unused.

        Raises:
            NotImplementedError: Always.  Use :meth:`dispatch` for
                broker-backed execution or
                :class:`~loom.core.job.service.InlineJobService` for
                synchronous runs.
        """
        raise NotImplementedError(
            "CeleryJobService.run() is not supported. "
            "Use dispatch() for broker-backed execution, "
            "or InlineJobService.run() for synchronous execution."
        )

    def dispatch(
        self,
        job_type: type[Job[ResultT]],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        on_success: type[JobCallback] | None = None,
        on_failure: type[JobCallback] | None = None,
        queue: str | None = None,
        countdown: int | None = None,
        priority: int | None = None,
        eta: datetime | None = None,
    ) -> JobHandle[ResultT]:
        """Register a Job for deferred dispatch to the Celery broker.

        The job is NOT sent immediately.  A :class:`_PendingCeleryDispatch`
        is added to the async context's pending queue and executed after
        the current Unit of Work commits.  If the UoW rolls back, the
        pending entry is discarded and no task reaches the broker.

        The current trace ID is captured at registration time so it is
        propagated to the Celery worker even though the send happens later.

        Args:
            job_type: Concrete ``Job`` subclass to dispatch.
            params: Primitive params for the execute signature.
            payload: Raw dict for ``Input()`` command construction.
            on_success: Callback type invoked by the broker on task success.
            on_failure: Callback type invoked by the broker on task failure.
            queue: Override the job's ``__queue__`` for this dispatch only.
            countdown: Delay in seconds before first execution.  Defaults
                to ``job_type.__countdown__``.
            priority: Task priority.  Defaults to ``job_type.__priority__``.
            eta: Absolute datetime for first execution.  Mutually exclusive
                with ``countdown`` in Celery semantics.

        Returns:
            A :class:`~loom.core.job.handle.JobHandle` whose ``job_id``
            matches the ``task_id`` that will be passed to ``send_task``.
        """
        task_id = str(uuid.uuid4())
        trace_id = get_trace_id()
        eff_queue = queue or job_type.__queue__
        eff_countdown = countdown if countdown is not None else job_type.__countdown__
        eff_priority = priority if priority is not None else job_type.__priority__

        link = (
            _build_success_link(self._app, on_success, payload or {}, task_id, trace_id)
            if on_success
            else None
        )
        link_error = (
            _build_failure_link(self._app, on_failure, payload or {}, task_id, trace_id)
            if on_failure
            else None
        )

        pending = _PendingCeleryDispatch(
            celery_app=self._app,
            task_name=f"loom.job.{job_type.__qualname__}",
            task_id=task_id,
            payload=payload or {},
            params=params,
            trace_id=trace_id,
            queue=eff_queue,
            countdown=eff_countdown,
            priority=eff_priority,
            eta=eta,
            soft_time_limit=job_type.__timeout__,
            link=link,
            link_error=link_error,
        )
        add_pending_dispatch(pending)

        return JobHandle(
            job_id=task_id,
            queue=eff_queue,
            dispatched_at=datetime.now(UTC),
        )

    def dispatch_parallel(
        self,
        jobs: Sequence[tuple[type[Job[Any]], dict[str, Any]]],
        *,
        on_all_success: type[JobCallback] | None = None,
        on_any_failure: type[JobCallback] | None = None,
    ) -> JobGroup:
        """Dispatch multiple Jobs and return a group handle.

        Each job is dispatched independently via :meth:`dispatch` and
        will be executed in parallel by the available workers once the
        Unit of Work commits.

        ``on_all_success`` and ``on_any_failure`` require native Celery
        chord support which is not yet implemented.  Passing either raises
        :class:`NotImplementedError`.  Parallel dispatch without callbacks
        works as expected.

        Args:
            jobs: Sequence of ``(job_type, payload)`` pairs.
            on_all_success: Not yet supported.  Pass ``None``.
            on_any_failure: Not yet supported.  Pass ``None``.

        Returns:
            A :class:`~loom.core.job.handle.JobGroup` with one handle
            per dispatched job.

        Raises:
            NotImplementedError: When *on_all_success* or *on_any_failure*
                is provided.  Celery chord support is reserved for a
                future release.
        """
        if on_all_success is not None or on_any_failure is not None:
            raise NotImplementedError(
                "dispatch_parallel() callbacks (on_all_success / on_any_failure) "
                "require Celery chord support, which is not yet implemented. "
                "Pass None for both callback arguments."
            )
        handles = tuple(
            self.dispatch(job_type, payload=job_payload) for job_type, job_payload in jobs
        )
        return JobGroup(handles=handles)

    # ------------------------------------------------------------------
    # Metrics hook — wired by Piece 9
    # ------------------------------------------------------------------

    def _emit(self, event: Any) -> None:
        if self._metrics is not None:
            self._metrics.on_event(event)
