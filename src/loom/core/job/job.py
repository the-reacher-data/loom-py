"""Distributable background unit of work.

A :class:`Job` is a self-contained, dispatachable unit of work — smaller in
scope than a :class:`~loom.core.use_case.UseCase` and not tied to a single
domain entity.  Jobs are orchestrated *by* UseCases (or other Jobs), not
triggered directly from HTTP.

The Celery runner detects whether ``execute`` is a coroutine at startup via
``inspect.iscoroutinefunction`` and dispatches accordingly, so both ``async
def`` and ``def`` implementations are supported without changes to the caller.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar

from loom.core.use_case.compute import ComputeFn
from loom.core.use_case.rule import RuleFn

if TYPE_CHECKING:
    from loom.core.engine.plan import ExecutionPlan

ResultT = TypeVar("ResultT")


class Job(ABC, Generic[ResultT]):
    """Base class for all distributable background units of work.

    Subclass and implement :meth:`execute` with a typed signature.  The
    method may be ``async def`` (I/O-bound) or ``def`` (CPU-bound) — the
    runner auto-detects the variant via ``inspect.iscoroutinefunction``.

    Class attributes ``computes`` and ``rules`` are processed by
    :class:`~loom.core.engine.compiler.UseCaseCompiler` at startup, exactly
    as for :class:`~loom.core.use_case.UseCase`.

    ClassVars prefixed with ``__`` configure Celery routing and retry
    behaviour.  Override them in subclasses to customise per-job:

    .. code-block:: python

        class SendWelcomeEmailJob(Job[None]):
            __queue__    = "emails"
            __retries__  = 3
            __countdown__ = 5

    Attributes:
        computes: Compute transformations applied in order before rule checks.
        rules: Rule validations applied in order after computes.
        __execution_plan__: Compiled plan cached by ``UseCaseCompiler``.
            ``None`` until first compilation.
        __queue__: Celery queue name.  Defaults to ``"default"``.
        __retries__: Maximum automatic retries on failure.  Defaults to ``0``.
        __countdown__: Seconds to delay before first execution.
            Defaults to ``0`` (immediate).
        __timeout__: Hard execution time-limit in seconds.
            ``None`` means no limit.
        __priority__: Celery task priority.  Defaults to ``0`` (normal).

    Example::

        class GenerateReportJob(Job[ReportResult]):
            __queue__    = "reports"
            __retries__  = 2
            __timeout__  = 120

            def __init__(self, report_repo: ReportRepository) -> None:
                self._report_repo = report_repo

            async def execute(self, report_id: int) -> ReportResult:
                data = await self._report_repo.get_data(report_id)
                return ReportResult.build(data)
    """

    __execution_plan__: ClassVar[ExecutionPlan | None] = None
    computes: ClassVar[Sequence[ComputeFn[Any]]] = ()
    rules: ClassVar[Sequence[RuleFn]] = ()

    __queue__: ClassVar[str] = "default"
    __retries__: ClassVar[int] = 0
    __countdown__: ClassVar[int] = 0
    __timeout__: ClassVar[int | None] = None
    __priority__: ClassVar[int] = 0

    @abstractmethod
    def execute(self, *args: Any, **kwargs: Any) -> Any:
        """Execute the job's core logic.

        Override with an explicit typed signature.  The compiler inspects this
        method once at startup to build the :class:`~loom.core.engine.plan.ExecutionPlan`.

        Returns:
            The result of the job.  Type is determined by ``ResultT``.
        """
        ...
