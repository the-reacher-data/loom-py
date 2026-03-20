"""ETL executor — walks a compiled plan and drives I/O + observability.

The executor is pure Python with no scheduling or broker dependencies.
It receives three injected collaborators:

* :class:`~loom.etl._io.SourceReader`     — reads sources into frames
* :class:`~loom.etl._io.TargetWriter`     — writes frames to targets
* :class:`~loom.etl.executor.ETLRunObserver` — receives lifecycle events

A thin Celery / Prefect task wrapper can call :meth:`ETLExecutor.run_step`
without any change to the executor itself.

Parallelism
-----------
Sequential nodes in a :class:`~loom.etl.compiler.ProcessPlan` run in order.
Nested lists (``ParallelStepGroup``, ``ParallelProcessGroup``) are dispatched
through the injected :class:`~loom.etl.executor.ParallelDispatcher`, defaulting
to :class:`~loom.etl.executor.ThreadDispatcher`.

Example::

    from loom.etl.compiler import ETLCompiler
    from loom.etl.executor import ETLExecutor, LoggingRunObserver

    plan = ETLCompiler().compile_step(DailyOrdersStep)
    executor = ETLExecutor(
        reader=MyDeltaReader(),
        writer=MyDeltaWriter(),
        observer=LoggingRunObserver(),
    )
    executor.run_step(plan, params=DailyOrdersParams(run_date=date.today()))
"""

from __future__ import annotations

import time
import uuid
from typing import Any

from loom.etl._io import SourceReader, TargetWriter
from loom.etl.compiler._plan import (
    ParallelProcessGroup,
    ParallelStepGroup,
    PipelinePlan,
    PipelineProcessNode,
    ProcessPlan,
    ProcessStepNode,
    StepPlan,
)
from loom.etl.executor._dispatcher import ParallelDispatcher, ThreadDispatcher
from loom.etl.executor._observer import (
    ETLRunObserver,
    NoopRunObserver,
    RunStatus,
)


class ETLExecutor:
    """Drives execution of compiled ETL plans.

    Responsibilities:

    * Read each source via the injected :class:`~loom.etl._io.SourceReader`.
    * Invoke the step's ``execute()`` with the resulting frames.
    * Write the result via the injected :class:`~loom.etl._io.TargetWriter`.
    * Emit lifecycle events to the :class:`~loom.etl.executor.ETLRunObserver`.
    * Dispatch parallel groups through the :class:`~loom.etl.executor.ParallelDispatcher`.

    All collaborators are injected — the executor has no dependency on any
    specific backend, broker, or observability system.

    Args:
        reader:     Source reader implementation (Delta, Polars, stub, etc.).
        writer:     Target writer implementation.
        observer:   Lifecycle observer.  Defaults to :class:`~loom.etl.executor.NoopRunObserver`.
        dispatcher: Parallel task dispatcher.  Defaults to
                    :class:`~loom.etl.executor.ThreadDispatcher`.

    Example::

        executor = ETLExecutor(
            reader=PolarsDeltaReader(catalog_root),
            writer=PolarsDeltaWriter(catalog_root),
            observer=CompositeRunObserver(LoggingRunObserver(), DeltaRunObserver()),
        )
        executor.run_pipeline(plan, params)
    """

    def __init__(
        self,
        reader: SourceReader,
        writer: TargetWriter,
        observer: ETLRunObserver | None = None,
        dispatcher: ParallelDispatcher | None = None,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._observer: ETLRunObserver = observer or NoopRunObserver()
        self._dispatcher: ParallelDispatcher = dispatcher or ThreadDispatcher()

    # ------------------------------------------------------------------
    # Public run methods
    # ------------------------------------------------------------------

    def run_pipeline(
        self,
        plan: PipelinePlan,
        params: Any,
        run_id: str | None = None,
    ) -> None:
        """Execute a full pipeline plan.

        Processes run sequentially unless wrapped in a
        :class:`~loom.etl.compiler.ParallelProcessGroup`.

        Args:
            plan:   Compiled :class:`~loom.etl.compiler.PipelinePlan`.
            params: Concrete params instance for this run.
            run_id: Optional trace ID.  Generated as UUID4 when omitted.

        Raises:
            Exception: First unhandled exception from any step, after the
                       observer has received the ``pipeline_end(FAILED)`` event.
        """
        run_id = run_id or str(uuid.uuid4())
        start = time.monotonic()
        self._observer.on_pipeline_start(plan, params, run_id)
        try:
            for node in plan.nodes:
                self._run_pipeline_node(node, params, run_id)
            self._observer.on_pipeline_end(run_id, RunStatus.SUCCESS, _ms(start))
        except Exception:
            self._observer.on_pipeline_end(run_id, RunStatus.FAILED, _ms(start))
            raise

    def run_process(
        self,
        plan: ProcessPlan,
        params: Any,
        run_id: str | None = None,
    ) -> None:
        """Execute a single process plan.

        Steps run sequentially unless wrapped in a
        :class:`~loom.etl.compiler.ParallelStepGroup`.

        Args:
            plan:   Compiled :class:`~loom.etl.compiler.ProcessPlan`.
            params: Concrete params instance for this run.
            run_id: Parent pipeline run ID, or a new UUID4 when called standalone.

        Raises:
            Exception: First unhandled exception from any step.
        """
        run_id = run_id or str(uuid.uuid4())
        process_run_id = str(uuid.uuid4())
        start = time.monotonic()
        self._observer.on_process_start(plan, run_id, process_run_id)
        try:
            for node in plan.nodes:
                self._run_process_node(node, params, run_id)
            self._observer.on_process_end(process_run_id, RunStatus.SUCCESS, _ms(start))
        except Exception:
            self._observer.on_process_end(process_run_id, RunStatus.FAILED, _ms(start))
            raise

    def run_step(
        self,
        plan: StepPlan,
        params: Any,
        run_id: str | None = None,
    ) -> None:
        """Execute a single step plan.

        Reads all sources, calls ``execute()``, writes the target.

        Args:
            plan:   Compiled :class:`~loom.etl.compiler.StepPlan`.
            params: Concrete params instance for this run.
            run_id: Parent run ID, or a new UUID4 when called standalone.

        Raises:
            Exception: Any unhandled exception from read, execute, or write —
                       after the observer has received ``on_step_error`` and
                       ``on_step_end(FAILED)``.
        """
        run_id = run_id or str(uuid.uuid4())
        step_run_id = str(uuid.uuid4())
        start = time.monotonic()
        self._observer.on_step_start(plan, run_id, step_run_id)
        try:
            frames = {b.alias: self._reader.read(b.spec, params) for b in plan.source_bindings}
            result = plan.step_type().execute(params, **frames)
            self._writer.write(result, plan.target_binding.spec, params)
            self._observer.on_step_end(step_run_id, RunStatus.SUCCESS, 0, 0, _ms(start))
        except Exception as exc:
            self._observer.on_step_error(step_run_id, exc)
            self._observer.on_step_end(step_run_id, RunStatus.FAILED, 0, 0, _ms(start))
            raise

    # ------------------------------------------------------------------
    # Internal node dispatch
    # ------------------------------------------------------------------

    def _run_pipeline_node(self, node: PipelineProcessNode, params: Any, run_id: str) -> None:
        match node:
            case ParallelProcessGroup(plans=plans):

                def _run_proc(p: ProcessPlan) -> None:
                    self.run_process(p, params, run_id)

                self._dispatcher.run_all([lambda p=p: _run_proc(p) for p in plans])  # type: ignore[misc]
            case ProcessPlan():
                self.run_process(node, params, run_id)

    def _run_process_node(self, node: ProcessStepNode, params: Any, run_id: str) -> None:
        match node:
            case ParallelStepGroup(plans=plans):

                def _run_step(p: StepPlan) -> None:
                    self.run_step(p, params, run_id)

                self._dispatcher.run_all([lambda p=p: _run_step(p) for p in plans])  # type: ignore[misc]
            case StepPlan():
                self.run_step(node, params, run_id)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _ms(start: float) -> int:
    """Elapsed milliseconds since ``start`` (from ``time.monotonic()``)."""
    return int((time.monotonic() - start) * 1000)
