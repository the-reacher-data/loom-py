"""ETL executor — internal engine; use :class:`~loom.etl.ETLRunner` instead.

Walks a compiled :class:`~loom.etl.compiler._plan.PipelinePlan` and drives
read → execute → write for each step, emitting lifecycle events to the
injected observers.

This class is **not** part of the public API.  It is instantiated exclusively
by :class:`~loom.etl.ETLRunner`, which wires reader, writer, catalog, and
observers from a :data:`~loom.etl.StorageConfig`.

For normal usage::

    from loom.etl import ETLRunner
    from loom.etl.executor import StructlogRunObserver

    runner = ETLRunner.from_yaml(
        "loom.yaml",
        observers=[StructlogRunObserver()],
    )
    runner.run(DailyOrdersPipeline, DailyOrdersParams(run_date=date.today()))

Parallelism
-----------
Sequential nodes run in order.  Nested lists (``ParallelStepGroup``,
``ParallelProcessGroup``) are dispatched through the injected
:class:`~loom.etl.executor.ParallelDispatcher`, defaulting to
:class:`~loom.etl.executor.ThreadDispatcher`.
"""

from __future__ import annotations

import functools
import logging
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import replace
from typing import Any

from loom.etl.checkpoint import CheckpointStore
from loom.etl.compiler._plan import (
    ParallelProcessGroup,
    ParallelStepGroup,
    PipelinePlan,
    PipelineProcessNode,
    ProcessPlan,
    StepPlan,
)
from loom.etl.declarative.source import TempSourceSpec
from loom.etl.declarative.target._temp import TempFanInSpec, TempSpec
from loom.etl.executor._dispatcher import ParallelDispatcher, ThreadDispatcher
from loom.etl.observability.observers.protocol import ETLRunObserver
from loom.etl.observability.records import RunContext, RunStatus
from loom.etl.runtime.contracts import SourceReader, TargetWriter

_log = logging.getLogger(__name__)


class ETLExecutor:
    """Drives execution of compiled ETL plans.

    Responsibilities:

    * Read each source via the injected :class:`~loom.etl._io.SourceReader`.
    * Invoke the step's ``execute()`` with the resulting frames.
    * Write the result via the injected :class:`~loom.etl._io.TargetWriter`.
    * Emit lifecycle events to each :class:`~loom.etl.executor.ETLRunObserver`.
    * Dispatch parallel groups through the :class:`~loom.etl.executor.ParallelDispatcher`.

    All collaborators are injected — the executor has no dependency on any
    specific backend, broker, or observability system.

    Internal class — use :class:`~loom.etl.ETLRunner` for production code.
    This class is useful for unit tests with stub readers/writers and for
    custom orchestration adapters (Celery, Prefect, Airflow) that need to
    run individual steps outside a full pipeline.

    Args:
        reader:     Source reader implementation.
        writer:     Target writer implementation.
        observers:  Sequence of lifecycle observers.  Defaults to empty.
        dispatcher: Parallel task dispatcher.  Defaults to
                    :class:`~loom.etl.executor.ThreadDispatcher`.
    """

    def __init__(
        self,
        reader: SourceReader,
        writer: TargetWriter,
        observers: Sequence[ETLRunObserver] = (),
        dispatcher: ParallelDispatcher | None = None,
        checkpoint_store: CheckpointStore | None = None,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._observers: Sequence[ETLRunObserver] = observers
        self._dispatcher: ParallelDispatcher = dispatcher or ThreadDispatcher()
        self._checkpoint_store: CheckpointStore | None = checkpoint_store

    def run_pipeline(
        self,
        plan: PipelinePlan,
        params: Any,
        ctx: RunContext | None = None,
    ) -> None:
        """Execute a full pipeline plan.

        Processes run sequentially unless wrapped in a
        :class:`~loom.etl.compiler.ParallelProcessGroup`.

        Args:
            plan:   Compiled :class:`~loom.etl.compiler.PipelinePlan`.
            params: Concrete params instance for this run.
            ctx:    Run context carrying ``run_id``, ``correlation_id``, and
                    ``attempt``.  Generated with a fresh UUID4 when omitted.

        Raises:
            Exception: First unhandled exception from any step, after all
                       observers have received the ``pipeline_end(FAILED)`` event.
        """
        ctx = ctx or RunContext(run_id=_new_run_id())
        _log.info(
            "pipeline start pipeline=%s run_id=%s nodes=%d attempt=%d",
            plan.pipeline_type.__name__,
            ctx.run_id,
            len(plan.nodes),
            ctx.attempt,
        )
        start = time.monotonic()
        for obs in self._observers:
            obs.on_pipeline_start(plan, params, ctx)
        status = RunStatus.SUCCESS
        try:
            for node in plan.nodes:
                self._run_pipeline_node(node, params, ctx)
        except Exception:
            status = RunStatus.FAILED
            raise
        finally:
            for obs in self._observers:
                obs.on_pipeline_end(ctx, status, _ms(start))
            self._cleanup_temps(ctx, status)

    def run_process(
        self,
        plan: ProcessPlan,
        params: Any,
        ctx: RunContext | None = None,
    ) -> None:
        """Execute a single process plan.

        Steps run sequentially unless wrapped in a
        :class:`~loom.etl.compiler.ParallelStepGroup`.

        Args:
            plan:   Compiled :class:`~loom.etl.compiler.ProcessPlan`.
            params: Concrete params instance for this run.
            ctx:    Run context from the parent pipeline, or a fresh one.

        Raises:
            Exception: First unhandled exception from any step.
        """
        ctx = ctx or RunContext(run_id=_new_run_id())
        process_run_id = _new_run_id()
        _log.debug(
            "process start process=%s process_run_id=%s nodes=%d",
            plan.process_type.__name__,
            process_run_id,
            len(plan.nodes),
        )
        start = time.monotonic()
        for obs in self._observers:
            obs.on_process_start(plan, ctx, process_run_id)
        process_ctx = replace(ctx, process_run_id=process_run_id)
        status = RunStatus.SUCCESS
        try:
            for node in plan.nodes:
                process_node: StepPlan | ParallelStepGroup = node
                self._run_process_node(process_node, params, process_ctx)
        except Exception:
            status = RunStatus.FAILED
            raise
        finally:
            for obs in self._observers:
                obs.on_process_end(process_run_id, status, _ms(start))

    def run_step(
        self,
        plan: StepPlan,
        params: Any,
        ctx: RunContext | None = None,
    ) -> None:
        """Execute a single step plan.

        Reads all sources, calls ``execute()``, writes the target.

        Args:
            plan:   Compiled :class:`~loom.etl.compiler.StepPlan`.
            params: Concrete params instance for this run.
            ctx:    Run context from the parent pipeline, or a fresh one.

        Raises:
            Exception: Any unhandled exception from read, execute, or write —
                       after all observers have received ``on_step_error`` and
                       ``on_step_end(FAILED)``.
        """
        ctx = ctx or RunContext(run_id=_new_run_id())
        step_run_id = _new_run_id()
        _log.debug(
            "step start step=%s step_run_id=%s sources=%s",
            plan.step_type.__name__,
            step_run_id,
            [b.alias for b in plan.source_bindings],
        )
        start = time.monotonic()
        for obs in self._observers:
            obs.on_step_start(plan, ctx, step_run_id)
        status = RunStatus.SUCCESS
        try:
            frames = {b.alias: self._read_source(b.spec, params, ctx) for b in plan.source_bindings}
            step = plan.step_type()
            if _is_sql_step(step):
                query = _render_sql_query(step, params)
                result = self._reader.execute_sql(frames, query)
            else:
                result = step.execute(params, **frames)
            self._write_target(
                result, plan.target_binding.spec, params, ctx, streaming=plan.streaming
            )
        except Exception as exc:
            status = RunStatus.FAILED
            for obs in self._observers:
                obs.on_step_error(step_run_id, exc)
            raise
        finally:
            for obs in self._observers:
                obs.on_step_end(step_run_id, status, _ms(start))

    def _require_checkpoint_store(self, name: str) -> CheckpointStore:
        """Return the checkpoint store, or raise if unconfigured."""
        if self._checkpoint_store is None:
            raise RuntimeError(
                f"Step uses checkpoint intermediate {name!r} but no checkpoint store is "
                "configured. Set 'checkpoint:' in your storage config."
            )
        return self._checkpoint_store

    def _read_source(self, spec: Any, params: Any, ctx: RunContext) -> Any:
        if isinstance(spec, TempSourceSpec):
            _log.debug("read source kind=TEMP name=%s", spec.temp_name)
            return self._require_checkpoint_store(spec.temp_name).get(
                spec.temp_name,
                run_id=ctx.run_id,
                correlation_id=ctx.correlation_id,
            )
        _log.debug(
            "read source kind=%s ref=%s",
            spec.kind,
            getattr(spec, "table_ref", None) or getattr(spec, "path", None),
        )
        return self._reader.read(spec, params)

    def _write_target(
        self, result: Any, spec: Any, params: Any, ctx: RunContext, *, streaming: bool = False
    ) -> None:
        if isinstance(spec, (TempSpec, TempFanInSpec)):
            _log.debug("write target kind=TEMP name=%s scope=%s", spec.temp_name, spec.temp_scope)
            self._require_checkpoint_store(spec.temp_name).put(
                spec.temp_name,
                run_id=ctx.run_id,
                correlation_id=ctx.correlation_id,
                scope=spec.temp_scope,
                data=result,
                append=isinstance(spec, TempFanInSpec),
            )
        else:
            _log.debug(
                "write target ref=%s",
                getattr(spec, "table_ref", None) or getattr(spec, "path", None),
            )
            self._writer.write(result, spec, params, streaming=streaming)

    def _cleanup_temps(self, ctx: RunContext, status: RunStatus) -> None:
        if self._checkpoint_store is None:
            return
        self._checkpoint_store.cleanup_run(ctx.run_id)
        if ctx.correlation_id is None:
            return
        if status is RunStatus.SUCCESS and ctx.last_attempt:
            self._checkpoint_store.cleanup_correlation(ctx.correlation_id)
        elif status is RunStatus.FAILED and ctx.last_attempt:
            _log.warning(
                "CORRELATION intermediates were NOT cleaned — pipeline failed on last attempt. "
                "Call runner.cleanup_correlation(%r) to reclaim storage.",
                ctx.correlation_id,
            )

    def _dispatch_parallel(
        self,
        run_fn: Callable[..., None],
        plans: tuple[Any, ...],
        params: Any,
        ctx: RunContext,
    ) -> None:
        """Dispatch *plans* concurrently via the configured dispatcher."""
        tasks = [functools.partial(run_fn, p, params, ctx) for p in plans]
        self._dispatcher.run_all(tasks)

    def _run_pipeline_node(self, node: PipelineProcessNode, params: Any, ctx: RunContext) -> None:
        match node:
            case ParallelProcessGroup(plans=plans):
                self._dispatch_parallel(self.run_process, plans, params, ctx)
            case ProcessPlan():
                self.run_process(node, params, ctx)

    def _run_process_node(
        self, node: StepPlan | ParallelStepGroup, params: Any, ctx: RunContext
    ) -> None:
        match node:
            case ParallelStepGroup(plans=plans):
                self._dispatch_parallel(self.run_step, plans, params, ctx)
            case StepPlan():
                self.run_step(node, params, ctx)


def _new_run_id() -> str:
    """Generate a fresh UUID4 run identifier."""
    return str(uuid.uuid4())


def _ms(start: float) -> int:
    """Elapsed milliseconds since ``start`` (from ``time.monotonic()``)."""
    return int((time.monotonic() - start) * 1000)


def _is_sql_step(step: Any) -> bool:
    """Return ``True`` when *step* follows StepSQL marker contract."""
    return bool(getattr(step, "_loom_sql_step", False))


def _render_sql_query(step: Any, params: Any) -> str:
    """Render SQL query from a StepSQL-like step instance."""
    render = getattr(step, "render_sql", None)
    if not callable(render):
        raise TypeError(
            f"{type(step).__qualname__} is marked as SQL step but has no callable render_sql()."
        )
    query = render(params)
    if not isinstance(query, str):
        raise TypeError(
            f"{type(step).__qualname__}.render_sql() must return str, got {type(query)!r}."
        )
    return query
