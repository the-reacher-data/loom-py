"""Build Prefect flows from ETLPipeline definitions."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import prefect
import prefect.runtime

from loom.etl.compiler import ETLCompiler
from loom.etl.compiler._plan import (
    PipelinePlan,
    ProcessPlan,
    StepPlan,
    visit_pipeline_nodes,
    visit_process_nodes,
)
from loom.etl.lineage._records import RunStatus
from loom.etl.pipeline._pipeline import ETLPipeline
from loom.etl.runner import ETLRunner
from loom.prefect._config import FlowConfig, _load_flow_config
from loom.prefect._ctx import FlowCtx
from loom.prefect._manifest import (
    ManifestStore,
    RunManifest,
    completed_steps,
    mark_step,
)

_log = logging.getLogger(__name__)


def _get_run_count() -> int:
    """Return the current Prefect flow run attempt count.

    Returns 0 when called outside a Prefect flow context (tests / local CLI).
    Callers must treat 0 as "outside Prefect — this is a single last attempt."
    """
    try:
        flow_run = prefect.runtime.flow_run  # pyright: ignore[reportAttributeAccessIssue]
        return int(getattr(flow_run, "run_count", 0))
    except Exception:
        return 0


def _is_last_attempt(run_count: int, flow_retries: int) -> bool:
    """Return True when this is the final allowed attempt.

    ``run_count == 0`` signals that we are outside a Prefect context (tests /
    local CLI) — treated as a single last attempt so manifests are cleaned up.
    """
    return run_count == 0 or run_count > flow_retries


def build_etl_flow(
    pipeline_cls: type[ETLPipeline[Any]],
    *,
    runner_factory: Callable[[], ETLRunner],
    manifest_store: ManifestStore,
    config_path: str,
    name: str | None = None,
) -> Any:
    """Build a Prefect ``@flow`` from an :class:`~loom.etl.ETLPipeline`.

    The pipeline plan is compiled once at build time. Configuration is loaded
    at build time with a fallback to :class:`~loom.prefect._config.FlowConfig`
    defaults so that the factory can be called in environments without a real
    config file (e.g. unit tests). Each ``ETLStep`` becomes an observable
    Prefect ``@task``. Parallel steps in the compiled plan are submitted via
    ``run_step.submit()`` so Prefect tracks them concurrently.

    Args:
        pipeline_cls: ``ETLPipeline`` class to build the flow from.
        runner_factory: Callable returning a configured ``ETLRunner``. Called
            once per flow run (per Fargate container) inside the ``@flow`` body.
        manifest_store: Backend for the ephemeral retry manifest.
        config_path: Path to the YAML file containing flow retry configuration.
        name: Override for the Prefect flow name. Defaults to
            ``pipeline_cls.__name__``.

    Returns:
        A Prefect ``@flow``-decorated callable that accepts ``(ctx, params)``.

    Example::

        daily_orders_flow = build_etl_flow(
            DailyOrdersETL,
            runner_factory=lambda: ETLRunner.from_yaml("config/storage.yaml"),
            manifest_store=S3JsonManifestStore(bucket="my-data-lake"),
            config_path="config/etl_flows.yaml",
        )
        daily_orders_flow(
            ctx=FlowCtx(correlation_id="orders-2026-06-02", run_id="run-1"),
            params=DailyOrdersParams(run_date=date(2026, 6, 2)),
        )
    """
    plan = ETLCompiler().compile(pipeline_cls)

    try:
        cfg = _load_flow_config(config_path, pipeline_cls.__name__)
    except Exception:
        cfg = FlowConfig()

    @prefect.task(name="etl-step", retries=cfg.task_retries)
    def run_step(
        step_name: str,
        ctx: FlowCtx,
        params: Any,
        runner: ETLRunner,
    ) -> None:
        run_count = _get_run_count()
        runner.run(
            pipeline_cls,
            params,
            include=[step_name],
            run_id=ctx.run_id,
            correlation_id=ctx.correlation_id,
            attempt=max(run_count, 1),
            last_attempt=_is_last_attempt(run_count, cfg.flow_retries),
        )

    @prefect.flow(
        name=name or pipeline_cls.__name__,
        retries=cfg.flow_retries,
        retry_delay_seconds=cfg.flow_retry_delay_seconds,
    )
    def etl_flow(ctx: FlowCtx, params: Any) -> None:
        # Load manifest first — before anything that can fail — so retry state
        # is always preserved even if a later operation raises.
        manifest = (None if ctx.force else manifest_store.load(ctx.correlation_id)) or RunManifest(
            correlation_id=ctx.correlation_id,
            steps=(),
            updated_at=datetime.now(tz=UTC),
        )

        runner = runner_factory()
        done = completed_steps(manifest)
        execution_nodes = _build_execution_nodes(plan, ctx.processes)

        run_count = _get_run_count()
        is_last = _is_last_attempt(run_count, cfg.flow_retries)

        try:
            _execute_nodes(
                execution_nodes,
                done,
                ctx,
                params,
                manifest,
                manifest_store,
                run_step,
                runner,
            )
        finally:
            if is_last:
                manifest_store.delete(ctx.correlation_id)

    return etl_flow


def _build_execution_nodes(
    plan: PipelinePlan,
    processes: tuple[str, ...] | None,
) -> list[StepPlan | tuple[StepPlan, ...]]:
    """Return a flat execution list from the plan, optionally filtered by process.

    Uses the existing ``visit_pipeline_nodes`` / ``visit_process_nodes`` helpers
    from ``loom.etl.compiler._plan`` — no new traversal logic.

    Args:
        plan: Compiled pipeline plan.
        processes: Process class names to include. ``None`` includes all.

    Returns:
        List where each element is either a single ``StepPlan`` (sequential)
        or a tuple of ``StepPlan`` objects (parallel group).
    """
    result: list[StepPlan | tuple[StepPlan, ...]] = []

    def _add_process(proc: ProcessPlan) -> None:
        if processes is not None and proc.process_type.__name__ not in processes:
            return

        def _add_step(step: StepPlan) -> None:
            result.append(step)

        def _add_parallel(steps: tuple[StepPlan, ...]) -> None:
            result.append(steps)

        visit_process_nodes(proc.nodes, _add_step, on_parallel_group=_add_parallel)

    def _add_parallel_procs(procs: tuple[ProcessPlan, ...]) -> None:
        for proc in procs:
            _add_process(proc)

    visit_pipeline_nodes(plan.nodes, _add_process, on_parallel_group=_add_parallel_procs)
    return result


def _execute_nodes(
    nodes: list[StepPlan | tuple[StepPlan, ...]],
    done: frozenset[str],
    ctx: FlowCtx,
    params: Any,
    manifest: RunManifest,
    manifest_store: ManifestStore,
    run_step: Any,
    runner: ETLRunner,
) -> None:
    """Execute sequential steps and parallel groups respecting the manifest.

    Args:
        nodes: Flat execution list from :func:`_build_execution_nodes`.
        done: Step names already completed (will be skipped).
        ctx: Flow operational context.
        params: Pipeline-specific params instance.
        manifest: Current in-memory manifest state.
        manifest_store: Persistence backend (written only on failure).
        run_step: Prefect ``@task`` function (or plain callable in tests).
        runner: Configured ``ETLRunner`` for this flow run.
    """
    for node in nodes:
        if isinstance(node, tuple):
            manifest = _run_parallel_group(
                node,
                done,
                ctx,
                params,
                manifest,
                manifest_store,
                run_step,
                runner,
            )
        else:
            manifest = _run_sequential_step(
                node,
                done,
                ctx,
                params,
                manifest,
                manifest_store,
                run_step,
                runner,
            )


def _run_sequential_step(
    step: StepPlan,
    done: frozenset[str],
    ctx: FlowCtx,
    params: Any,
    manifest: RunManifest,
    manifest_store: ManifestStore,
    run_step: Any,
    runner: ETLRunner,
) -> RunManifest:
    step_name = step.step_type.__name__
    if step_name in done:
        _log.debug("skip %s (already SUCCESS in manifest)", step_name)
        return manifest
    try:
        run_step(step_name, ctx, params, runner)
        return mark_step(manifest, step_name, RunStatus.SUCCESS)
    except Exception:
        manifest = mark_step(manifest, step_name, RunStatus.FAILED)
        try:
            manifest_store.save(manifest)
        except Exception:
            _log.warning("manifest save failed for %s", step_name, exc_info=True)
        raise


def _run_parallel_group(
    steps: tuple[StepPlan, ...],
    done: frozenset[str],
    ctx: FlowCtx,
    params: Any,
    manifest: RunManifest,
    manifest_store: ManifestStore,
    run_step: Any,
    runner: ETLRunner,
) -> RunManifest:
    futures: list[tuple[str, Any]] = []

    for step in steps:
        step_name = step.step_type.__name__
        if step_name in done:
            _log.debug("skip %s (already SUCCESS in manifest)", step_name)
            continue
        try:
            future = run_step.submit(step_name, ctx, params, runner)
            futures.append((step_name, future))
        except AttributeError:
            # @task is a no-op in test mode — .submit() does not exist
            run_step(step_name, ctx, params, runner)
            manifest = mark_step(manifest, step_name, RunStatus.SUCCESS)

    for step_name, future in futures:
        try:
            future.result()
            manifest = mark_step(manifest, step_name, RunStatus.SUCCESS)
        except Exception:
            manifest = mark_step(manifest, step_name, RunStatus.FAILED)
            try:
                manifest_store.save(manifest)
            except Exception:
                _log.warning("manifest save failed for %s", step_name, exc_info=True)
            raise

    return manifest


__all__ = ["build_etl_flow"]
