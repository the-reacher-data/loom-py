"""Per-ETL Prefect flow factory + deployment discovery.

The user declares one ETL like this::

    # pipelines/flows/motos_snapshot.py
    from loom.prefect import etl_flow
    from pipelines.entrypoint import MotosSnapshotPipeline
    from pipelines.common.window import WindowParams

    motos_snapshot = etl_flow(
        name="motos-snapshot",
        pipeline=MotosSnapshotPipeline,
        params_type=WindowParams,
        config_path="config/etls/motos_snapshot.yaml",
        source_file=__file__,
    )

CI/CD then registers every per-ETL flow against a Prefect work pool::

    from loom.prefect import discover_and_deploy_etls

    discover_and_deploy_etls(
        flows_package="pipelines.flows",
        work_pool="loom-fargate",     # ECS-typed pool in prod
        # work_pool="loom-docker",   # docker-typed pool in local dev
    )

The flow's signature is **synthesized from the params type** so the
Prefect UI shows a typed parameter form (``updated_at_from: datetime``,
``updated_at_to: datetime``, …) and ``-p key=value`` CLI overrides work.

Architecture: this flow runs inside whatever container the Prefect worker
provisions. The worker (``prefect-aws.ECSWorker`` in prod,
``prefect-docker.DockerWorker`` locally) is responsible for spawning the
container; the flow body itself executes the ETL steps. No custom
launcher is needed; per-step ``@prefect.task`` runs appear in the UI
under this single flow run.
"""

from __future__ import annotations

import importlib
import inspect
import logging
import pkgutil
import re
from datetime import UTC, date, datetime
from pathlib import Path
from types import UnionType
from typing import Any, Union, get_args, get_origin, get_type_hints
from uuid import uuid4

import msgspec
import prefect
import prefect.runtime

from loom.core.model import LoomFrozenStruct
from loom.etl.compiler import ETLCompiler
from loom.etl.compiler._plan import (
    PipelinePlan,
    ProcessPlan,
    StepPlan,
    visit_pipeline_nodes,
    visit_process_nodes,
)
from loom.etl.lineage._records import RunStatus
from loom.etl.pipeline import ETLPipeline
from loom.etl.runner import ETLRunner
from loom.prefect._config import FlowConfig, _load_flow_config
from loom.prefect._ctx import FlowCtx
from loom.prefect._manifest import (
    ManifestStore,
    RunManifest,
    completed_steps,
    mark_step,
)
from loom.prefect._placeholders import resolve_placeholder
from loom.prefect._task_run_observer import PrefectTaskRunObserver

_log = logging.getLogger(__name__)

# Attribute name used to attach discovery metadata to each flow.
_LOOM_ETL_META_ATTR = "__loom_etl_meta__"


# ---------------------------------------------------------------------------
# Discovery metadata
# ---------------------------------------------------------------------------


class _ETLFlowMeta(LoomFrozenStruct, frozen=True, kw_only=True):
    """Per-flow metadata consumed by :func:`discover_and_deploy_etls`."""

    name: str
    config_path: str
    source_file: str
    correlation_field: str | None
    schedule: dict[str, Any] | None
    raw_params: dict[str, Any]
    # Map ``environment`` → ``{"work_pool": str | None, "job_variables": dict}``
    pool_config: dict[str, dict[str, Any]]


def _pause_schedule_on_failure(flow: Any, flow_run: Any, state: Any) -> None:
    """Prefect ``on_failure`` hook: deactivate the deployment's schedules.

    Fires only when the flow finally enters Failed state (after
    ``flow_retries`` have been exhausted). Stops the cron from firing
    further until an operator investigates and re-enables the schedule.
    Best-effort: any error here is logged and swallowed — pausing the
    schedule is a safety net, not part of the critical path.
    """
    deployment_id = getattr(flow_run, "deployment_id", None)
    if not deployment_id:
        return  # ad-hoc run, no schedule to pause

    try:
        from prefect.client.orchestration import get_client  # noqa: PLC0415

        from loom.prefect._async import run_sync  # noqa: PLC0415

        async def _pause() -> None:
            async with get_client() as client:
                deployment = await client.read_deployment(deployment_id)
                for sched in deployment.schedules or []:
                    await client.update_deployment_schedule(deployment_id, sched.id, active=False)

        run_sync(_pause())
        _log.warning(
            "pause-on-failure: deactivated schedules for deployment %s",
            deployment_id,
        )
    except Exception:  # noqa: BLE001
        _log.warning("pause-on-failure hook failed", exc_info=True)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def etl_flow(
    *,
    name: str,
    pipeline: type[ETLPipeline[Any]],
    params_type: type[msgspec.Struct],
    config_path: str,
    source_file: str,
    storage_config_path: str = "/app/config.yaml",
    flow_config_path: str | None = None,
    manifest_store: ManifestStore | None = None,
) -> Any:
    """Build a per-ETL Prefect flow with a typed, synthesized signature.

    Args:
        name: Logical ETL name. Used as the Prefect flow name AND the
            deployment name; operators see this verbatim in Prefect UI.
        pipeline: ``ETLPipeline`` subclass to execute.
        params_type: ``msgspec.Struct`` (typically an ``ETLParams``
            subclass) whose fields become typed kwargs of the flow.
        config_path: Path to the per-ETL YAML (used at deploy-time for
            schedule + parameter defaults + work-pool job_variables).
        source_file: ``__file__`` of the user's module where this call
            occurs. Required so Prefect 3 ``from_source`` can find the
            flow on disk.
        storage_config_path: Path to the loom storage YAML the runner
            reads at flow-run time inside the container. Defaults to
            ``/app/config.yaml`` (the path baked into the image).
        flow_config_path: Optional path to a YAML with
            :class:`~loom.prefect.FlowConfig` retry settings. Falls back
            to :class:`FlowConfig` defaults when omitted.
        manifest_store: Optional :class:`~loom.prefect.ManifestStore` for
            cross-attempt resume. ``None`` disables manifest persistence.

    Returns:
        A ``@prefect.flow``-decorated callable. Discovery metadata is
        attached at ``__loom_etl_meta__``.
    """
    raw_cfg = _read_yaml(config_path)
    correlation_field = raw_cfg.get("correlation_field")
    schedule = raw_cfg.get("schedule")
    raw_params = dict(raw_cfg.get("params") or {})
    pool_config = _extract_pool_config(raw_cfg)

    plan = ETLCompiler().compile(pipeline)

    if flow_config_path:
        try:
            flow_cfg = _load_flow_config(flow_config_path, pipeline.__name__)
        except KeyError:
            # Pipeline not listed under ``flows:`` in the YAML — fall back to
            # defaults. Any other error (missing file, bad YAML) propagates.
            flow_cfg = FlowConfig()
    else:
        flow_cfg = FlowConfig()

    resolved_config_path = str(Path(config_path).resolve())
    resolved_source_file = str(Path(source_file).resolve())

    user_parameters = _signature_from_params_type(params_type)
    universal_parameters = [
        inspect.Parameter("env", inspect.Parameter.KEYWORD_ONLY, default="prod", annotation=str),
        inspect.Parameter(
            "correlation_id",
            inspect.Parameter.KEYWORD_ONLY,
            default=None,
            annotation=str | None,
        ),
    ]
    new_signature = inspect.Signature(
        parameters=user_parameters + universal_parameters,
        return_annotation=int,
    )

    def _flow_body(**kwargs: Any) -> int:
        env = kwargs.pop("env", "prod")
        explicit_correlation = kwargs.pop("correlation_id", None)
        resolved = {key: resolve_placeholder(value) for key, value in kwargs.items()}
        resolved = _normalize_datetime_fields(resolved, params_type)
        params_obj = msgspec.convert(resolved, type=params_type)
        ctx = FlowCtx(
            correlation_id=(
                explicit_correlation or _compute_correlation_id(name, correlation_field, resolved)
            ),
            run_id=f"{name}-{uuid4().hex[:8]}",
            environment=env,
        )

        import os as _os  # noqa: PLC0415

        actual_config_path = _os.environ.get("LOOM_STORAGE_CONFIG_PATH") or storage_config_path

        manifest = (
            manifest_store.load(ctx.correlation_id) if manifest_store is not None else None
        ) or RunManifest(
            correlation_id=ctx.correlation_id,
            steps=(),
            updated_at=datetime.now(tz=UTC),
        )
        done = completed_steps(manifest)
        all_step_names = _collect_step_names(plan, ctx.processes)
        pending = [s for s in all_step_names if s not in done]

        if not pending:
            if manifest_store is not None:
                manifest_store.delete(ctx.correlation_id)
            return 0

        flow_run_id: Any = None
        try:
            from prefect.runtime import flow_run as _fr  # noqa: PLC0415

            flow_run_id = _fr.id
        except Exception:  # noqa: BLE001
            flow_run_id = None

        extra_observers: list[Any] = []
        if flow_run_id is not None:
            extra_observers.append(PrefectTaskRunObserver(flow_run_id=flow_run_id))
        if manifest_store is not None:
            extra_observers.append(_ManifestObserver(manifest_store, manifest))

        runner = ETLRunner.from_yaml(actual_config_path, extra_observers=extra_observers)
        runner.run(
            pipeline,
            params_obj,
            include=pending,
            run_id=ctx.run_id,
            correlation_id=ctx.correlation_id,
        )
        # Cleared on full success; on failure the manifest stays so the next
        # attempt can skip SUCCESS steps via ``include=pending``.
        if manifest_store is not None:
            manifest_store.delete(ctx.correlation_id)
        return 0

    safe_name = name.replace("-", "_")
    _flow_body.__signature__ = new_signature  # type: ignore[attr-defined]
    _flow_body.__name__ = safe_name
    _flow_body.__qualname__ = safe_name

    def _run_name(**params: Any) -> str:
        """Compute a meaningful run name from the bound flow parameters.

        Resolution order:
          1. Schedule-triggered runs (no explicit correlation override and a
             ``scheduled_start_time`` set by Prefect) → ``<slot UTC ISO minute>``.
          2. explicit ``correlation_id`` kwarg.
          3. ``<value of correlation_field>-<HHMMSS>``.
          4. ``<UTC timestamp>`` as last resort.
        """
        # Fallback to ``prefect.runtime`` if Prefect calls us without kwargs
        # (some code paths skip the injection).
        scheduled_start = None
        if not params:
            try:
                from prefect.runtime import flow_run as _fr  # noqa: PLC0415

                params = dict(_fr.parameters or {})
                scheduled_start = _fr.scheduled_start_time
            except Exception:  # noqa: BLE001
                params = {}
        else:
            try:
                from prefect.runtime import flow_run as _fr  # noqa: PLC0415

                scheduled_start = _fr.scheduled_start_time
            except Exception:  # noqa: BLE001
                scheduled_start = None

        # Schedule-triggered: no explicit correlation override was passed and
        # Prefect injected a ``scheduled_start_time``. Use the scheduled slot
        # so cron runs read as ``2026-06-05T08:00`` in the UI.
        has_explicit_correlation = bool(
            params.get("correlation_id")
            or (correlation_field and params.get(correlation_field) is not None)
        )
        if scheduled_start is not None and not has_explicit_correlation:
            return str(scheduled_start.strftime("%Y-%m-%dT%H:%M"))

        # The run already lives under the flow named ``<name>`` in the UI,
        # so we don't repeat it in the run name. Format: the correlation
        # value followed by a short HHMMSS tiebreaker so multiple triggers
        # with the same correlation_id (resume / manual reruns) appear
        # as distinguishable rows in the UI.
        ts = datetime.now(tz=UTC).strftime("%H%M%S")

        cid = params.get("correlation_id")
        if cid:
            return f"{cid}-{ts}"
        if correlation_field and params.get(correlation_field) is not None:
            value = params[correlation_field]
            # The callable runs BEFORE the flow body that resolves
            # ``${now}`` etc. — resolve here too so the displayed name
            # reflects the real bound value.
            resolved_value = resolve_placeholder(value)
            if isinstance(resolved_value, datetime | date):
                rendered = resolved_value.strftime("%Y%m%dT%H%M%S")
            else:
                rendered = str(resolved_value)
            safe = _UNSAFE_CORR_CHARS.sub("_", rendered)
            return f"{safe}-{ts}"
        return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%S")

    decorated = prefect.flow(
        name=name,
        flow_run_name=_run_name,
        retries=flow_cfg.flow_retries,
        retry_delay_seconds=flow_cfg.flow_retry_delay_seconds,
        validate_parameters=False,
        on_failure=[_pause_schedule_on_failure],
    )(_flow_body)
    setattr(
        decorated,
        _LOOM_ETL_META_ATTR,
        _ETLFlowMeta(
            name=name,
            config_path=resolved_config_path,
            source_file=resolved_source_file,
            correlation_field=correlation_field,
            schedule=schedule,
            raw_params=raw_params,
            pool_config=pool_config,
        ),
    )
    return decorated


# ---------------------------------------------------------------------------
# Discovery + deployment
# ---------------------------------------------------------------------------


def discover_and_deploy_etls(
    *,
    flows_package: str,
    work_pool: str = "loom-fargate",
    env: str = "prod",
) -> list[str]:
    """Walk ``flows_package`` and register one Prefect Deployment per ETL.

    Each deployment targets ``work_pool`` (an ECS-typed pool in prod, a
    docker-typed pool in local dev) and carries the per-ETL
    ``job_variables`` from the YAML's ``environments.<env>`` block.

    Args:
        flows_package: Dotted package containing one module per ETL.
        work_pool: Prefect work pool name. Default matches the prod ECS
            pool; override with ``loom-docker`` for local dev.
        env: Environment key used to pull ``job_variables`` from each
            ETL's YAML.

    Returns:
        List of deployment ids.
    """
    pkg = importlib.import_module(flows_package)
    pkg_path = getattr(pkg, "__path__", None)
    if pkg_path is None:
        raise ValueError(f"{flows_package!r} is not a package")

    deployment_ids: list[str] = []
    for module_info in pkgutil.iter_modules(pkg_path):
        module = importlib.import_module(f"{flows_package}.{module_info.name}")
        for attr in vars(module).values():
            meta = getattr(attr, _LOOM_ETL_META_ATTR, None)
            if meta is None:
                continue
            deployment_ids.append(_deploy_single(attr, meta, work_pool, env))
    return deployment_ids


def _deploy_single(flow_obj: Any, meta: _ETLFlowMeta, work_pool: str, env: str) -> str:
    """Register one deployment.

    Prefect 3 quirk: ``Flow.deploy()`` does NOT accept an ``entrypoint``
    kwarg. For flows with synthesized signatures (our case — ``flow.fn``
    points back at this module), the canonical override is
    ``flow.from_source(source=..., entrypoint=...).deploy(image=..., ...)``.
    We point ``source`` at the in-image working_dir so the pull step's
    ``set_working_directory <path>`` resolves inside the spawned
    container (which uses the SAME image as the deploy step).
    """
    pool_env_config = meta.pool_config.get(env, {})
    job_variables = dict(pool_env_config.get("job_variables") or {})
    actual_pool = pool_env_config.get("work_pool") or work_pool

    image = job_variables.pop("image", None)
    working_dir = job_variables.get("working_dir", "/app/src")

    # Entrypoint = path relative to ``working_dir``. The flow_file we
    # captured at factory time may live under a different absolute prefix
    # on the deploy host, so we compute it via the ``pipelines`` package
    # anchor when ``relative_to`` fails.
    flow_file = Path(meta.source_file)
    try:
        relative = flow_file.relative_to(Path(working_dir))
    except ValueError:
        parts = flow_file.parts
        anchor = next(
            (i for i, p in enumerate(parts) if p == Path(working_dir).name),
            None,
        )
        relative = Path(*parts[anchor + 1 :]) if anchor is not None else Path(flow_file.name)
    entrypoint = f"{relative.as_posix()}:{flow_obj.fn.__name__}"

    sourced = flow_obj.from_source(source=working_dir, entrypoint=entrypoint)

    kwargs: dict[str, Any] = {
        "name": meta.name,
        "work_pool_name": actual_pool,
        "build": False,
        "push": False,
        "tags": ["loom-etl", meta.name],
        "parameters": dict(meta.raw_params),
        "job_variables": job_variables,
        "enforce_parameter_schema": False,
    }
    if image is not None:
        kwargs["image"] = image
    schedule = _build_cron_schedule(meta.schedule)
    if schedule is not None:
        kwargs["schedules"] = [schedule]
    return str(sourced.deploy(**kwargs))


# ---------------------------------------------------------------------------
# Execution helpers
# ---------------------------------------------------------------------------


def _collect_step_names(
    plan: PipelinePlan,
    processes: tuple[str, ...] | None,
) -> list[str]:
    """Flatten the compiled plan into the ordered list of step class names.

    Honours the optional ``processes`` filter from :class:`FlowCtx` so users
    can scope a run to a subset of processes (e.g. ``processes=("Raw",)``).
    """
    names: list[str] = []

    def _add_process(proc: ProcessPlan) -> None:
        if processes is not None and proc.process_type.__name__ not in processes:
            return

        def _add_step(step: StepPlan) -> None:
            names.append(step.step_type.__name__)

        visit_process_nodes(proc.nodes, _add_step)

    visit_pipeline_nodes(plan.nodes, _add_process)
    return names


class _ManifestObserver:
    """``LifecycleObserver`` that mirrors loom STEP events into the manifest.

    Persists the manifest after every state transition so a failure mid-run
    leaves a recoverable snapshot. On full success the flow body deletes
    the manifest entirely; on failure it stays and the next attempt loads
    it via :func:`completed_steps` to skip already-SUCCESS steps.
    """

    def __init__(self, store: ManifestStore, initial: RunManifest) -> None:
        self._store = store
        self._manifest = initial

    def on_event(self, event: Any) -> None:
        # Lazy import to avoid a cycle in case loom.core changes later.
        from loom.core.observability.event import EventKind, Scope  # noqa: PLC0415

        if event.scope is not Scope.STEP:
            return
        if event.kind is EventKind.END:
            status = RunStatus.SUCCESS
        elif event.kind is EventKind.ERROR:
            status = RunStatus.FAILED
        else:
            return  # START events have no manifest transition

        self._manifest = mark_step(self._manifest, event.name, status)
        try:
            self._store.save(self._manifest)
        except Exception:  # noqa: BLE001
            _log.warning(
                "manifest save failed for step=%s status=%s",
                event.name,
                status,
                exc_info=True,
            )


# ---------------------------------------------------------------------------
# YAML + signature helpers
# ---------------------------------------------------------------------------


def _read_yaml(config_path: str) -> dict[str, Any]:
    """Read the per-ETL YAML, applying a single level of ``extends:``."""
    from omegaconf import DictConfig, OmegaConf  # noqa: PLC0415

    raw = OmegaConf.load(config_path)
    extends_target = None
    if isinstance(raw, DictConfig):
        extends_target = raw.get("extends")
    if extends_target:
        base_path = (Path(config_path).parent / str(extends_target)).resolve()
        base = OmegaConf.load(str(base_path))
        raw = OmegaConf.merge(base, raw)
    container = OmegaConf.to_container(raw, resolve=False)
    if not isinstance(container, dict):
        raise ValueError(f"{config_path}: top-level YAML must be a mapping")
    container.pop("extends", None)
    return {str(k): v for k, v in container.items()}


def _extract_pool_config(raw_cfg: dict[str, Any]) -> dict[str, dict[str, Any]]:
    environments = raw_cfg.get("environments") or {}
    out: dict[str, dict[str, Any]] = {}
    for env_name, env_block in environments.items():
        if not isinstance(env_block, dict):
            continue
        out[env_name] = {
            "work_pool": env_block.get("work_pool"),
            "job_variables": env_block.get("job_variables", {}) or {},
        }
    return out


def _signature_from_params_type(
    params_type: type[msgspec.Struct],
) -> list[inspect.Parameter]:
    hints = get_type_hints(params_type)
    return [
        inspect.Parameter(
            name=field.name,
            kind=inspect.Parameter.KEYWORD_ONLY,
            default=inspect.Parameter.empty,
            annotation=hints.get(field.name, Any),
        )
        for field in msgspec.structs.fields(params_type)
    ]


def _is_datetime_annotation(annotation: Any) -> bool:
    """Return True iff ``annotation`` is ``datetime`` or a union including it.

    Strictly rejects ``date`` (which is a supertype of ``datetime``) so we
    only touch fields that are explicitly typed as ``datetime``.
    """
    if annotation is datetime:
        return True
    if get_origin(annotation) in (Union, UnionType):
        return any(arg is datetime for arg in get_args(annotation))
    return False


def _coerce_to_utc(value: Any) -> Any:
    """Promote a naive ``datetime`` (or naive ISO string) to UTC-aware.

    Returns the original value unchanged when it is already tz-aware, when
    it is ``None``, or when it is a string that cannot be parsed as a naive
    ISO datetime.
    """
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return value
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else value
    return value


def _normalize_datetime_fields(
    resolved: dict[str, Any],
    params_type: type[msgspec.Struct],
) -> dict[str, Any]:
    """Normalize naive datetime fields to UTC-aware.

    Prefect injects naive datetime strings via CLI/UI (e.g.
    ``'2026-06-03T00:00:00'``); Loom comparisons against Polars columns
    require UTC-aware datetimes. This function inspects ``params_type`` and
    promotes any value bound to a ``datetime``-typed field to UTC when it is
    naive, leaving aware datetimes, ``None``, and non-datetime fields
    untouched (idempotent).

    Args:
        resolved: Parameter mapping with placeholders already resolved.
        params_type: ``msgspec.Struct`` describing the expected field types.

    Returns:
        A new dict with naive datetime values promoted to UTC-aware.
    """
    hints = get_type_hints(params_type)
    datetime_fields = {
        field.name
        for field in msgspec.structs.fields(params_type)
        if _is_datetime_annotation(hints.get(field.name))
    }
    if not datetime_fields:
        return resolved
    return {
        key: _coerce_to_utc(value) if key in datetime_fields else value
        for key, value in resolved.items()
    }


_UNSAFE_CORR_CHARS = re.compile(r"[^a-zA-Z0-9_\-]")


def _compute_correlation_id(
    flow_name: str,
    correlation_field: str | None,
    resolved: dict[str, Any],
) -> str:
    if correlation_field is None:
        return f"{flow_name}-{uuid4().hex}"
    value = resolved[correlation_field]
    # datetimes are common as correlation_field (window-based ETLs) — render
    # them compactly without ``:`` / ``+`` / ``.`` so the value is safe for
    # S3 keys, filesystem paths, and the manifest store's regex.
    rendered = value.strftime("%Y%m%dT%H%M%S") if isinstance(value, datetime | date) else str(value)
    safe = _UNSAFE_CORR_CHARS.sub("_", rendered)
    return f"{flow_name}-{safe}"


def _build_cron_schedule(schedule: dict[str, Any] | None) -> Any | None:
    if schedule is None or not schedule.get("enabled", True):
        return None
    cron = schedule.get("cron")
    if cron is None:
        return None
    try:
        from prefect.client.schemas.actions import (  # noqa: PLC0415
            DeploymentScheduleCreate,
        )
        from prefect.client.schemas.schedules import CronSchedule  # noqa: PLC0415
    except ImportError:  # pragma: no cover
        return None
    return DeploymentScheduleCreate(
        schedule=CronSchedule(cron=cron, timezone=schedule.get("timezone")),
        active=True,
        max_scheduled_runs=int(schedule.get("max_scheduled_runs", 1)),
    )
