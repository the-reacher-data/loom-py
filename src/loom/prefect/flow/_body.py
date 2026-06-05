"""The runtime body that ``etl_flow()`` returns inside the ``@prefect.flow``.

Split into small helpers so each unit has a single responsibility:

- :func:`build_flow_body` is the entrypoint — it closes over the factory
  state and returns the ``_flow_body(**kwargs)`` callable Prefect calls.
- :func:`_load_or_init_manifest` and :func:`_resolve_pending` together
  decide which steps still need to run.
- :func:`_build_observers` wires the Prefect-side observers loom's
  runtime will dispatch lifecycle events to.
- :func:`_invoke_runner` constructs the ``ETLRunner`` and runs the
  pending steps, then deletes the manifest on success.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import msgspec

from loom.etl.compiler import flatten_step_names
from loom.etl.compiler._plan import PipelinePlan
from loom.etl.pipeline import ETLPipeline
from loom.etl.runner import ETLRunner
from loom.prefect._ctx import FlowCtx
from loom.prefect._placeholders import resolve_placeholder
from loom.prefect.flow._run_name import compute_correlation_id
from loom.prefect.flow._signature import normalize_datetime_fields
from loom.prefect.manifest import (
    ManifestStore,
    RunManifest,
    completed_steps,
)
from loom.prefect.observer import ManifestObserver, PrefectTaskRunObserver
from loom.prefect.observer._logging_bridge import (
    install_log_bridge,
    uninstall_log_bridge,
)


def build_flow_body(
    *,
    flow_name: str,
    pipeline: type[ETLPipeline[Any]],
    params_type: type[msgspec.Struct],
    plan: PipelinePlan,
    correlation_field: str | None,
    storage_config_path: str,
    manifest_store: ManifestStore | None,
) -> Any:
    """Return the ``_flow_body(**kwargs)`` callable bound to the factory state.

    The returned callable is what Prefect runs inside the
    ``@prefect.flow`` wrapper; ``etl_flow()`` decorates it after
    attaching a synthesised ``__signature__``.

    Args:
        flow_name: Logical ETL name (used in correlation_id and run_id).
        pipeline: ``ETLPipeline`` subclass to execute.
        params_type: Struct used to decode the bound parameters.
        plan: Pre-compiled pipeline plan.
        correlation_field: Parameter whose value seeds the correlation_id.
        storage_config_path: Loom storage YAML path. Overridable at
            runtime via ``LOOM_STORAGE_CONFIG_PATH``.
        manifest_store: Cross-attempt resume backend, or ``None``.

    Returns:
        The flow body callable. Returns ``0`` on success, raises on
        runner failure.
    """

    def _flow_body(**kwargs: Any) -> int:
        env = kwargs.pop("env", "prod")
        explicit_correlation = kwargs.pop("correlation_id", None)
        resolved = {key: resolve_placeholder(value) for key, value in kwargs.items()}
        resolved = normalize_datetime_fields(resolved, params_type)
        params_obj = msgspec.convert(resolved, type=params_type)
        ctx = FlowCtx(
            correlation_id=(
                explicit_correlation
                or compute_correlation_id(flow_name, correlation_field, resolved)
            ),
            run_id=f"{flow_name}-{uuid4().hex[:8]}",
            environment=env,
        )

        actual_config_path = os.environ.get("LOOM_STORAGE_CONFIG_PATH") or storage_config_path

        manifest = _load_or_init_manifest(manifest_store, ctx.correlation_id)
        pending = _resolve_pending(plan, ctx.processes, manifest)

        if not pending:
            _maybe_delete_manifest(manifest_store, ctx.correlation_id)
            return 0

        flow_run_id = _current_flow_run_id()
        install_log_bridge(flow_run_id)
        try:
            observers = _build_observers(flow_run_id, manifest_store, manifest)
            _invoke_runner(
                actual_config_path,
                pipeline,
                params_obj,
                pending,
                ctx,
                observers,
            )
        finally:
            uninstall_log_bridge()
        # Cleared on full success; on failure the manifest stays so the next
        # attempt can skip SUCCESS steps via ``include=pending``.
        _maybe_delete_manifest(manifest_store, ctx.correlation_id)
        return 0

    return _flow_body


def _load_or_init_manifest(store: ManifestStore | None, correlation_id: str) -> RunManifest:
    loaded = store.load(correlation_id) if store is not None else None
    if loaded is not None:
        return loaded
    return RunManifest(
        correlation_id=correlation_id,
        steps=(),
        updated_at=datetime.now(tz=UTC),
    )


def _resolve_pending(
    plan: PipelinePlan,
    processes: tuple[str, ...] | None,
    manifest: RunManifest,
) -> list[str]:
    done = completed_steps(manifest)
    all_step_names = flatten_step_names(plan, processes)
    return [s for s in all_step_names if s not in done]


def _build_observers(
    flow_run_id: Any,
    manifest_store: ManifestStore | None,
    manifest: RunManifest,
) -> list[Any]:
    observers: list[Any] = []
    if flow_run_id is not None:
        observers.append(PrefectTaskRunObserver(flow_run_id=flow_run_id))
    if manifest_store is not None:
        observers.append(ManifestObserver(manifest_store, manifest))
    prometheus = _maybe_build_prometheus_adapter()
    if prometheus is not None:
        observers.append(prometheus)
    return observers


def _maybe_build_prometheus_adapter() -> Any:
    pushgateway = os.environ.get("PROMETHEUS_PUSHGATEWAY_URL")
    if not pushgateway:
        return None
    try:
        from loom.prometheus.lifecycle import PrometheusLifecycleAdapter  # noqa: PLC0415
    except ImportError:
        return None
    return PrometheusLifecycleAdapter(pushgateway_url=pushgateway)


def _current_flow_run_id() -> Any:
    try:
        from prefect.runtime import flow_run as _fr  # noqa: PLC0415

        return _fr.id
    except Exception:  # noqa: BLE001
        return None


def _invoke_runner(
    config_path: str,
    pipeline: type[ETLPipeline[Any]],
    params_obj: Any,
    pending: list[str],
    ctx: FlowCtx,
    observers: list[Any],
) -> None:
    runner = ETLRunner.from_yaml(config_path, extra_observers=observers)
    runner.run(
        pipeline,
        params_obj,
        include=pending,
        run_id=ctx.run_id,
        correlation_id=ctx.correlation_id,
    )


def _maybe_delete_manifest(store: ManifestStore | None, correlation_id: str) -> None:
    if store is None:
        return
    store.delete(correlation_id)


__all__ = ["build_flow_body"]
