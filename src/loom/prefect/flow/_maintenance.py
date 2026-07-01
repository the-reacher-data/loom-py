"""Prefect flow factory for MaintenanceStep — mirrors etl_flow() structure."""

from __future__ import annotations

import inspect
import os
from pathlib import Path
from typing import Any

import msgspec
import prefect

from loom.etl.maintenance._runner import MaintenanceRunner
from loom.etl.maintenance._step import MaintenanceStep
from loom.etl.runner.config_loader import _load_yaml
from loom.prefect._meta import LOOM_ETL_META_ATTR, ETLFlowMeta
from loom.prefect._placeholders import resolve_placeholder
from loom.prefect._summary import set_run_summary
from loom.prefect.deploy._schedule import extract_pool_config
from loom.prefect.deploy._yaml import read_yaml
from loom.prefect.flow._common import coerce_tags as _coerce_tags
from loom.prefect.flow._common import prefect_flow_run_id
from loom.prefect.flow._hooks import make_notification_hooks, pause_schedule_on_failure
from loom.prefect.flow._run_name import make_run_name_callback
from loom.prefect.flow._signature import normalize_datetime_fields, signature_from_params_type
from loom.prefect.notify import build_notifiers
from loom.prefect.observer._logging_bridge import install_log_bridge, uninstall_log_bridge


def maintenance_flow(
    *,
    name: str,
    step: type[MaintenanceStep[Any]],
    params_type: type[msgspec.Struct],
    config_path: str,
    source_file: str,
    storage_config_path: str = "/app/config.yaml",
) -> Any:
    """Build a Prefect flow for a :class:`~loom.etl.maintenance.MaintenanceStep`.

    Mirrors :func:`~loom.prefect.etl_flow` so the deployment machinery
    (schedule, tags, notifications, work-pool config) works identically.

    Maintenance flows do not support retries at the Prefect level (vacuum and
    compaction are idempotent but long-running; retries should be triggered
    manually by the operator if needed).  They also do not use a
    ``ManifestStore`` — each run starts fresh.

    Args:
        name: Logical flow name shown in the Prefect UI.
        step: :class:`~loom.etl.maintenance.MaintenanceStep` subclass to run.
        params_type: ``msgspec.Struct`` whose fields become typed flow kwargs.
        config_path: Path to the per-flow YAML (schedule, params, tags, …).
        source_file: ``__file__`` of the calling module (needed by Prefect
            ``from_source`` to locate the flow on disk).
        storage_config_path: Path to the loom storage YAML read at runtime
            inside the container. Defaults to ``/app/config.yaml``.

    Returns:
        A ``@prefect.flow``-decorated callable with ``__loom_etl_meta__``
        attached for the deployer.
    """
    raw_cfg = read_yaml(config_path)
    schedule = raw_cfg.get("schedule")
    raw_params = dict(raw_cfg.get("params") or {})
    pool_config = extract_pool_config(raw_cfg)
    tags = _coerce_tags(raw_cfg.get("tags"))
    notifiers = build_notifiers(raw_cfg.get("notifications"))

    resolved_config_path = str(Path(config_path).resolve())
    resolved_source_file = str(Path(source_file).resolve())

    def _flow_body(**kwargs: Any) -> None:
        # "env" is exposed in the synthesised signature so Prefect accepts it,
        # but maintenance flows do not route by environment — drained here.
        kwargs.pop("env", "prod")
        resolved = {k: resolve_placeholder(v) for k, v in kwargs.items()}
        resolved = normalize_datetime_fields(resolved, params_type)
        params = msgspec.convert(resolved, type=params_type)
        actual_path = os.environ.get("LOOM_STORAGE_CONFIG_PATH") or storage_config_path
        # Bridge loom logs into the Prefect run (mirrors etl_flow).
        install_log_bridge(prefect_flow_run_id())
        try:
            storage_config, _ = _load_yaml(actual_path)
            report = MaintenanceRunner.from_config(storage_config).run(step, params=params)
        finally:
            uninstall_log_bridge()
        set_run_summary(_maintenance_summary(report, resolved))
        report.raise_if_errors()

    safe_name = name.replace("-", "_")
    body: Any = _flow_body  # cast to Any — __signature__ is a valid runtime attribute
    body.__signature__ = _synthesise_signature(params_type)
    body.__name__ = safe_name
    body.__qualname__ = safe_name

    failure_hooks, completion_hooks = make_notification_hooks(name, notifiers)
    decorated = prefect.flow(
        name=name,
        flow_run_name=make_run_name_callback(name, None),
        validate_parameters=False,
        on_failure=[pause_schedule_on_failure, *failure_hooks],
        on_completion=completion_hooks or None,
    )(body)
    setattr(
        decorated,
        LOOM_ETL_META_ATTR,
        ETLFlowMeta(
            name=name,
            config_path=resolved_config_path,
            source_file=resolved_source_file,
            correlation_field=None,
            schedule=schedule,
            raw_params=raw_params,
            pool_config=pool_config,
            tags=tags,
        ),
    )
    return decorated


def _maintenance_summary(report: Any, params: dict[str, Any]) -> str:
    """Format a one-line summary from a MaintenanceReport.

    Examples:
        ``5 tables — vacuum ✓  compact ✓  dry_run: false``
        ``3/5 tables failed: raw.events, staging.snapshots``
    """
    from loom.etl.maintenance._runner import MaintenanceReport  # noqa: PLC0415

    if not isinstance(report, MaintenanceReport):
        return ""

    total = len(report.results)
    failed = [r for r in report.results if not r.ok]

    dry_run = params.get("dry_run")
    dry_run_tag = f"  dry_run: {str(dry_run).lower()}" if dry_run is not None else ""

    if failed:
        refs = ", ".join(r.table_ref for r in failed)
        return f"{len(failed)}/{total} tables failed: {refs}{dry_run_tag}"

    # Collect op names that ran across all tables
    op_names: list[str] = []
    seen: set[str] = set()
    for result in report.results:
        for name in result.op_results:
            if name not in seen:
                op_names.append(name)
                seen.add(name)
    ops_str = "  ".join(f"{n} ✓" for n in op_names) if op_names else "no ops"
    return f"{total} tables — {ops_str}{dry_run_tag}"


def _synthesise_signature(params_type: type[msgspec.Struct]) -> inspect.Signature:
    user_params = signature_from_params_type(params_type)
    env_param = inspect.Parameter(
        "env", inspect.Parameter.KEYWORD_ONLY, default="prod", annotation=str
    )
    return inspect.Signature(parameters=user_params + [env_param], return_annotation=None)


__all__ = ["maintenance_flow"]
