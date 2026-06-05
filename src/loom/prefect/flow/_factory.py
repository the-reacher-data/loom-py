"""Per-ETL Prefect flow factory.

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

The flow's signature is **synthesised from the params type** so the
Prefect UI shows a typed parameter form and ``-p key=value`` CLI
overrides work.

Architecture: this flow runs inside whatever container the Prefect
worker provisions. Per-step ``@prefect.task`` rows appear in the UI
under this single flow run; the runner itself executes loom steps
in-process.
"""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any

import msgspec
import prefect

from loom.etl.compiler import ETLCompiler
from loom.etl.pipeline import ETLPipeline
from loom.prefect._config import FlowConfig, _load_flow_config
from loom.prefect._meta import LOOM_ETL_META_ATTR, ETLFlowMeta
from loom.prefect.deploy._schedule import extract_pool_config
from loom.prefect.deploy._yaml import read_yaml
from loom.prefect.flow._body import build_flow_body
from loom.prefect.flow._hooks import make_notification_hooks, pause_schedule_on_failure
from loom.prefect.flow._run_name import make_run_name_callback
from loom.prefect.flow._signature import signature_from_params_type
from loom.prefect.manifest import ManifestStore
from loom.prefect.notify import build_notifiers


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
    """Build a per-ETL Prefect flow with a typed, synthesised signature.

    Args:
        name: Logical ETL name. Used as the Prefect flow name AND the
            deployment name; operators see this verbatim in the UI.
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
    raw_cfg = read_yaml(config_path)
    correlation_field = raw_cfg.get("correlation_field")
    schedule = raw_cfg.get("schedule")
    raw_params = dict(raw_cfg.get("params") or {})
    pool_config = extract_pool_config(raw_cfg)
    tags = _coerce_tags(raw_cfg.get("tags"))
    notifiers = build_notifiers(raw_cfg.get("notifications"))

    plan = ETLCompiler().compile(pipeline)

    flow_cfg = _resolve_flow_config(flow_config_path, pipeline)

    resolved_config_path = str(Path(config_path).resolve())
    resolved_source_file = str(Path(source_file).resolve())

    new_signature = _synthesise_signature(params_type)

    flow_body = build_flow_body(
        flow_name=name,
        pipeline=pipeline,
        params_type=params_type,
        plan=plan,
        correlation_field=correlation_field,
        storage_config_path=storage_config_path,
        manifest_store=manifest_store,
    )
    safe_name = name.replace("-", "_")
    flow_body.__signature__ = new_signature
    flow_body.__name__ = safe_name
    flow_body.__qualname__ = safe_name

    failure_hooks, completion_hooks = make_notification_hooks(name, notifiers)
    decorated = prefect.flow(
        name=name,
        flow_run_name=make_run_name_callback(name, correlation_field),
        retries=flow_cfg.flow_retries,
        retry_delay_seconds=flow_cfg.flow_retry_delay_seconds,
        validate_parameters=False,
        on_failure=[pause_schedule_on_failure, *failure_hooks],
        on_completion=completion_hooks or None,
    )(flow_body)
    setattr(
        decorated,
        LOOM_ETL_META_ATTR,
        ETLFlowMeta(
            name=name,
            config_path=resolved_config_path,
            source_file=resolved_source_file,
            correlation_field=correlation_field,
            schedule=schedule,
            raw_params=raw_params,
            pool_config=pool_config,
            tags=tags,
        ),
    )
    return decorated


def _coerce_tags(raw: Any) -> tuple[str, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise TypeError(f"tags: expected a list of strings, got {type(raw).__name__}")
    coerced: list[str] = []
    for value in raw:
        if not isinstance(value, str):
            raise TypeError(f"tags: every entry must be str, got {type(value).__name__}")
        coerced.append(value)
    return tuple(coerced)


def _resolve_flow_config(
    flow_config_path: str | None, pipeline: type[ETLPipeline[Any]]
) -> FlowConfig:
    if flow_config_path is None:
        return FlowConfig()
    try:
        return _load_flow_config(flow_config_path, pipeline.__name__)
    except KeyError:
        # Pipeline not listed under ``flows:`` in the YAML — fall back to
        # defaults. Any other error (missing file, bad YAML) propagates.
        return FlowConfig()


def _synthesise_signature(params_type: type[msgspec.Struct]) -> inspect.Signature:
    user_parameters = signature_from_params_type(params_type)
    universal_parameters = [
        inspect.Parameter("env", inspect.Parameter.KEYWORD_ONLY, default="prod", annotation=str),
        inspect.Parameter(
            "correlation_id",
            inspect.Parameter.KEYWORD_ONLY,
            default=None,
            annotation=str | None,
        ),
    ]
    return inspect.Signature(
        parameters=user_parameters + universal_parameters,
        return_annotation=int,
    )


__all__ = ["etl_flow"]
