"""Per-ETL Prefect flow factory."""

from __future__ import annotations

import inspect
from typing import Any

import msgspec

from loom.etl.compiler import ETLCompiler
from loom.etl.pipeline import ETLPipeline
from loom.prefect._config import FlowConfig, _load_flow_config
from loom.prefect._meta import DEFAULT_STORAGE_CONFIG_PATH
from loom.prefect.flow._assemble import FlowSettings, assemble_flow, load_flow_settings
from loom.prefect.flow._body import build_flow_body
from loom.prefect.flow._signature import synthesise_flow_signature
from loom.prefect.manifest import ManifestStore

_EXTRA_PARAMETERS = [
    inspect.Parameter(
        "correlation_id",
        inspect.Parameter.KEYWORD_ONLY,
        default=None,
        annotation=str | None,
    ),
    inspect.Parameter(
        "processes",
        inspect.Parameter.KEYWORD_ONLY,
        default=None,
        annotation=list[str] | None,
    ),
]


def etl_flow(
    *,
    name: str,
    pipeline: type[ETLPipeline[Any]],
    params_type: type[msgspec.Struct],
    config_path: str,
    source_file: str,
    storage_config_path: str = DEFAULT_STORAGE_CONFIG_PATH,
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
    settings = load_flow_settings(config_path)
    return build_etl_flow(
        name=name,
        pipeline=pipeline,
        params_type=params_type,
        settings=settings,
        config_path=config_path,
        source_file=source_file,
        storage_config_path=storage_config_path,
        manifest_store=manifest_store,
        flow_config=_resolve_flow_config(flow_config_path, pipeline, settings),
    )


def build_etl_flow(
    *,
    name: str,
    pipeline: type[ETLPipeline[Any]],
    params_type: type[msgspec.Struct],
    settings: FlowSettings,
    config_path: str,
    source_file: str,
    storage_config_path: str = DEFAULT_STORAGE_CONFIG_PATH,
    manifest_store: ManifestStore | None = None,
    flow_config: FlowConfig | None = None,
) -> Any:
    """Assemble the per-ETL flow from already-resolved settings.

    Args:
        name: Logical ETL name (Prefect flow name AND deployment name).
        pipeline: ``ETLPipeline`` subclass to execute.
        params_type: ``msgspec.Struct`` whose fields become flow kwargs.
        settings: Parsed per-flow settings (from a file or a mapping).
        config_path: Path or URI of the per-ETL YAML recorded in the metadata.
        source_file: ``__file__`` of the module Prefect loads the flow from.
        storage_config_path: Storage YAML path read at flow-run time.
        manifest_store: Optional store for cross-attempt resume.
        flow_config: Retry policy applied to the Prefect flow; defaults to
            ``settings.retry_policy``.

    Returns:
        A ``@prefect.flow``-decorated callable with ``__loom_etl_meta__``.
    """
    if flow_config is None:
        flow_config = settings.retry_policy
    plan = ETLCompiler().compile(pipeline)
    flow_body = build_flow_body(
        flow_name=name,
        pipeline=pipeline,
        params_type=params_type,
        plan=plan,
        correlation_field=settings.correlation_field,
        storage_config_path=storage_config_path,
        manifest_store=manifest_store,
    )
    return assemble_flow(
        name=name,
        body=flow_body,
        signature=synthesise_flow_signature(params_type, extra_parameters=_EXTRA_PARAMETERS),
        settings=settings,
        config_path=config_path,
        source_file=source_file,
        correlation_field=settings.correlation_field,
        retries=flow_config.flow_retries,
        retry_delay_seconds=flow_config.flow_retry_delay_seconds,
    )


def _resolve_flow_config(
    flow_config_path: str | None,
    pipeline: type[ETLPipeline[Any]],
    settings: FlowSettings,
) -> FlowConfig:
    """Resolve the retry policy, preferring a dedicated file when passed."""
    if flow_config_path is None:
        return settings.retry_policy
    try:
        return _load_flow_config(flow_config_path, pipeline.__name__)
    except KeyError:
        return FlowConfig()


__all__ = ["etl_flow"]
