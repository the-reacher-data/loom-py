"""Deploy-time flow assembly shared by every flow factory.

``etl_flow``, ``maintenance_flow`` and ``backfill_flow`` all read the same
per-flow YAML settings and produce the same artefact: a ``@prefect.flow``
decorated body with :class:`~loom.prefect._meta.ETLFlowMeta` attached for the
deployer. This module owns that boilerplate so a change to the decoration or
the metadata contract is made in exactly one place.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import prefect

from loom.prefect._meta import LOOM_ETL_META_ATTR, ETLFlowMeta
from loom.prefect.deploy._schedule import extract_pool_config
from loom.prefect.deploy._yaml import read_yaml
from loom.prefect.flow._common import coerce_tags
from loom.prefect.flow._hooks import make_notification_hooks, pause_schedule_on_failure
from loom.prefect.flow._run_name import make_run_name_callback
from loom.prefect.notify import Notifier, build_notifiers


@dataclass(frozen=True)
class FlowSettings:
    """Per-flow YAML settings consumed by :func:`assemble_flow`."""

    correlation_field: str | None
    schedule: dict[str, Any] | None
    raw_params: dict[str, Any]
    pool_config: dict[str, dict[str, Any]]
    tags: tuple[str, ...]
    notifiers: tuple[Notifier, ...]


def load_flow_settings(config_path: str) -> FlowSettings:
    """Read the per-flow YAML into the settings every factory needs.

    Args:
        config_path: Path to the per-flow YAML (schedule, params, tags, …).

    Returns:
        Parsed :class:`FlowSettings`.
    """
    raw_cfg = read_yaml(config_path)
    return FlowSettings(
        correlation_field=raw_cfg.get("correlation_field"),
        schedule=raw_cfg.get("schedule"),
        raw_params=dict(raw_cfg.get("params") or {}),
        pool_config=extract_pool_config(raw_cfg),
        tags=coerce_tags(raw_cfg.get("tags")),
        notifiers=build_notifiers(raw_cfg.get("notifications")),
    )


def assemble_flow(
    *,
    name: str,
    body: Callable[..., None],
    signature: inspect.Signature,
    settings: FlowSettings,
    config_path: str,
    source_file: str,
    correlation_field: str | None = None,
    retries: int | None = None,
    retry_delay_seconds: int | None = None,
) -> Any:
    """Decorate *body* as a Prefect flow and attach the deployer metadata.

    Args:
        name: Logical flow name (Prefect flow name AND deployment name).
        body: The ``**kwargs`` flow-body callable produced by a factory.
        signature: Synthesised ``inspect.Signature`` exposed to Prefect.
        settings: Parsed per-flow YAML settings.
        config_path: Path to the per-flow YAML (resolved into the metadata).
        source_file: ``__file__`` of the user's flow module.
        correlation_field: Parameter whose value seeds the run name and the
            correlation id, or ``None`` for timestamp/random naming.
        retries: Prefect flow retries; ``None`` disables retries.
        retry_delay_seconds: Delay between Prefect flow retries.

    Returns:
        The ``@prefect.flow``-decorated callable with ``__loom_etl_meta__``
        attached at :data:`~loom.prefect._meta.LOOM_ETL_META_ATTR`.
    """
    safe_name = name.replace("-", "_")
    flow_body: Any = body  # cast to Any — __signature__ is a valid runtime attribute
    flow_body.__signature__ = signature
    flow_body.__name__ = safe_name
    flow_body.__qualname__ = safe_name

    failure_hooks, completion_hooks = make_notification_hooks(name, settings.notifiers)
    decorated = prefect.flow(
        name=name,
        flow_run_name=make_run_name_callback(name, correlation_field),
        retries=retries,
        retry_delay_seconds=retry_delay_seconds,
        validate_parameters=False,
        on_failure=[pause_schedule_on_failure, *failure_hooks],
        on_completion=completion_hooks or None,
    )(flow_body)
    setattr(
        decorated,
        LOOM_ETL_META_ATTR,
        ETLFlowMeta(
            name=name,
            config_path=str(Path(config_path).resolve()),
            source_file=str(Path(source_file).resolve()),
            correlation_field=correlation_field,
            schedule=settings.schedule,
            raw_params=settings.raw_params,
            pool_config=settings.pool_config,
            tags=settings.tags,
        ),
    )
    return decorated


__all__ = ["FlowSettings", "assemble_flow", "load_flow_settings"]
