"""``deploy_etl`` and the module-level generic ETL flow.

The flow lives at module scope so Prefect 3's ``.deploy()`` can pickle it.
Each ETL re-uses this same function via ``with_options(name=cfg.etl)``.

Trust boundary
--------------
The YAML files consumed here drive Python ``importlib.import_module`` via
``_import_obj`` (``pipeline`` / ``params_type`` dotted refs). Importing
executes module top-level code, which means a YAML that can name any
importable module gets to run that module's import-time side effects
inside the Prefect worker. We assume the YAML is part of the project
repository under CI/CD review. Do NOT extend this loader to accept YAMLs
from untrusted sources without restricting ``_import_obj`` to an
allow-list of dotted prefixes.
"""

from __future__ import annotations

import importlib
from datetime import date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import msgspec
import prefect
from omegaconf import DictConfig, ListConfig, OmegaConf

from loom.core.model import LoomFrozenStruct
from loom.prefect._ctx import FlowCtx
from loom.prefect._launcher import run_etl_in_container
from loom.prefect._launcher_config import _load_etl_yaml, build_launcher
from loom.prefect._placeholders import resolve_placeholder


class _ETLDeployConfig(LoomFrozenStruct, frozen=True, kw_only=True):
    """Resolved per-ETL deploy configuration."""

    etl: str
    pipeline: str | None = None
    params_type: str | None = None
    correlation_field: str | None = None
    params: dict[str, Any] = msgspec.field(default_factory=dict)
    schedule: dict[str, Any] | None = None


def _to_plain(node: Any) -> Any:
    if isinstance(node, (DictConfig, ListConfig)):
        return OmegaConf.to_container(node, resolve=False)
    return node


def _load_etl_config(config_path: str) -> _ETLDeployConfig:
    cfg = _load_etl_yaml(config_path)
    raw: Any = OmegaConf.to_container(cfg, resolve=False)
    if not isinstance(raw, dict):
        raise ValueError(f"{config_path}: top-level YAML must be a mapping")
    return _ETLDeployConfig(
        etl=str(raw["etl"]),
        pipeline=raw.get("pipeline"),
        params_type=raw.get("params_type"),
        correlation_field=raw.get("correlation_field"),
        params=dict(raw.get("params") or {}),
        schedule=raw.get("schedule"),
    )


def _import_obj(dotted: str) -> Any:
    """Import ``"module.path:Attr"`` or ``"module.path.Attr"`` lazily."""
    if ":" in dotted:
        module_path, attr = dotted.split(":", 1)
    else:
        module_path, _, attr = dotted.rpartition(".")
    if not module_path or not attr:
        raise ValueError(f"invalid dotted path: {dotted!r}")
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise ValueError(f"cannot import {module_path!r}: {exc}") from exc
    try:
        return getattr(module, attr)
    except AttributeError as exc:
        raise ValueError(f"{module_path!r} has no attribute {attr!r}") from exc


def _compute_correlation_id(
    cfg: Any,
    resolved: dict[str, Any],
) -> str:
    """Derive a stable correlation id from a configured params field.

    Args:
        cfg: Object exposing ``etl`` and ``correlation_field`` attributes.
        resolved: Mapping of resolved params after placeholder expansion.

    Returns:
        ``"<etl>-<uuid4_hex>"`` when ``correlation_field`` is ``None``;
        otherwise ``"<etl>-<value>"`` where ``value`` is the field rendered
        as ``isoformat()`` for dates/datetimes or ``str()`` for ``str`` / ``int``.

    Raises:
        KeyError: ``correlation_field`` is set but absent from ``resolved``.
        TypeError: Field value is not one of ``str | date | datetime | int``.
    """
    if cfg.correlation_field is None:
        return f"{cfg.etl}-{uuid4().hex}"
    field = cfg.correlation_field
    if field not in resolved:
        raise KeyError(f"correlation_field {field!r} not found in resolved params")
    value = resolved[field]
    if isinstance(value, bool):
        raise TypeError(f"correlation_field {field!r} has unsupported type bool")
    if isinstance(value, (datetime, date)):
        rendered = value.isoformat()
    elif isinstance(value, (str, int)):
        rendered = str(value)
    else:
        raise TypeError(f"correlation_field {field!r} has unsupported type {type(value).__name__}")
    return f"{cfg.etl}-{rendered}"


def _make_schedule(schedule: dict[str, Any] | None) -> Any | None:
    """Translate the YAML ``schedule`` block to a Prefect schedule object.

    Returns ``None`` when ``schedule`` is missing or ``enabled: false``.
    """
    if schedule is None:
        return None
    if not schedule.get("enabled", True):
        return None
    cron = schedule.get("cron")
    if cron is None:
        return None
    try:
        from prefect.client.schemas.schedules import CronSchedule
    except ImportError:  # pragma: no cover — Prefect version mismatch.
        return None
    return CronSchedule(cron=cron, timezone=schedule.get("timezone"))


def _generic_etl_flow_impl(
    config_path: str,
    env: str = "prod",
    correlation_id: str | None = None,
    **raw_params: Any,
) -> int:
    """Generic flow parameterized by an absolute YAML path.

    Resolves placeholders, builds the typed params struct via
    :func:`msgspec.convert`, constructs a :class:`FlowCtx`, and delegates
    to :func:`run_etl_in_container`. ``deploy_etl`` clones this flow
    per-ETL via ``with_options(name=cfg.etl)`` — never define it as a
    closure or Prefect 3 deploy will fail to pickle.

    Args:
        config_path: Absolute path to the per-ETL YAML.
        env: Environment key to look up under ``environments``.
        correlation_id: Optional override; otherwise derived from
            ``correlation_field``.
        **raw_params: Per-ETL parameters with placeholders preserved.

    Returns:
        ``0`` on success.

    Raises:
        ValueError: ``pipeline`` / ``params_type`` missing or not importable.
        KeyError: ``correlation_field`` set but absent from params.
    """
    cfg = _load_etl_config(config_path)
    if cfg.params_type is None:
        raise ValueError(f"{config_path}: params_type must be declared to run the flow")
    params_type = _import_obj(cfg.params_type)

    resolved = {key: resolve_placeholder(value) for key, value in raw_params.items()}
    params = msgspec.convert(resolved, type=params_type)

    corr_id = correlation_id or _compute_correlation_id(cfg, resolved)
    ctx = FlowCtx(
        correlation_id=corr_id,
        run_id=f"{cfg.etl}-{uuid4().hex[:8]}",
        environment=env,
    )
    launcher = build_launcher(config_path=config_path, environment=env)
    return run_etl_in_container(ctx=ctx, params=params, launcher=launcher, flow_name=cfg.etl)


# Module-level Prefect flow. The implementation lives in
# ``_generic_etl_flow_impl`` so the Flow wrapper and the underlying
# function do not share a qualname (otherwise ``pickle.dumps`` of the
# Flow reduces to the inner function, which then fails to round-trip
# because the module attribute ``_generic_etl_flow`` resolves to the
# wrapper rather than the inner function).
_generic_etl_flow = prefect.flow(name="loom-etl-generic")(_generic_etl_flow_impl)


def deploy_etl(
    *,
    config_path: str,
    work_pool: str = "default",
) -> str:
    """Register a Prefect Deployment for a single ETL.

    Loads the YAML, bakes the absolute ``config_path`` plus the default
    params into the deployment parameters, and registers it under
    ``cfg.etl`` via ``with_options(name=cfg.etl).deploy(...)``.

    Args:
        config_path: Path to the per-ETL YAML.
        work_pool: Prefect work pool to register against.

    Returns:
        The Prefect deployment id (UUID string).
    """
    cfg = _load_etl_config(config_path)
    absolute_path = str(Path(config_path).resolve())
    parameters: dict[str, Any] = {
        "config_path": absolute_path,
        "env": "prod",
        **cfg.params,
    }
    deploy_kwargs: dict[str, Any] = {
        "name": cfg.etl,
        "work_pool_name": work_pool,
        "parameters": parameters,
        "tags": ["loom-etl", cfg.etl],
    }
    schedule = _make_schedule(cfg.schedule)
    if schedule is not None:
        # Prefect 3 favors ``schedules=[...]``; the singular ``schedule`` kwarg
        # is deprecated. Always pass as a list of one entry.
        deploy_kwargs["schedules"] = [schedule]

    flow = _generic_etl_flow.with_options(name=cfg.etl)
    # ``flow.deploy`` returns a UUID; cast to ``str`` so the signature is
    # honored regardless of how Prefect represents the id internally.
    return str(flow.deploy(**deploy_kwargs))


__all__ = [
    "_compute_correlation_id",
    "_generic_etl_flow",
    "deploy_etl",
]
