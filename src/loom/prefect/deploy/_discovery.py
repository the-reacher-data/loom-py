"""Walk a flows package and register one Prefect deployment per ETL."""

from __future__ import annotations

import importlib
import pkgutil
from pathlib import Path
from typing import Any

from loom.prefect._meta import LOOM_ETL_META_ATTR, ETLFlowMeta
from loom.prefect.deploy._schedule import build_cron_schedule


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

    Raises:
        ValueError: When ``flows_package`` is not actually a package
            (has no ``__path__``).
    """
    pkg = importlib.import_module(flows_package)
    pkg_path = getattr(pkg, "__path__", None)
    if pkg_path is None:
        raise ValueError(f"{flows_package!r} is not a package")

    deployment_ids: list[str] = []
    for module_info in pkgutil.iter_modules(pkg_path):
        module = importlib.import_module(f"{flows_package}.{module_info.name}")
        for attr in vars(module).values():
            meta = getattr(attr, LOOM_ETL_META_ATTR, None)
            if meta is None:
                continue
            deployment_ids.append(_deploy_single(attr, meta, work_pool, env))
    return deployment_ids


def _deploy_single(flow_obj: Any, meta: ETLFlowMeta, work_pool: str, env: str) -> str:
    """Register one deployment.

    Prefect 3 quirk: ``Flow.deploy()`` does NOT accept an ``entrypoint``
    kwarg. For flows with synthesised signatures (our case — ``flow.fn``
    points back at this module), the canonical override is
    ``flow.from_source(source=..., entrypoint=...).deploy(image=..., ...)``.
    """
    pool_env_config = meta.pool_config.get(env, {})
    job_variables = dict(pool_env_config.get("job_variables") or {})
    actual_pool = pool_env_config.get("work_pool") or work_pool

    image = job_variables.pop("image", None)
    working_dir = job_variables.get("working_dir", "/app/src")

    # Entrypoint = path relative to ``working_dir``. The flow_file we
    # captured at factory time may live under a different absolute prefix
    # on the deploy host, so we compute it via the working_dir anchor
    # when ``relative_to`` fails.
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
    schedule = build_cron_schedule(meta.schedule)
    if schedule is not None:
        kwargs["schedules"] = [schedule]
    return str(sourced.deploy(**kwargs))


__all__ = ["discover_and_deploy_etls"]
