"""Register one Prefect deployment per ETL, from a flows package or from YAML."""

from __future__ import annotations

import importlib
import os
import pkgutil
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from loom.prefect._meta import LOOM_ETL_META_ATTR, ETLFlowMeta
from loom.prefect.deploy._schedule import build_cron_schedule
from loom.prefect.deploy._yaml_etls import EtlDeclaration, read_declarations
from loom.prefect.deploy.entrypoint import LOOM_ETL_CONFIG, build_flow

ENTRYPOINT_MODULE = "loom.prefect.deploy.entrypoint"


def discover_and_deploy_etls(
    *,
    flows_package: str | None = None,
    config: str | None = None,
    work_pool: str = "loom-fargate",
    env: str = "prod",
) -> list[str]:
    """Register one Prefect Deployment per ETL.

    ETLs come either from ``flows_package`` (one module per ETL calling
    ``etl_flow``) or from ``config`` (YAML declarations rebuilt at run time
    through :mod:`loom.prefect.deploy.entrypoint`). Each deployment targets
    ``work_pool`` (an ECS-typed pool in prod, a docker-typed pool in local
    dev) and carries the per-ETL ``job_variables`` from the YAML's
    ``environments.<env>`` block.

    Args:
        flows_package: Dotted package containing one module per ETL.
        config: Local path, cloud URI or glob of ETL declaration files.
        work_pool: Prefect work pool name. Default matches the prod ECS
            pool; override with ``loom-docker`` for local dev.
        env: Environment key used to pull ``job_variables`` from each
            ETL's YAML.

    Returns:
        List of deployment ids.

    Raises:
        ValueError: When both or neither of ``flows_package`` and ``config``
            are given, or when ``flows_package`` is not a package.
        ConfigError: When a declaration in ``config`` is invalid; raised
            before any deployment is registered.
    """
    if (flows_package is None) == (config is None):
        raise ValueError("pass exactly one of flows_package= or config=")
    if flows_package is not None:
        return _deploy_package(flows_package, work_pool, env)
    return [_deploy_declaration(d, work_pool, env) for d in read_declarations(str(config))]


def _deploy_package(flows_package: str, work_pool: str, env: str) -> list[str]:
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


def _deploy_declaration(declaration: EtlDeclaration, work_pool: str, env: str) -> str:
    flow_obj = build_flow(declaration)
    meta = getattr(flow_obj, LOOM_ETL_META_ATTR)
    with _exported(LOOM_ETL_CONFIG, declaration.config_uri):
        return _deploy_single(
            flow_obj,
            meta,
            work_pool,
            env,
            entrypoint=f"{ENTRYPOINT_MODULE}.{declaration.attribute}",
            extra_env={LOOM_ETL_CONFIG: declaration.config_uri},
        )


@contextmanager
def _exported(name: str, value: str) -> Iterator[None]:
    """Set ``name`` in the process environment, restoring the previous state on exit."""
    previous = os.environ.get(name)
    os.environ[name] = value
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous


def _deploy_single(
    flow_obj: Any,
    meta: ETLFlowMeta,
    work_pool: str,
    env: str,
    *,
    entrypoint: str | None = None,
    extra_env: Mapping[str, str] | None = None,
) -> str:
    """Register one deployment.

    Prefect 3 quirk: ``Flow.deploy()`` does NOT accept an ``entrypoint``
    kwarg. For flows with synthesised signatures (our case — ``flow.fn``
    points back at this module), the canonical override is
    ``flow.from_source(source=..., entrypoint=...).deploy(image=..., ...)``.
    ``entrypoint`` replaces the file-relative one computed from
    ``meta.source_file``; ``extra_env`` is merged under ``job_variables.env``
    with the user's keys winning.
    """
    pool_env_config = meta.pool_config.get(env, {})
    job_variables = dict(pool_env_config.get("job_variables") or {})
    actual_pool = pool_env_config.get("work_pool") or work_pool

    image = job_variables.pop("image", None)
    working_dir = job_variables.get("working_dir", "/app/src")
    if extra_env:
        job_variables["env"] = {**extra_env, **(job_variables.get("env") or {})}
    if entrypoint is None:
        entrypoint = _file_entrypoint(flow_obj, meta, working_dir)

    sourced = flow_obj.from_source(source=working_dir, entrypoint=entrypoint)

    kwargs: dict[str, Any] = {
        "name": meta.name,
        "work_pool_name": actual_pool,
        "build": False,
        "push": False,
        "tags": [meta.name, *meta.tags],
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


def _file_entrypoint(flow_obj: Any, meta: ETLFlowMeta, working_dir: str) -> str:
    """Return ``<path relative to working_dir>:<flow function>``.

    The source file captured at factory time may live under a different
    absolute prefix on the deploy host, so the path is re-anchored on the
    ``working_dir`` name when ``relative_to`` fails.
    """
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
    return f"{relative.as_posix()}:{flow_obj.fn.__name__}"


__all__ = ["discover_and_deploy_etls"]
