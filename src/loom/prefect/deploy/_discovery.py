"""Register one Prefect deployment per ETL, from a flows package or from YAML."""

from __future__ import annotations

import importlib
import os
import pkgutil
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loom.core.config import ConfigError
from loom.prefect._meta import LOOM_ETL_META_ATTR, ETLFlowMeta
from loom.prefect.deploy import entrypoint
from loom.prefect.deploy._schedule import build_cron_schedule
from loom.prefect.deploy._yaml_etls import EtlDeclaration, read_declarations

_ENV_LOCK = threading.Lock()


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
        ConfigError: When a declaration in ``config`` is invalid or sets
            ``LOOM_ETL_CONFIG`` under ``job_variables.env``; raised before
            any deployment is registered.
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
    flow_obj = entrypoint._build_flow(declaration)
    meta = getattr(flow_obj, LOOM_ETL_META_ATTR)
    recorded_env = {entrypoint.LOOM_ETL_CONFIG: declaration.config_uri}
    plan = _plan(meta, work_pool, env, extra_env=recorded_env)
    with _exported(entrypoint.LOOM_ETL_CONFIG, declaration.config_uri):
        sourced = flow_obj.from_source(
            source=plan.working_dir,
            entrypoint=f"{entrypoint.__name__}.{declaration.attribute}",
        )
    return _register(sourced, meta, plan)


@contextmanager
def _exported(name: str, value: str) -> Iterator[None]:
    """Set ``name`` in the process environment, restoring the previous state on exit.

    Serialised with a lock so concurrent deployers in one process never see
    each other's value.
    """
    with _ENV_LOCK:
        previous = os.environ.get(name)
        os.environ[name] = value
        try:
            yield
        finally:
            if previous is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = previous


def _deploy_single(flow_obj: Any, meta: ETLFlowMeta, work_pool: str, env: str) -> str:
    """Register one deployment of a flow defined in a user module.

    Prefect 3 quirk: ``Flow.deploy()`` does NOT accept an ``entrypoint``
    kwarg. For flows with synthesised signatures (our case — ``flow.fn``
    points back at this module), the canonical override is
    ``flow.from_source(source=..., entrypoint=...).deploy(image=..., ...)``.
    """
    plan = _plan(meta, work_pool, env, extra_env={})
    sourced = flow_obj.from_source(
        source=plan.working_dir,
        entrypoint=_file_entrypoint(flow_obj, meta, plan.working_dir),
    )
    return _register(sourced, meta, plan)


@dataclass(frozen=True)
class _DeploymentPlan:
    """Work-pool settings of one deployment, resolved from ``meta.pool_config``."""

    work_pool: str
    working_dir: str
    image: str | None
    job_variables: dict[str, Any]


def _plan(
    meta: ETLFlowMeta, work_pool: str, env: str, *, extra_env: Mapping[str, str]
) -> _DeploymentPlan:
    """Resolve the pool, image and job variables of ``meta`` for ``env``.

    ``extra_env`` holds the keys the deployer records under
    ``job_variables.env``; a declaration that sets one of them itself is
    rejected so the recorded value is always the validated one.
    """
    pool_env_config = meta.pool_config.get(env, {})
    job_variables = dict(pool_env_config.get("job_variables") or {})
    image = job_variables.pop("image", None)
    if extra_env:
        job_variables["env"] = _merged_env(meta.name, job_variables.get("env"), extra_env)
    return _DeploymentPlan(
        work_pool=pool_env_config.get("work_pool") or work_pool,
        working_dir=job_variables.get("working_dir", "/app/src"),
        image=image,
        job_variables=job_variables,
    )


def _merged_env(etl: str, user_env: Any, extra_env: Mapping[str, str]) -> dict[str, Any]:
    env = dict(user_env or {})
    reserved = sorted(set(env) & set(extra_env))
    if reserved:
        raise ConfigError(
            f"ETL {etl!r}: job_variables.env may not set {', '.join(reserved)}; "
            "the deployer records it from the validated declaration"
        )
    return {**env, **extra_env}


def _register(sourced: Any, meta: ETLFlowMeta, plan: _DeploymentPlan) -> str:
    kwargs: dict[str, Any] = {
        "name": meta.name,
        "work_pool_name": plan.work_pool,
        "build": False,
        "push": False,
        "tags": [meta.name, *meta.tags],
        "parameters": dict(meta.raw_params),
        "job_variables": plan.job_variables,
        "enforce_parameter_schema": False,
    }
    if plan.image is not None:
        kwargs["image"] = plan.image
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
