"""Automatic FastAPI app creation from YAML configuration."""

from __future__ import annotations

import sys
import warnings
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from omegaconf import DictConfig
    from prometheus_client import CollectorRegistry

import msgspec
import prometheus_client
from fastapi import FastAPI
from starlette.responses import Response

from loom.core.backend.sqlalchemy import compile_all, get_metadata, reset_registry
from loom.core.bootstrap import KernelRuntime, create_kernel
from loom.core.config.errors import ConfigError
from loom.core.config.loader import load_config, section
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.discovery import (
    InterfacesDiscoveryEngine,
    ManifestDiscoveryEngine,
    ModulesDiscoveryEngine,
)
from loom.core.discovery.base import DiscoveryResult
from loom.core.job.service import InlineJobService, JobService
from loom.core.logger import LoggerConfig, configure_logging_from_values
from loom.core.model import BaseModel
from loom.core.repository.sqlalchemy import build_sqlalchemy_repository_registration_module
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from loom.prometheus import PrometheusMetricsAdapter
from loom.prometheus.middleware import PrometheusMiddleware
from loom.rest.fastapi.app import create_fastapi_app
from loom.rest.middleware import TraceIdMiddleware

_CfgT = TypeVar("_CfgT")


def _section_or_default(raw: DictConfig, key: str, cls: type[_CfgT]) -> _CfgT:
    """Load *key* from config, returning ``cls()`` when the section is absent.

    Args:
        raw: Root OmegaConf DictConfig produced by :func:`load_config`.
        key: Top-level YAML key to look up.
        cls: Config struct class.  Must be instantiable with no arguments
            (all fields optional or have defaults).

    Returns:
        Parsed instance of *cls*, or ``cls()`` if *key* is not present.
    """
    try:
        return section(raw, key, cls)
    except ConfigError:
        return cls()


class _DiscoveryInterfaces(msgspec.Struct, kw_only=True):
    modules: list[str] = msgspec.field(default_factory=list)
    warn_recommended: bool = True


class _DiscoveryModules(msgspec.Struct, kw_only=True):
    include: list[str] = msgspec.field(default_factory=list)


class _DiscoveryManifest(msgspec.Struct, kw_only=True):
    module: str = ""


class _DiscoveryConfig(msgspec.Struct, kw_only=True):
    mode: str = "interfaces"
    interfaces: _DiscoveryInterfaces = msgspec.field(default_factory=_DiscoveryInterfaces)
    modules: _DiscoveryModules = msgspec.field(default_factory=_DiscoveryModules)
    manifest: _DiscoveryManifest = msgspec.field(default_factory=_DiscoveryManifest)


class _RestConfig(msgspec.Struct, kw_only=True):
    backend: str = "fastapi"
    title: str = "Loom API"
    version: str = "0.1.0"
    docs_url: str | None = "/docs"
    redoc_url: str | None = "/redoc"


class _AppConfig(msgspec.Struct, kw_only=True):
    name: str
    code_path: str = "src"
    discovery: _DiscoveryConfig = msgspec.field(default_factory=_DiscoveryConfig)
    rest: _RestConfig = msgspec.field(default_factory=_RestConfig)


class _DatabaseConfig(msgspec.Struct, kw_only=True):
    url: str
    echo: bool | None = None  # None = inherit from trace.enabled
    pool_pre_ping: bool = True


class _MetricsConfig(msgspec.Struct, kw_only=True):
    enabled: bool = False
    path: str = "/metrics"


class _TraceConfig(msgspec.Struct, kw_only=True):
    enabled: bool = True
    header: str = "x-request-id"


_DISCOVERY_ENGINES: dict[str, Callable[[_DiscoveryConfig], DiscoveryResult]] = {
    "interfaces": lambda cfg: InterfacesDiscoveryEngine(
        cfg.interfaces.modules,
        warn_recommended=cfg.interfaces.warn_recommended,
    ).discover(),
    "modules": lambda cfg: ModulesDiscoveryEngine(cfg.modules.include).discover(),
    "manifest": lambda cfg: ManifestDiscoveryEngine(cfg.manifest.module).discover(),
}


def _ensure_code_path(code_path: Path) -> None:
    path_str = str(code_path.resolve())
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


def _register_repositories(
    session_manager: SessionManager,
    models: tuple[type[BaseModel], ...],
) -> Callable[[LoomContainer], None]:
    return build_sqlalchemy_repository_registration_module(session_manager, models)


def _build_discovery_result(discovery_cfg: _DiscoveryConfig) -> DiscoveryResult:
    engine = _DISCOVERY_ENGINES.get(discovery_cfg.mode)
    if engine is None:
        raise ValueError(f"Unsupported discovery mode: {discovery_cfg.mode!r}")
    return engine(discovery_cfg)


def _build_celery_service(raw: DictConfig, result: KernelRuntime) -> Any | None:
    """Return a ``CeleryJobService`` if the celery extra is installed and configured.

    Returns ``None`` when the ``loom[celery]`` extra is absent or the
    ``celery`` config section is missing / malformed, so the caller can
    fall back to :func:`_build_inline_service`.

    Args:
        raw: Root :class:`omegaconf.DictConfig` from :func:`load_config`.
        result: Kernel runtime carrying container, factory and executor.

    Returns:
        ``CeleryJobService`` instance, or ``None``.
    """
    try:
        from loom.celery.config import (  # type: ignore[import-untyped,unused-ignore]
            CeleryConfig as _CC,
        )
        from loom.celery.config import (
            create_celery_app,
        )
        from loom.celery.service import (
            CeleryJobService,  # type: ignore[import-untyped,unused-ignore]
        )

        celery_cfg = section(raw, "celery", _CC)
        return CeleryJobService(
            create_celery_app(celery_cfg),
            metrics=result.metrics,
            factory=result.factory,
            executor=result.executor,
        )
    except ImportError:
        return None
    except ConfigError:
        warnings.warn(
            "Celery config section is missing or malformed — falling back to InlineJobService. "
            "Add a 'celery' section to your config or install loom[celery] to suppress this.",
            stacklevel=3,
        )
        return None


def _build_inline_service(result: KernelRuntime) -> InlineJobService:
    """Return an ``InlineJobService`` backed by the kernel's factory and executor.

    Args:
        result: Kernel runtime carrying factory and executor.

    Returns:
        ``InlineJobService`` instance.
    """
    return InlineJobService(result.factory, result.executor)


def _configure_job_service(
    raw: DictConfig,
    result: KernelRuntime,
) -> None:
    """Register a ``JobService`` implementation in the container.

    Registers :class:`~loom.celery.service.CeleryJobService` when a
    ``celery`` config section is present and the ``loom[celery]`` extra is
    installed.  Falls back to
    :class:`~loom.core.job.service.InlineJobService` otherwise — enabling
    local development and tests without a broker.

    The registration uses ``APPLICATION`` scope so the service is created
    once and shared across all requests.

    Args:
        raw: Root :class:`omegaconf.DictConfig` from :func:`load_config`.
        result: Kernel runtime carrying container, factory and executor.
    """
    svc = _build_celery_service(raw, result) or _build_inline_service(result)
    result.container.register(JobService, lambda: svc, scope=Scope.APPLICATION)


def _resolve_effective_echo(db_cfg: _DatabaseConfig, trace_cfg: _TraceConfig) -> bool:
    """Return the effective SQLAlchemy echo setting.

    Args:
        db_cfg: Database configuration.
        trace_cfg: Trace configuration.

    Returns:
        ``db_cfg.echo`` when explicitly set; ``trace_cfg.enabled`` otherwise.
    """
    return db_cfg.echo if db_cfg.echo is not None else trace_cfg.enabled


def _build_bootstrap(
    app_cfg: _AppConfig,
    db_cfg: _DatabaseConfig,
    echo: bool,
    metrics: Any | None = None,
) -> tuple[KernelRuntime, SessionManager, DiscoveryResult]:
    discovered = _discover_components(app_cfg)
    session_manager = _build_sqlalchemy_session_manager(db_cfg, echo)
    _compile_discovered_models(discovered)
    result = _build_kernel_runtime(app_cfg, discovered, session_manager, metrics=metrics)
    return result, session_manager, discovered


def _discover_components(app_cfg: _AppConfig) -> DiscoveryResult:
    discovered = _build_discovery_result(app_cfg.discovery)
    if not discovered.use_cases:
        raise RuntimeError("No UseCase classes discovered.")
    if not discovered.interfaces:
        raise RuntimeError("No RestInterface classes discovered.")
    if not discovered.models:
        raise RuntimeError("No BaseModel classes discovered.")
    return discovered


def _compile_discovered_models(discovered: DiscoveryResult) -> None:
    reset_registry()
    compile_all(*discovered.models)


def _build_sqlalchemy_session_manager(
    db_cfg: _DatabaseConfig,
    echo: bool,
) -> SessionManager:
    return SessionManager(
        db_cfg.url,
        echo=echo,
        pool_pre_ping=db_cfg.pool_pre_ping,
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        connect_args={},
    )


def _build_kernel_runtime(
    app_cfg: _AppConfig,
    discovered: DiscoveryResult,
    session_manager: SessionManager,
    metrics: Any | None = None,
) -> KernelRuntime:
    uow_factory = SQLAlchemyUnitOfWorkFactory(session_manager)
    return create_kernel(
        config=app_cfg,
        use_cases=discovered.use_cases,
        modules=[_register_repositories(session_manager, discovered.models)],
        uow_factory=uow_factory,
        metrics=metrics,
    )


def _build_metrics_adapter(
    cfg: _MetricsConfig,
    registry: CollectorRegistry | None,
) -> Any | None:
    """Return a ``PrometheusMetricsAdapter`` when metrics are enabled, else ``None``.

    Args:
        cfg: Metrics feature config.
        registry: Optional Prometheus registry override.

    Returns:
        ``PrometheusMetricsAdapter`` or ``None``.
    """
    if not cfg.enabled:
        return None
    return PrometheusMetricsAdapter(registry=registry)


def _mount_optional_middlewares(
    app: FastAPI,
    trace_cfg: _TraceConfig,
    metrics_cfg: _MetricsConfig,
    registry: CollectorRegistry | None,
) -> None:
    """Mount trace and metrics middlewares when their feature flags are enabled.

    Args:
        app: FastAPI application to mutate.
        trace_cfg: Trace feature config.
        metrics_cfg: Metrics feature config.
        registry: Optional Prometheus registry override.
    """
    if trace_cfg.enabled:
        app.add_middleware(TraceIdMiddleware, header=trace_cfg.header)
    if metrics_cfg.enabled:
        _mount_metrics(app, metrics_cfg, registry)


def _mount_metrics(
    app: FastAPI,
    cfg: _MetricsConfig,
    registry: CollectorRegistry | None,
) -> None:
    """Add Prometheus middleware and scrape endpoint to *app*.

    Args:
        app: FastAPI application to mutate.
        cfg: Metrics feature config.
        registry: Optional Prometheus registry override.
    """
    if "{" in cfg.path:
        raise ValueError(f"metrics.path must not contain path parameters, got: {cfg.path!r}")
    app.add_middleware(PrometheusMiddleware, registry=registry)
    scrape_registry = registry or prometheus_client.REGISTRY

    def _scrape() -> Response:
        return Response(
            content=prometheus_client.generate_latest(scrape_registry),
            media_type=prometheus_client.CONTENT_TYPE_LATEST,
        )

    def _scrape_trailing_slash() -> Response:
        # Return 404 for trailing-slash variant to avoid ambiguous scrape targets.
        return Response(status_code=404)

    app.add_api_route(cfg.path, _scrape, methods=["GET"], include_in_schema=False)
    app.add_api_route(
        f"{cfg.path}/", _scrape_trailing_slash, methods=["GET"], include_in_schema=False
    )


def create_app(
    *config_paths: str,
    code_path: str | None = None,
    metrics_registry: CollectorRegistry | None = None,
) -> FastAPI:
    """Create a FastAPI application from one or more YAML config files.

    Config files are merged left-to-right — later files override earlier ones.
    Each file may also declare a top-level ``includes`` list to pull in
    additional base files before its own values (see :func:`load_config`).

    :class:`~loom.rest.middleware.TraceIdMiddleware` is mounted
    automatically when ``trace.enabled`` is ``true`` (the default).
    SQLAlchemy SQL echo inherits ``trace.enabled`` unless ``database.echo``
    is set explicitly.  Prometheus middleware is mounted when
    ``metrics.enabled`` is ``true``.

    Args:
        *config_paths: One or more paths to YAML configuration files.
        code_path: Optional override for ``app.code_path``.  Resolved relative
            to the first config file when not absolute.
        metrics_registry: Optional Prometheus ``CollectorRegistry`` used for
            ``PrometheusMiddleware`` and the scrape endpoint.  Defaults to the
            global registry.  Pass a fresh ``CollectorRegistry()`` in tests to
            avoid ``ValueError: Duplicated timeseries`` when multiple apps with
            ``metrics.enabled: true`` are created in the same process.

    Returns:
        Configured :class:`fastapi.FastAPI` application, ready to serve.

    Example — single config::

        app = create_app("config/app.yaml")

    Example — base + environment override::

        app = create_app("config/base.yaml", "config/production.yaml")

    Example — single file using inline includes::

        # config/app.yaml
        # includes:
        #   - base.yaml
        #   - secrets.yaml
        app = create_app("config/app.yaml")
    """
    if not config_paths:
        raise ConfigError("create_app requires at least one config file path.")

    raw = load_config(*config_paths)
    app_cfg = section(raw, "app", _AppConfig)
    db_cfg = section(raw, "database", _DatabaseConfig)
    metrics_cfg = section(raw, "metrics", _MetricsConfig)
    trace_cfg = _section_or_default(raw, "trace", _TraceConfig)
    logger_cfg = _section_or_default(raw, "logger", LoggerConfig)
    configure_logging_from_values(
        name=logger_cfg.name,
        environment=logger_cfg.environment,
        renderer=logger_cfg.renderer,
        colors=logger_cfg.colors,
        level=logger_cfg.level,
        named_levels=logger_cfg.named_levels,
        handlers=logger_cfg.handlers,
    )

    config_file = Path(config_paths[0]).resolve()
    effective_code_path = Path(code_path) if code_path is not None else Path(app_cfg.code_path)
    if not effective_code_path.is_absolute():
        effective_code_path = (config_file.parent / effective_code_path).resolve()
    _ensure_code_path(effective_code_path)

    effective_echo = _resolve_effective_echo(db_cfg, trace_cfg)
    metrics_adapter = _build_metrics_adapter(metrics_cfg, metrics_registry)

    result, session_manager, discovered = _build_bootstrap(
        app_cfg,
        db_cfg,
        effective_echo,
        metrics=metrics_adapter,
    )
    _configure_job_service(raw, result)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        async with session_manager.engine.begin() as connection:
            await connection.run_sync(get_metadata().create_all)
        try:
            yield
        finally:
            await session_manager.dispose()
            reset_registry()

    app = create_fastapi_app(
        result,
        interfaces=tuple(type_i for type_i in discovered.interfaces),
        title=app_cfg.rest.title,
        version=app_cfg.rest.version,
        docs_url=app_cfg.rest.docs_url,
        redoc_url=app_cfg.rest.redoc_url,
        lifespan=lifespan,
    )
    _mount_optional_middlewares(app, trace_cfg, metrics_cfg, metrics_registry)
    return app
