"""Automatic FastAPI app creation from YAML configuration."""

from __future__ import annotations

import sys
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, TypeVar

if TYPE_CHECKING:
    from omegaconf import DictConfig
    from prometheus_client import CollectorRegistry

import msgspec
from fastapi import FastAPI

from loom.core.backend.sqlalchemy import compile_all, get_metadata, reset_registry
from loom.core.bootstrap.bootstrap import BootstrapResult, bootstrap_app
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
from loom.core.logger import LoggerConfig, configure_logging_from_values
from loom.core.model import BaseModel
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.prometheus.middleware import PrometheusMiddleware
from loom.rest.fastapi.app import create_fastapi_app
from loom.rest.middleware import TraceIdMiddleware

ModelT = TypeVar("ModelT", bound=BaseModel)
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


@dataclass(frozen=True)
class _RepoToken(Generic[ModelT]):
    """Typed DI token used to bind repository instances by model."""

    model: type[ModelT]


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
    repositories: dict[type[BaseModel], RepositorySQLAlchemy[Any, int]] = {
        model: RepositorySQLAlchemy(session_manager=session_manager, model=model)
        for model in models
    }

    def _provider_for(
        repo: RepositorySQLAlchemy[Any, int],
    ) -> Callable[[], RepositorySQLAlchemy[Any, int]]:
        def _provider() -> RepositorySQLAlchemy[Any, int]:
            return repo

        return _provider

    def register(container: LoomContainer) -> None:
        for model, repository in repositories.items():
            token = _RepoToken(model)
            container.register(token, _provider_for(repository), scope=Scope.APPLICATION)
            container.register_repo(model, token)

    return register


def _build_discovery_result(discovery_cfg: _DiscoveryConfig) -> DiscoveryResult:
    engine = _DISCOVERY_ENGINES.get(discovery_cfg.mode)
    if engine is None:
        raise ValueError(f"Unsupported discovery mode: {discovery_cfg.mode!r}")
    return engine(discovery_cfg)


def _configure_job_service(
    raw: DictConfig,
    result: BootstrapResult,
    session_manager: SessionManager,
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
        result: Bootstrap result carrying the container, compiler, and
            factory.
        session_manager: Live ``SessionManager`` for the inline fallback
            path (used to build a ``UoW`` factory for the executor).
    """
    try:
        from loom.celery.config import (
            CeleryConfig as _CC,  # type: ignore[import-untyped,unused-ignore]
        )
        from loom.celery.config import (
            create_celery_app,  # type: ignore[import-untyped,unused-ignore]
        )
        from loom.celery.service import (
            CeleryJobService,  # type: ignore[import-untyped,unused-ignore]
        )

        celery_cfg = section(raw, "celery", _CC)
        celery_app = create_celery_app(celery_cfg)
        svc: Any = CeleryJobService(celery_app, metrics=result.metrics)
    except (ConfigError, ImportError):
        from loom.core.engine.executor import RuntimeExecutor as _Exec
        from loom.core.job.service import InlineJobService
        from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory

        uow_factory = SQLAlchemyUnitOfWorkFactory(session_manager)
        executor = _Exec(result.compiler, uow_factory=uow_factory, metrics=result.metrics)
        svc = InlineJobService(result.factory, executor)

    from loom.core.job.service import JobService

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
) -> tuple[BootstrapResult, SessionManager, DiscoveryResult]:
    discovered = _build_discovery_result(app_cfg.discovery)
    if not discovered.use_cases:
        raise RuntimeError("No UseCase classes discovered.")
    if not discovered.interfaces:
        raise RuntimeError("No RestInterface classes discovered.")
    if not discovered.models:
        raise RuntimeError("No BaseModel classes discovered.")

    reset_registry()
    compile_all(*discovered.models)

    session_manager = SessionManager(
        db_cfg.url,
        echo=echo,
        pool_pre_ping=db_cfg.pool_pre_ping,
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        connect_args={},
    )

    result = bootstrap_app(
        config=app_cfg,
        use_cases=discovered.use_cases,
        modules=[_register_repositories(session_manager, discovered.models)],
    )
    return result, session_manager, discovered


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
        handlers=logger_cfg.handlers,
    )

    config_file = Path(config_paths[0]).resolve()
    effective_code_path = Path(code_path) if code_path is not None else Path(app_cfg.code_path)
    if not effective_code_path.is_absolute():
        effective_code_path = (config_file.parent / effective_code_path).resolve()
    _ensure_code_path(effective_code_path)

    effective_echo = _resolve_effective_echo(db_cfg, trace_cfg)
    result, session_manager, discovered = _build_bootstrap(app_cfg, db_cfg, effective_echo)
    _configure_job_service(raw, result, session_manager)

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

    if trace_cfg.enabled:
        app.add_middleware(TraceIdMiddleware, header=trace_cfg.header)

    if metrics_cfg.enabled:
        import prometheus_client

        app.add_middleware(PrometheusMiddleware, registry=metrics_registry)
        app.mount(
            metrics_cfg.path,
            prometheus_client.make_asgi_app(
                registry=metrics_registry or prometheus_client.REGISTRY,
            ),
        )

    return app
