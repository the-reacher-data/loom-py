"""Automatic FastAPI app creation from YAML configuration."""

from __future__ import annotations

import os
import sys
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

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
from loom.core.logger import (
    Environment,
    HandlerConfig,
    LogConfig,
    Renderer,
    configure_logging,
)
from loom.core.model import BaseModel
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.prometheus.middleware import PrometheusMiddleware
from loom.rest.fastapi.app import create_fastapi_app


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
    echo: bool = False
    pool_pre_ping: bool = True


class _MetricsConfig(msgspec.Struct, kw_only=True):
    enabled: bool = False
    path: str = "/metrics"


class _LoggerConfig(msgspec.Struct, kw_only=True):
    name: str = ""
    environment: str = ""
    renderer: str | None = None
    colors: bool | None = None
    level: str = "INFO"
    handlers: list[HandlerConfig] = msgspec.field(default_factory=list)


def _discover_interfaces(discovery_cfg: _DiscoveryConfig) -> DiscoveryResult:
    return InterfacesDiscoveryEngine(
        discovery_cfg.interfaces.modules,
        warn_recommended=discovery_cfg.interfaces.warn_recommended,
    ).discover()


def _discover_modules(discovery_cfg: _DiscoveryConfig) -> DiscoveryResult:
    return ModulesDiscoveryEngine(discovery_cfg.modules.include).discover()


def _discover_manifest(discovery_cfg: _DiscoveryConfig) -> DiscoveryResult:
    return ManifestDiscoveryEngine(discovery_cfg.manifest.module).discover()


_DISCOVERY_ENGINES: dict[str, Callable[[_DiscoveryConfig], DiscoveryResult]] = {
    "interfaces": _discover_interfaces,
    "modules": _discover_modules,
    "manifest": _discover_manifest,
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
            sentinel: type[Any] = type(f"_Repo_{model.__name__}", (), {})
            container.register(sentinel, _provider_for(repository), scope=Scope.APPLICATION)
            container.register_repo(model, sentinel)

    return register


def _build_discovery_result(discovery_cfg: _DiscoveryConfig) -> DiscoveryResult:
    engine = _DISCOVERY_ENGINES.get(discovery_cfg.mode)
    if engine is None:
        raise ValueError(f"Unsupported discovery mode: {discovery_cfg.mode!r}")
    return engine(discovery_cfg)


def _parse_renderer(value: str | None) -> Renderer | None:
    return Renderer.from_str(value) if value is not None else None


def _configure_logger(logger_cfg: _LoggerConfig) -> None:
    env_str = logger_cfg.environment.strip() or os.getenv("ENVIRONMENT", "dev")
    configure_logging(
        LogConfig(
            name=logger_cfg.name,
            environment=Environment.from_str(env_str),
            renderer=_parse_renderer(logger_cfg.renderer),
            colors=logger_cfg.colors,
            level=logger_cfg.level,
            handlers=tuple(logger_cfg.handlers),
        )
    )


def _build_bootstrap(
    app_cfg: _AppConfig,
    db_cfg: _DatabaseConfig,
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
        echo=db_cfg.echo,
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


def create_app(config_path: str, code_path: str | None = None) -> FastAPI:
    """Create a FastAPI application from YAML config and discovered components."""
    raw = load_config(config_path)
    app_cfg = section(raw, "app", _AppConfig)
    db_cfg = section(raw, "database", _DatabaseConfig)
    metrics_cfg = section(raw, "metrics", _MetricsConfig)
    try:
        logger_cfg = section(raw, "logger", _LoggerConfig)
    except ConfigError:
        logger_cfg = _LoggerConfig()
    _configure_logger(logger_cfg)

    config_file = Path(config_path).resolve()
    effective_code_path = Path(code_path) if code_path is not None else Path(app_cfg.code_path)
    if not effective_code_path.is_absolute():
        effective_code_path = (config_file.parent / effective_code_path).resolve()
    _ensure_code_path(effective_code_path)

    result, session_manager, discovered = _build_bootstrap(app_cfg, db_cfg)

    middleware: list[type[Any]] = []
    if metrics_cfg.enabled:
        middleware.append(PrometheusMiddleware)

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
        middleware=middleware,
        title=app_cfg.rest.title,
        version=app_cfg.rest.version,
        docs_url=app_cfg.rest.docs_url,
        redoc_url=app_cfg.rest.redoc_url,
        lifespan=lifespan,
    )

    if metrics_cfg.enabled:
        import prometheus_client

        app.mount(metrics_cfg.path, prometheus_client.make_asgi_app())

    return app
