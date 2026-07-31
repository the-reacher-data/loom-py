"""Automatic FastAPI app creation from YAML configuration."""

from __future__ import annotations

import sys
import warnings
from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import msgspec
import prometheus_client
from fastapi import FastAPI
from prometheus_client import CollectorRegistry
from starlette.responses import Response

from loom.core.backend.sqlalchemy import compile_all, get_metadata, reset_registry
from loom.core.bootstrap import KernelRuntime, create_kernel
from loom.core.config import ConfigContext, ConfigKey
from loom.core.config.errors import ConfigError
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.discovery import (
    InterfacesDiscoveryEngine,
    ManifestDiscoveryEngine,
    ModulesDiscoveryEngine,
)
from loom.core.discovery.base import DiscoveryResult
from loom.core.job.service import InlineJobService, JobService
from loom.core.model import BaseModel
from loom.core.observability.config import ObservabilityConfig, PrometheusObservabilityConfig
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.repository.dynamodb import (
    DynamoUnitOfWorkFactory,
    build_dynamodb_repository_registration_module,
)
from loom.core.repository.sqlalchemy import build_sqlalchemy_repository_registration_module
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from loom.core.sql import NullSqlQueryService, SqlConfig, SqlExecutor, SqlQueryService
from loom.core.uow.abc import UnitOfWorkFactory
from loom.prometheus import PrometheusMetricsAdapter
from loom.prometheus.middleware import PrometheusMiddleware
from loom.rest.auth import JwtAuthConfig, JwtAuthMiddleware
from loom.rest.fastapi.app import create_fastapi_app
from loom.rest.fastapi.sql import bind_sql_endpoints
from loom.rest.middleware import TraceIdMiddleware

if TYPE_CHECKING:
    from loom.core.sql.clickhouse import ClickHouseConnectionRegistry


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


class _RestAuthConfig(msgspec.Struct, kw_only=True):
    jwt: JwtAuthConfig | None = None


class _RestConfig(msgspec.Struct, kw_only=True):
    backend: str = "fastapi"
    title: str = "Loom API"
    version: str = "0.1.0"
    docs_url: str | None = "/docs"
    redoc_url: str | None = "/redoc"
    auth: _RestAuthConfig = msgspec.field(default_factory=_RestAuthConfig)


class _AppConfig(msgspec.Struct, kw_only=True):
    name: str
    code_path: str = "src"
    discovery: _DiscoveryConfig = msgspec.field(default_factory=_DiscoveryConfig)
    rest: _RestConfig = msgspec.field(default_factory=_RestConfig)


class _DatabaseConfig(msgspec.Struct, kw_only=True):
    url: str
    echo: bool | None = None
    pool_pre_ping: bool = True


class _DynamoDBConfig(msgspec.Struct, kw_only=True):
    region: str
    table: str
    endpoint_url: str | None = None
    max_pool_connections: int = 32


class _PersistenceConfig(msgspec.Struct, kw_only=True):
    backend: str = "sqlalchemy"
    dynamodb: _DynamoDBConfig | None = None


@dataclass(frozen=True)
class _PersistenceWiring:
    """Resolved persistence choice for the auto-bootstrap REST app.

    Groups everything ``create_app`` needs from a persistence backend so the
    orchestration stays backend-agnostic:

    Args:
        uow_factory: Unit-of-work factory bound to the kernel.
        repo_registration_module: DI module registering model repositories.
        lifespan_init: Async context manager driving backend startup/shutdown
            (schema creation, resource disposal, ...).
        requires_relational_models: Whether the backend needs at least one
            ``BaseModel`` discovered and a ``database`` config section.
    """

    uow_factory: UnitOfWorkFactory
    repo_registration_module: Callable[[LoomContainer], None]
    lifespan_init: Callable[[], AbstractAsyncContextManager[None]]
    requires_relational_models: bool


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


def _build_celery_service(
    ctx: ConfigContext,
    result: KernelRuntime,
    observability_runtime: ObservabilityRuntime | None,
) -> Any | None:
    """Return a ``CeleryJobService`` if the celery extra is installed and configured.

    Returns ``None`` when the ``loom[celery]`` extra is absent or the
    ``celery`` config section is missing / malformed, so the caller can
    fall back to :func:`_build_inline_service`.

    Args:
        ctx: Resolved configuration context.
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

        celery_cfg = ctx.section(ConfigKey.CELERY, _CC)
        return CeleryJobService(
            create_celery_app(celery_cfg),
            metrics=result.metrics,
            factory=result.factory,
            executor=result.executor,
            observability_runtime=observability_runtime,
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
    ctx: ConfigContext,
    result: KernelRuntime,
    observability_runtime: ObservabilityRuntime | None,
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
        ctx: Resolved configuration context.
        result: Kernel runtime carrying container, factory and executor.
    """
    svc = _build_celery_service(ctx, result, observability_runtime) or _build_inline_service(result)
    result.container.register(JobService, lambda: svc, scope=Scope.APPLICATION)


def _load_observability_config(ctx: ConfigContext) -> ObservabilityConfig:
    """Load top-level observability config or fall back to defaults."""
    try:
        return ctx.section(ConfigKey.OBSERVABILITY, ObservabilityConfig)
    except ConfigError:
        return ObservabilityConfig()


def _build_bootstrap(
    app_cfg: _AppConfig,
    ctx: ConfigContext,
    metrics: Any | None = None,
) -> tuple[KernelRuntime, _PersistenceWiring, DiscoveryResult]:
    discovered = _discover_components(app_cfg)
    # Fail before any backend allocates resources (e.g. a SQLAlchemy engine):
    # resolving persistence is what creates them, so this guard runs first.
    if _requires_relational_models(ctx) and not discovered.models:
        raise RuntimeError("No BaseModel classes discovered.")
    wiring = _resolve_persistence(ctx, discovered)
    _compile_discovered_models(discovered)
    result = _build_kernel_runtime(app_cfg, discovered, wiring, metrics=metrics)
    return result, wiring, discovered


def _discover_components(app_cfg: _AppConfig) -> DiscoveryResult:
    discovered = _build_discovery_result(app_cfg.discovery)
    if not discovered.use_cases:
        raise RuntimeError("No UseCase classes discovered.")
    if not discovered.interfaces:
        raise RuntimeError("No RestInterface classes discovered.")
    return discovered


def _load_persistence_config(ctx: ConfigContext) -> _PersistenceConfig:
    return ctx.section_or_default(ConfigKey.PERSISTENCE, _PersistenceConfig, _PersistenceConfig())


def _requires_relational_models(ctx: ConfigContext) -> bool:
    """Whether the configured backend needs at least one discovered ``BaseModel``.

    Read from config alone so the caller can enforce the requirement *before*
    :func:`_resolve_persistence` allocates any backend resource (e.g. an engine).
    """
    return _load_persistence_config(ctx).backend == "sqlalchemy"


def _resolve_persistence(
    ctx: ConfigContext,
    discovered: DiscoveryResult,
) -> _PersistenceWiring:
    """Select the persistence wiring for the configured backend.

    The two-branch ``if/elif`` is deliberate for exactly two backends: with so
    few, an explicit branch is clearer than the indirection of a dispatch
    table. Promote this to a ``dict`` mapping ``backend -> wiring_fn`` when a
    third backend arrives.
    """
    persistence_cfg = _load_persistence_config(ctx)
    backend = persistence_cfg.backend
    if backend == "sqlalchemy":
        return _sqlalchemy_wiring(ctx, discovered)
    elif backend == "dynamodb":
        return _dynamodb_wiring(persistence_cfg, discovered)
    raise ValueError(f"Unsupported persistence backend: {backend!r}")


def _sqlalchemy_wiring(
    ctx: ConfigContext,
    discovered: DiscoveryResult,
) -> _PersistenceWiring:
    db_cfg = ctx.section(ConfigKey.DATABASE, _DatabaseConfig)
    echo = db_cfg.echo if db_cfg.echo is not None else False
    session_manager = _build_sqlalchemy_session_manager(db_cfg, echo)

    @asynccontextmanager
    async def _lifespan() -> AsyncIterator[None]:
        async with session_manager.engine.begin() as connection:
            await connection.run_sync(get_metadata().create_all)
        try:
            yield
        finally:
            await session_manager.dispose()
            reset_registry()

    return _PersistenceWiring(
        uow_factory=SQLAlchemyUnitOfWorkFactory(session_manager),
        repo_registration_module=_register_repositories(session_manager, discovered.models),
        lifespan_init=_lifespan,
        requires_relational_models=True,
    )


def _dynamodb_wiring(
    persistence_cfg: _PersistenceConfig,
    discovered: DiscoveryResult,
) -> _PersistenceWiring:
    if persistence_cfg.dynamodb is None:
        raise ConfigError(
            "persistence.backend is 'dynamodb' but the 'persistence.dynamodb' "
            "section (region, table) is missing."
        )
    dynamo_cfg = persistence_cfg.dynamodb
    client = _build_dynamodb_client(dynamo_cfg)

    return _PersistenceWiring(
        uow_factory=DynamoUnitOfWorkFactory(),
        repo_registration_module=build_dynamodb_repository_registration_module(
            client, dynamo_cfg.table, discovered.models
        ),
        lifespan_init=_noop_lifespan,
        requires_relational_models=False,
    )


def _build_dynamodb_client(dynamo_cfg: _DynamoDBConfig) -> Any:
    """Construct a boto3 low-level ``dynamodb`` client from config.

    The low-level client (not the resource) is used because it is thread-safe:
    repository operations run under ``asyncio.to_thread`` and share one client
    across worker threads. ``max_pool_connections`` sizes the underlying
    connection pool to that concurrency.

    Credentials are never taken from config: the client is created without
    explicit keys so boto3's default credential chain applies — the task role
    on ECS, or ``endpoint_url`` plus environment credentials against a local /
    fake DynamoDB in tests.
    """
    # Local import: boto3/botocore are optional dependencies (loom[dynamodb])
    # and must not be required by apps using the default SQLAlchemy backend.
    import boto3  # type: ignore[import-untyped]
    from botocore.config import Config  # type: ignore[import-untyped]

    kwargs: dict[str, Any] = {
        "region_name": dynamo_cfg.region,
        "config": Config(max_pool_connections=dynamo_cfg.max_pool_connections),
    }
    if dynamo_cfg.endpoint_url is not None:
        kwargs["endpoint_url"] = dynamo_cfg.endpoint_url
    return boto3.client("dynamodb", **kwargs)


@asynccontextmanager
async def _noop_lifespan() -> AsyncIterator[None]:
    """No-op lifespan for backends that manage no shared startup resource."""
    yield


@dataclass(frozen=True)
class _SqlWiring:
    """Resolved SQL subsystem pieces for the auto-bootstrap REST app.

    Args:
        config: Parsed ``sql:`` section, or ``None`` when absent.
        registry: ClickHouse connection registry entered by the app lifespan,
            or ``None`` when no section is configured.
        service: Query service registered in the container — the null
            implementation when *config* is ``None`` (spec M5).
    """

    config: SqlConfig | None
    registry: ClickHouseConnectionRegistry | None
    service: SqlQueryService


class _RegistryExecutors(Mapping[str, SqlExecutor]):
    """Lazy executor view over the ClickHouse connection registry.

    Lets ``create_app`` construct the SQL service and bind endpoints without
    opening any connection: executors resolve on first access, once the app
    lifespan has entered the registry.
    """

    def __init__(self, registry: ClickHouseConnectionRegistry, names: tuple[str, ...]) -> None:
        self._registry = registry
        self._names = names

    def __getitem__(self, name: str) -> SqlExecutor:
        if name not in self._names:
            raise KeyError(name)
        return self._registry.executor(name)

    def __contains__(self, name: object) -> bool:
        # Membership from config alone: never touches the registry, so the
        # service policy checks work outside the lifespan as well.
        return name in self._names

    def __iter__(self) -> Iterator[str]:
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._names)


def _resolve_sql(ctx: ConfigContext, jwt_cfg: JwtAuthConfig | None) -> _SqlWiring:
    """Load the optional ``sql:`` section into its registry and service.

    Absent section → no registry and the null service, keeping
    ``SqlQueryService`` always resolvable with an actionable error (spec M5).
    Present section → startup auth gate (spec §4) plus a registry whose
    connections only open inside the app lifespan.
    """
    sql_cfg = ctx.section_optional(ConfigKey.SQL, SqlConfig)
    if sql_cfg is None:
        return _SqlWiring(config=None, registry=None, service=NullSqlQueryService())
    _validate_sql_endpoint_auth(sql_cfg, jwt_cfg)
    registry = _build_sql_registry(sql_cfg)
    executors = _RegistryExecutors(registry, tuple(sql_cfg.connections))
    service = SqlQueryService(executors=executors, config=sql_cfg)
    return _SqlWiring(config=sql_cfg, registry=registry, service=service)


def _build_sql_registry(sql_cfg: SqlConfig) -> ClickHouseConnectionRegistry:
    """Construct the ClickHouse registry — no connection is opened here."""
    # Local import: clickhouse-connect is an optional dependency
    # (loom-kernel[clickhouse]) and must not be required without a 'sql' section.
    from loom.core.sql.clickhouse import ClickHouseConnectionRegistry

    return ClickHouseConnectionRegistry(config=sql_cfg)


def _validate_sql_endpoint_auth(sql_cfg: SqlConfig, jwt_cfg: JwtAuthConfig | None) -> None:
    """Enforce the startup gate: ``auth: jwt`` requires ``app.rest.auth.jwt`` (§4)."""
    if jwt_cfg is not None:
        return
    for name, connection in sql_cfg.connections.items():
        endpoint = connection.sql_endpoint
        if endpoint.enabled and endpoint.auth == "jwt":
            raise ConfigError(
                f"SQL connection {name!r}: sql_endpoint.auth is 'jwt' but the "
                "'app.rest.auth.jwt' section is not configured. Add the JWT "
                "section or switch the endpoint to auth: external."
            )


def _register_sql_service(container: LoomContainer, service: SqlQueryService) -> None:
    """Register ``SqlQueryService`` (APPLICATION scope) — always present (M5)."""
    container.register(SqlQueryService, lambda: service, scope=Scope.APPLICATION)


def _warn_sql_endpoints(sql_cfg: SqlConfig) -> None:
    """Emit the spec §4 startup warnings for enabled SQL endpoints."""
    for name, connection in sql_cfg.connections.items():
        endpoint = connection.sql_endpoint
        if not endpoint.enabled:
            continue
        if not connection.readonly:
            warnings.warn(
                f"SQL connection {name!r} has an enabled endpoint with 'readonly: false' — "
                "callers can mutate data through it. Ensure this is intentional.",
                stacklevel=3,
            )
        if endpoint.auth is None:
            continue
        path = endpoint.path or f"/sql/{name}"
        warnings.warn(
            f"SQL endpoint mounted at {path} (connection={name!r}, "
            f"readonly={connection.readonly}, allowed_roles={len(connection.allowed_roles)}, "
            f"auth={endpoint.auth})",
            stacklevel=3,
        )


def _mount_jwt_middleware(app: FastAPI, jwt_cfg: JwtAuthConfig | None) -> None:
    """Mount :class:`JwtAuthMiddleware` when ``app.rest.auth.jwt`` is configured.

    Mounted before the optional middlewares so ``TraceIdMiddleware`` wraps it
    and 401 bodies carry a trace id. A missing ``pyjwt`` extra surfaces at app
    startup as an ``ImportError`` with an install hint (fail-closed).
    """
    if jwt_cfg is None:
        return
    app.add_middleware(JwtAuthMiddleware, config=jwt_cfg)


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
    wiring: _PersistenceWiring,
    metrics: Any | None = None,
) -> KernelRuntime:
    return create_kernel(
        config=app_cfg,
        use_cases=discovered.use_cases,
        modules=[wiring.repo_registration_module],
        uow_factory=wiring.uow_factory,
        metrics=metrics,
    )


def _build_metrics_adapter(
    cfg: PrometheusObservabilityConfig,
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


def _metrics_path(cfg: PrometheusObservabilityConfig) -> str:
    """Return the REST metrics path declared in the Prometheus config."""
    return cfg.config.path if cfg.config is not None else "/metrics"


def _mount_optional_middlewares(
    app: FastAPI,
    metrics_cfg: PrometheusObservabilityConfig,
    registry: CollectorRegistry | None,
) -> None:
    """Mount request tracing and metrics middlewares.

    Args:
        app: FastAPI application to mutate.
        metrics_cfg: Metrics feature config.
        registry: Optional Prometheus registry override.
    """
    app.add_middleware(TraceIdMiddleware)
    if metrics_cfg.enabled:
        _mount_metrics(app, metrics_cfg, registry)


def _mount_metrics(
    app: FastAPI,
    cfg: PrometheusObservabilityConfig,
    registry: CollectorRegistry | None,
) -> None:
    """Add Prometheus middleware and scrape endpoint to *app*.

    Args:
        app: FastAPI application to mutate.
        cfg: Metrics feature config.
        registry: Optional Prometheus registry override.
    """
    path = _metrics_path(cfg)
    if "{" in path:
        raise ValueError(f"metrics.path must not contain path parameters, got: {path!r}")
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

    app.add_api_route(path, _scrape, methods=["GET"], include_in_schema=False)
    app.add_api_route(f"{path}/", _scrape_trailing_slash, methods=["GET"], include_in_schema=False)


def create_app(
    *config_paths: str,
    code_path: str | None = None,
    metrics_registry: CollectorRegistry | None = None,
) -> FastAPI:
    """Create a FastAPI application from one or more YAML config files.

    Config files are merged left-to-right — later files override earlier ones.
    Each file may also declare a top-level ``includes`` list to pull in
    additional base files before its own values (resolved by
    :meth:`loom.core.config.ConfigContext.from_yaml`).

    ``TraceIdMiddleware`` is mounted automatically. Structured logging and
    OTEL come from the top-level ``observability:`` section. Prometheus
    middleware is mounted when ``observability.prometheus.enabled`` is
    ``true``.

    The optional ``sql:`` section wires the SQL subsystem: its connections
    open inside the app lifespan and ``SqlQueryService`` is registered in the
    container (a null implementation raising an actionable ``ConfigError``
    when the section is absent). Connections opting in with
    ``sql_endpoint.enabled`` plus an explicit ``sql_endpoint.auth`` mount a
    ``POST /sql/{name}`` endpoint; ``auth: jwt`` additionally requires the
    ``app.rest.auth.jwt`` section, which mounts ``JwtAuthMiddleware``.

    Args:
        *config_paths: One or more paths to YAML configuration files.
        code_path: Optional override for ``app.code_path``.  Resolved relative
            to the first config file when not absolute.
        metrics_registry: Optional Prometheus ``CollectorRegistry`` used for
            ``PrometheusMiddleware`` and the scrape endpoint.  Defaults to the
            global registry.  Pass a fresh ``CollectorRegistry()`` in tests to
            avoid ``ValueError: Duplicated timeseries`` when multiple apps with
            ``observability.prometheus.enabled: true`` are created in the same
            process.

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

    ctx = ConfigContext.from_yaml(*config_paths)
    app_cfg = ctx.section(ConfigKey.APP, _AppConfig)
    sql = _resolve_sql(ctx, app_cfg.rest.auth.jwt)
    observability_cfg = _load_observability_config(ctx)
    observability_runtime = ObservabilityRuntime.from_config(observability_cfg)
    metrics_cfg = observability_cfg.prometheus

    config_file = Path(config_paths[0]).resolve()
    effective_code_path = Path(code_path) if code_path is not None else Path(app_cfg.code_path)
    if not effective_code_path.is_absolute():
        effective_code_path = (config_file.parent / effective_code_path).resolve()
    _ensure_code_path(effective_code_path)

    metrics_adapter = _build_metrics_adapter(metrics_cfg, metrics_registry)

    result, wiring, discovered = _build_bootstrap(
        app_cfg,
        ctx,
        metrics=metrics_adapter,
    )
    _configure_job_service(ctx, result, observability_runtime)
    _register_sql_service(result.container, sql.service)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        async with AsyncExitStack() as stack:
            # Registry first (outermost): its clients close even when the
            # persistence lifespan fails to start or to shut down.
            if sql.registry is not None:
                await stack.enter_async_context(sql.registry)
            await stack.enter_async_context(wiring.lifespan_init())
            yield

    app = create_fastapi_app(
        result,
        interfaces=tuple(type_i for type_i in discovered.interfaces),
        observability_runtime=observability_runtime,
        title=app_cfg.rest.title,
        version=app_cfg.rest.version,
        docs_url=app_cfg.rest.docs_url,
        redoc_url=app_cfg.rest.redoc_url,
        lifespan=lifespan,
    )
    _mount_jwt_middleware(app, app_cfg.rest.auth.jwt)
    _mount_optional_middlewares(app, metrics_cfg, metrics_registry)
    if sql.config is not None:
        bind_sql_endpoints(
            app,
            service=sql.service,
            config=sql.config,
            observability_runtime=observability_runtime,
        )
        _warn_sql_endpoints(sql.config)
    return app
