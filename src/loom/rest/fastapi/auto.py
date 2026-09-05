"""Automatic FastAPI app creation from YAML configuration."""

from __future__ import annotations

import logging
import sys
import warnings
from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

import msgspec
import prometheus_client
from fastapi import FastAPI
from prometheus_client import CollectorRegistry
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import Response

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
from loom.core.discovery.base import AGENTS_ONLY_HINT, DiscoveryResult
from loom.core.identity import Identity
from loom.core.introspection import (
    INTROSPECTION_STATE_ATTR,
    AppIntrospection,
    ContributorRef,
    IntrospectionError,
    describe_app,
)
from loom.core.job.service import InlineJobService, JobService
from loom.core.model import BaseModel
from loom.core.observability.config import ObservabilityConfig, PrometheusObservabilityConfig
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.repository.dynamodb import (
    DynamoUnitOfWorkFactory,
    build_dynamodb_repository_registration_module,
)
from loom.core.sql import NullSqlQueryService, SqlConfig, SqlExecutor, SqlQueryService
from loom.core.sql.config import roles_need_identity_binding
from loom.core.uow.abc import UnitOfWorkFactory
from loom.core.use_case.invoker import AppInvoker
from loom.prometheus import PrometheusMetricsAdapter
from loom.prometheus.middleware import PrometheusMiddleware
from loom.rest._body import DEFAULT_MAX_BODY_BYTES, BodySizeLimitMiddleware
from loom.rest.auth import (
    AuthenticationMiddleware,
    Authenticator,
    JwtAuthConfig,
    JwtAuthenticator,
)
from loom.rest.auth.config import DEFAULT_EXCLUDE_PATHS
from loom.rest.cors import CorsConfig
from loom.rest.fastapi._exclusions import verify_exclusion_paths
from loom.rest.fastapi.app import create_fastapi_app
from loom.rest.fastapi.sql import (
    _connection_mechanism,
    _role_exposure_notice,
    _roles_mechanism,
    bind_sql_endpoints,
)
from loom.rest.middleware import TraceIdMiddleware

_logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    # Annotations only: the AI pillar, the ClickHouse extra and the SQLAlchemy
    # backend are imported at run time solely by the branch that needs them.
    from loom.ai.compiler import AgentPlan
    from loom.ai.config import AiConfig
    from loom.ai.runtime import AgentRuntime
    from loom.core.repository.sqlalchemy.session_manager import SessionManager
    from loom.core.sql.clickhouse import ClickHouseConnectionRegistry


# Written as data, never imported here: an application without an 'ai:'
# section must not gain a single AI import (FR-050).  The reference resolves
# inside 'describe_app', and only when a description is asked for.
_AGENTS_SECTION = "agents"
# Single source of the agents prefix for both agent surfaces. Passed
# explicitly to 'bind_agent_endpoints' and to 'bind_a2a_endpoints' so the
# FR-041b exclusion guard measures the prefix the application really mounts.
_AGENTS_PREFIX: Final[str] = "/agents"
_AGENTS_CONTRIBUTOR = "loom.ai.describe:describe_agents"


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
    exclude_paths: tuple[str, ...] = DEFAULT_EXCLUDE_PATHS


class _RestConfig(msgspec.Struct, kw_only=True):
    backend: str = "fastapi"
    title: str = "Loom API"
    version: str = "0.1.0"
    docs_url: str | None = "/docs"
    redoc_url: str | None = "/redoc"
    openapi_url: str | None = "/openapi.json"
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES
    auth: _RestAuthConfig = msgspec.field(default_factory=_RestAuthConfig)
    cors: CorsConfig | None = None


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
        uow_factory: Unit-of-work factory bound to the kernel; ``None`` for
            backend ``none`` (no persistence), which the kernel executor
            accepts by running use cases without a unit of work.
        repo_registration_module: DI module registering model repositories.
        lifespan_init: Async context manager driving backend startup/shutdown
            (schema creation, resource disposal, ...).
    """

    uow_factory: UnitOfWorkFactory | None
    repo_registration_module: Callable[[LoomContainer], None]
    lifespan_init: Callable[[], AbstractAsyncContextManager[None]]


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
    # Local import: SQLAlchemy is an optional dependency (loom-kernel[sqlalchemy])
    # and must not be required by an application with 'persistence.backend: none'.
    from loom.core.repository.sqlalchemy import build_sqlalchemy_repository_registration_module

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
    persistence_cfg = _load_persistence_config(ctx)
    # Fail before any backend allocates resources (e.g. a SQLAlchemy engine):
    # resolving persistence is what creates them, so this guard runs first.
    if _requires_relational_models(persistence_cfg):
        _reject_autocrud_without_model(discovered)
        if not discovered.models:
            _logger.warning(
                "no BaseModel classes discovered: the application starts with an empty "
                "relational schema. Declare your first model, or set "
                "persistence.backend: none if it never persists."
            )
    wiring = _resolve_persistence(ctx, persistence_cfg, discovered)
    if _compiles_discovered_models(persistence_cfg):
        _compile_discovered_models(discovered)
    result = _build_kernel_runtime(app_cfg, discovered, wiring, metrics=metrics)
    return result, wiring, discovered


def _reject_autocrud_without_model(discovered: DiscoveryResult) -> None:
    """Reject an auto-CRUD interface whose model discovery never found.

    Auto-CRUD derives its operations from the model's repository, so an
    interface parameterised with an undiscovered model would mount routes that
    fail on their first request.

    Raises:
        RuntimeError: When such an interface exists, naming both classes.
    """
    from loom.rest.model import _extract_model_type

    known = set(discovered.models)
    for interface in discovered.interfaces:
        if not getattr(interface, "auto", False):
            continue
        model = _extract_model_type(interface)
        if model is not None and model not in known:
            raise RuntimeError(
                f"{interface.__name__} declares auto = True over "
                f"{model.__name__}, which discovery did not find. Add its module to "
                "app.discovery so its repository is registered."
            )


def _discover_components(app_cfg: _AppConfig) -> DiscoveryResult:
    """Run discovery and reject a result with nothing to serve.

    Use cases, REST interfaces and manifest agents are each optional on their
    own: an application whose only content is agents has neither use cases nor
    interfaces. Only a result with none of the three is an error.
    """
    discovered = _build_discovery_result(app_cfg.discovery)
    if not discovered.use_cases and not discovered.interfaces and not discovered.agent_specs:
        raise RuntimeError(
            f"Nothing discovered: no UseCase, no RestInterface and no agents. {AGENTS_ONLY_HINT}"
        )
    return discovered


def _load_persistence_config(ctx: ConfigContext) -> _PersistenceConfig:
    return ctx.section_or_default(ConfigKey.PERSISTENCE, _PersistenceConfig, _PersistenceConfig())


def _requires_relational_models(persistence_cfg: _PersistenceConfig) -> bool:
    """Whether the configured backend needs at least one discovered ``BaseModel``.

    Read from config alone so the caller can enforce the requirement *before*
    :func:`_resolve_persistence` allocates any backend resource (e.g. an engine).
    """
    return persistence_cfg.backend == "sqlalchemy"


def _compiles_discovered_models(persistence_cfg: _PersistenceConfig) -> bool:
    """Whether the configured backend compiles discovered ``BaseModel`` classes.

    Backend ``none`` accepts discovered models but neither compiles them nor
    registers repositories for them.
    """
    return persistence_cfg.backend != "none"


_PERSISTENCE_WIRINGS: dict[
    str, Callable[[ConfigContext, _PersistenceConfig, DiscoveryResult], _PersistenceWiring]
] = {
    "sqlalchemy": lambda ctx, _cfg, discovered: _sqlalchemy_wiring(ctx, discovered),
    "dynamodb": lambda _ctx, cfg, discovered: _dynamodb_wiring(cfg, discovered),
    "none": lambda _ctx, _cfg, _discovered: _none_wiring(),
}


def _resolve_persistence(
    ctx: ConfigContext,
    persistence_cfg: _PersistenceConfig,
    discovered: DiscoveryResult,
) -> _PersistenceWiring:
    """Select the persistence wiring for the configured backend.

    Dispatches through :data:`_PERSISTENCE_WIRINGS`; an unknown backend name
    raises ``ValueError``.
    """
    backend = persistence_cfg.backend
    wiring_fn = _PERSISTENCE_WIRINGS.get(backend)
    if wiring_fn is None:
        raise ValueError(f"Unsupported persistence backend: {backend!r}")
    return wiring_fn(ctx, persistence_cfg, discovered)


def _sqlalchemy_wiring(
    ctx: ConfigContext,
    discovered: DiscoveryResult,
) -> _PersistenceWiring:
    # Local imports: SQLAlchemy is an optional dependency (loom-kernel[sqlalchemy])
    # and must not be required by an application with 'persistence.backend: none'.
    from loom.core.backend.sqlalchemy import get_metadata, reset_registry
    from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory

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
    )


def _none_wiring() -> _PersistenceWiring:
    """Wiring for ``persistence.backend: none``: no persistence at all.

    No unit-of-work factory, no repositories, no startup resource; a
    ``database`` section, if present, is ignored.
    """
    return _PersistenceWiring(
        uow_factory=None,
        repo_registration_module=_register_no_repositories,
        lifespan_init=_noop_lifespan,
    )


def _register_no_repositories(container: LoomContainer) -> None:
    """No-op DI module for backend ``none``: nothing to register."""


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


@dataclass(frozen=True)
class _AgentDeps:
    """Per-invocation dependency bundle handed to an agent capability call.

    Args:
        identity: Verified caller of the invocation.
        container: Application container holding the singleton services.
        invoker: Application invoker already bound to ``identity``.
    """

    identity: Identity
    container: LoomContainer
    invoker: AppInvoker


class _AgentDepsFactory:
    """Builds the per-invocation dependencies of every agent capability call.

    Structural implementation of ``loom.ai.abc.DepsFactory``: it lives in the
    composition root, and depends only on ``loom.core``, so an application with
    no ``ai:`` section never imports the AI pillar to obtain it (FR-050).

    Binding the invoker to the caller happens here, at the one point that knows
    both: a granted use case declaring ``Caller()`` is executed as the identity
    that invoked the agent, and the capability never has to discover the caller
    from ambient state (FR-043).

    Args:
        invoker: Unbound application invoker of this application.
    """

    def __init__(self, invoker: AppInvoker) -> None:
        self._invoker = invoker

    def build(self, identity: Identity, container: LoomContainer) -> object:
        """Return the dependency bundle for one invocation.

        Args:
            identity: Verified caller of this invocation.
            container: Application container holding the singleton services.

        Returns:
            The bundle the engine passes to its capability calls.
        """
        return _AgentDeps(
            identity=identity,
            container=container,
            invoker=self._invoker.for_identity(identity),
        )


@dataclass(frozen=True)
class _AiWiring:
    """Resolved AI pillar pieces for the auto-bootstrap REST app.

    Args:
        config: Parsed ``ai:`` section, or ``None`` when absent.
        runtime: Agent runtime entered by the app lifespan alongside the SQL
            registry, or ``None`` when no section is configured.
        plans: Compiled agent plans, kept so the application can describe
            itself without reaching into the runtime.
    """

    config: AiConfig | None
    runtime: AgentRuntime | None
    plans: tuple[AgentPlan, ...] = ()


def _effective_agent_specs(
    config_specs: tuple[str, ...],
    manifest_specs: tuple[str, ...],
) -> tuple[str, ...]:
    """Return the single artifact source of the application.

    ``ai.specs`` and the manifest ``AGENTS`` attribute are mutually exclusive:
    an implicit precedence would silently ignore half of the agents an
    operator declared.

    Raises:
        AgentCompilationError: When both sources declare artifacts, or when
            neither does.
    """
    # Local import: same containment rule as '_resolve_ai'; this helper only
    # ever runs from the branch that already decided to load the AI pillar.
    from loom.ai.errors import AgentCompilationError, agent_specs_conflict, agent_specs_missing

    if config_specs and manifest_specs:
        raise AgentCompilationError([agent_specs_conflict()])
    if not config_specs and not manifest_specs:
        raise AgentCompilationError([agent_specs_missing()])
    return config_specs or manifest_specs


def _resolve_ai(
    ctx: ConfigContext,
    *,
    kernel: KernelRuntime,
    sql_cfg: SqlConfig | None,
    code_path: Path,
    manifest_agent_specs: tuple[str, ...],
) -> _AiWiring:
    """Load the optional ``ai:`` section into its compiled agent runtime.

    Absent section -> empty wiring, and the AI pillar is never imported.
    Present section -> the configured engine is resolved, every declared
    artifact is compiled offline, and the runtime is built without opening a
    single connection: its live clients open inside the app lifespan.
    """
    if not ctx.has(ConfigKey.AI):
        return _AiWiring(config=None, runtime=None)
    # Local imports: the AI pillar is optional, and importing it from an
    # application without an 'ai:' section would pull the whole agent layer
    # into every Loom app (FR-050) — the same rule '_build_sql_registry'
    # follows for the ClickHouse extra.
    from loom.ai.compiler import AgentCompiler
    from loom.ai.config import AiConfig
    from loom.ai.declarative import load_specs
    from loom.ai.registry import engine_client_factories, resolve_engine_provider
    from loom.ai.runtime import AgentRuntime

    ai_cfg = ctx.section(ConfigKey.AI, AiConfig)
    provider = resolve_engine_provider(ai_cfg.engine)
    mcp_factory, a2a_factory = engine_client_factories(provider)
    compiler = AgentCompiler(
        config=ai_cfg,
        registry=kernel.registry,
        supported_kinds=provider.supported_capability_kinds(),
        sql=sql_cfg,
    )
    specs = _effective_agent_specs(ai_cfg.specs, manifest_agent_specs)
    decoded = load_specs(specs, root=code_path)
    # DecodedSpec, not .spec: the artifact's own path is what resolves a
    # './library' skill grant, and dropping it fails them in real wiring.
    plans = compiler.compile_all(decoded)
    runtime = AgentRuntime(
        plans=plans,
        config=ai_cfg,
        engine_provider=provider,
        deps=_AgentDepsFactory(kernel.app),
        container=kernel.container,
        sql_config=sql_cfg,
        # Without these an artifact granting 'mcp' or 'a2a' compiles and then
        # fails to start: the runtime has no way to reach the server it must
        # validate the grant against.
        mcp_client_factory=mcp_factory,
        a2a_client_factory=a2a_factory,
    )
    return _AiWiring(config=ai_cfg, runtime=runtime, plans=plans)


def _bind_agent_surface(
    app: FastAPI,
    ai: _AiWiring,
    auth: _AuthWiring,
    observability_runtime: ObservabilityRuntime,
) -> None:
    """Mount the agent surfaces: HTTP for every opt-in, A2A for every exposure.

    Both mounts read the same ``_AGENTS_PREFIX``. That single source is what
    makes the FR-041b exclusion guard meaningful: it measures the agents prefix
    an exclusion could open, so a prefix nobody mounts would leave the real one
    unguarded.

    Raises:
        ConfigError: When an agent opts in without a usable authenticator, or
            when its callers are verified by a JWT without a validated ``aud``.
    """
    if ai.config is None or ai.runtime is None:
        return
    _validate_agent_endpoint_auth(ai.config, auth)
    # Local import: same containment rule as '_resolve_ai'.
    from loom.ai.fastapi.endpoints import bind_agent_endpoints

    bind_agent_endpoints(
        app,
        runtime=ai.runtime,
        config=ai.config,
        authenticator=auth.authenticator,
        observability_runtime=observability_runtime,
        prefix=_AGENTS_PREFIX,
    )
    _bind_a2a_surface(app, ai, auth, observability_runtime)


def _bind_a2a_surface(
    app: FastAPI,
    ai: _AiWiring,
    auth: _AuthWiring,
    observability_runtime: ObservabilityRuntime,
) -> None:
    """Publish the agents named in ``ai.a2a.expose`` over inbound A2A (FR-041).

    Guarded on the section rather than delegated to ``bind_a2a_endpoints``'s own
    early return: the import below pulls the ``ai-a2a`` extra, which a
    deployment without an ``ai.a2a`` section is not required to have installed.
    """
    if ai.config is None or ai.runtime is None or ai.config.a2a is None:
        return
    # Local import: the A2A transport lives behind the 'ai-a2a' extra.
    from loom.ai.a2a.server import bind_a2a_endpoints

    bind_a2a_endpoints(
        app,
        runtime=ai.runtime,
        config=ai.config,
        plans=ai.plans,
        authenticator=auth.authenticator,
        exclude_paths=auth.exclude_paths,
        observability_runtime=observability_runtime,
        agents_prefix=_AGENTS_PREFIX,
    )


def _validate_agent_endpoint_auth(ai_cfg: AiConfig, auth: _AuthWiring) -> None:
    """Apply the JWT start-up gates to every agent bound to a verified caller (§4).

    An agent surface is at least as privileged as a SQL one: the verified
    caller drives every capability the agent holds, as that caller. So it must
    not boot in a configuration the SQL surface already refuses.
    """
    if auth.jwt_config is None:
        return
    for name in _agents_bound_to_a_verified_caller(ai_cfg):
        _require_jwt_audience(f"Agent {name!r}", auth.jwt_config)


def _agents_bound_to_a_verified_caller(ai_cfg: AiConfig) -> tuple[str, ...]:
    """Name every agent whose callers are authenticated, over either surface.

    An A2A-published agent counts too, and is the stricter case of the two: the
    HTTP mount is opt-in per agent, while ``ai.a2a.expose`` puts the agent on
    the public internet, where an unvalidated ``aud`` accepts a token minted
    for another service as the caller driving every capability the agent holds.
    """
    http = {
        # The double opt-in 'bind_agent_endpoints' mounts on.
        name
        for name, endpoint in ai_cfg.endpoints.items()
        if endpoint.enabled and endpoint.auth.strip() and not endpoint.allow_anonymous
    }
    a2a = {name for name in _a2a_exposed(ai_cfg) if not _a2a_allows_anonymous(ai_cfg, name)}
    return tuple(sorted(http | a2a))


def _a2a_exposed(ai_cfg: AiConfig) -> tuple[str, ...]:
    return ai_cfg.a2a.expose if ai_cfg.a2a is not None else ()


def _a2a_allows_anonymous(ai_cfg: AiConfig, name: str) -> bool:
    """Mirror ``loom.ai.a2a._binding``'s rule for an unverified A2A caller.

    Anonymity is only in force when the HTTP stanza's own double opt-in holds:
    a disabled or unnamed-``auth`` stanza grants nothing, so its
    ``allow_anonymous`` does not travel to the A2A surface either.
    """
    endpoint = ai_cfg.endpoints.get(name)
    if endpoint is None or not endpoint.enabled or not endpoint.auth.strip():
        return False
    return endpoint.allow_anonymous


@dataclass(frozen=True)
class _AuthWiring:
    """Resolved authentication for the auto-bootstrap REST app.

    Args:
        authenticator: Mechanism authenticating callers, or ``None`` when the
            application configures none.
        exclude_paths: Paths served without authentication.
        jwt_config: JWT settings when the built-in mechanism is the one in use.
            Kept so the JWT-specific startup gates stay where the JWT contract
            is known, instead of leaking into the agnostic layers.
    """

    authenticator: Authenticator | None
    exclude_paths: tuple[str, ...]
    jwt_config: JwtAuthConfig | None


def _resolve_authentication(
    app_cfg: _AppConfig,
    authenticator: Authenticator | None,
    documentation_paths: tuple[str, ...],
) -> _AuthWiring:
    """Pick the single authentication mechanism of the application.

    Args:
        app_cfg: Parsed ``app`` section.
        authenticator: Mechanism supplied by the composition root, if any.
        documentation_paths: Effective docs/schema/metrics paths, used as the
            default exclusion list.

    Raises:
        ConfigError: When both a custom authenticator and the built-in JWT
            section are supplied — two mechanisms would mean two sources of
            truth for the caller identity.
    """
    auth_cfg = app_cfg.rest.auth
    jwt_cfg = auth_cfg.jwt
    if authenticator is not None and jwt_cfg is not None:
        raise ConfigError(
            "create_app received an 'authenticator' but 'app.rest.auth.jwt' is also "
            "configured. Exactly one authentication mechanism may be active: drop the "
            "JWT section, or drop the authenticator argument."
        )
    if authenticator is not None:
        return _AuthWiring(
            authenticator=authenticator,
            exclude_paths=_effective_exclusions(auth_cfg.exclude_paths, documentation_paths),
            jwt_config=None,
        )
    if jwt_cfg is not None:
        return _AuthWiring(
            authenticator=JwtAuthenticator(jwt_cfg),
            exclude_paths=_effective_exclusions(jwt_cfg.exclude_paths, documentation_paths),
            jwt_config=jwt_cfg,
        )
    return _AuthWiring(authenticator=None, exclude_paths=(), jwt_config=None)


def _effective_exclusions(
    configured: tuple[str, ...],
    documentation_paths: tuple[str, ...],
) -> tuple[str, ...]:
    """Resolve which paths are served without authentication.

    Left at its default, the exclusion list follows the paths the application
    actually publishes: a hardcoded ``/docs`` excludes nothing when the operator
    moved Swagger elsewhere, while still opening a hole if a route captures it.
    An explicit list is honoured as written.
    """
    if configured != DEFAULT_EXCLUDE_PATHS:
        return tuple(dict.fromkeys(configured))
    return documentation_paths


def _documentation_paths(
    rest_cfg: _RestConfig,
    metrics_cfg: PrometheusObservabilityConfig,
) -> tuple[str, ...]:
    """Return the effective docs, schema and metrics paths of the application."""
    candidates = [rest_cfg.docs_url, rest_cfg.redoc_url, rest_cfg.openapi_url]
    if metrics_cfg.enabled:
        candidates.append(_metrics_path(metrics_cfg))
    return tuple(dict.fromkeys(path for path in candidates if path))


def _resolve_sql(ctx: ConfigContext, auth: _AuthWiring) -> _SqlWiring:
    """Load the optional ``sql:`` section into its registry and service.

    Absent section → no registry and the null service, keeping
    ``SqlQueryService`` always resolvable with an actionable error (spec M5).
    Present section → startup auth gate (spec §4) plus a registry whose
    connections only open inside the app lifespan.
    """
    sql_cfg = ctx.section_optional(ConfigKey.SQL, SqlConfig)
    if sql_cfg is None:
        return _SqlWiring(config=None, registry=None, service=NullSqlQueryService())
    _validate_sql_endpoint_auth(sql_cfg, auth)
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


def _validate_sql_endpoint_auth(sql_cfg: SqlConfig, auth: _AuthWiring) -> None:
    """Enforce the startup gates of every mounted identity-bound endpoint (§4)."""
    for name, connection in sql_cfg.connections.items():
        endpoint = connection.sql_endpoint
        if not endpoint.enabled or not endpoint.binds_identity:
            continue
        authenticator = _require_authenticator(name, auth)
        if auth.jwt_config is not None:
            _require_jwt_audience(f"SQL connection {name!r}", auth.jwt_config)
        _require_role_binding(name, connection.allowed_roles, authenticator)
        _require_authenticated_path(name, endpoint.path or f"/sql/{name}", auth.exclude_paths)


def _require_authenticator(name: str, auth: _AuthWiring) -> Authenticator:
    """An identity-bound endpoint without a mechanism has no identity to bind (§4)."""
    if auth.authenticator is not None:
        return auth.authenticator
    raise ConfigError(
        f"SQL connection {name!r}: sql_endpoint.auth requires a verified caller but the "
        "application configures no authentication. Add the 'app.rest.auth.jwt' section, "
        "pass create_app(authenticator=...), or switch the endpoint to auth: external."
    )


def _require_jwt_audience(subject: str, jwt_cfg: JwtAuthConfig) -> None:
    """Binding an endpoint to a token is void without a validated ``aud`` (§4).

    Args:
        subject: What is being gated, already formatted for the message — for
            example ``"SQL connection 'analytics'"`` or ``"Agent 'analyst'"``.
        jwt_cfg: JWT settings of the application.
    """
    if jwt_cfg.audience is not None:
        return
    raise ConfigError(
        f"{subject}: the endpoint requires a verified caller but "
        "'app.rest.auth.jwt.audience' is not set. Without a validated 'aud' any "
        "token signed by the same key — including tokens minted for another "
        "service — would be accepted as that verified caller, with whatever "
        "claims it carries."
    )


def _require_role_binding(
    name: str,
    allowed_roles: tuple[str, ...],
    authenticator: Authenticator,
) -> None:
    """A mounted multi-role endpoint must bind its roles to the identity (§4)."""
    if not roles_need_identity_binding(
        allowed_roles, mechanism_binds_roles=authenticator.provides_roles
    ):
        return
    raise ConfigError(
        f"SQL connection {name!r}: 'allowed_roles' is not empty but the "
        f"{authenticator.name!r} authentication mechanism binds no role to the caller "
        "identity. Without that binding the endpoint would let any authenticated caller "
        "pick any allowlisted role. Either configure the mechanism to provide roles "
        "(for JWT: 'app.rest.auth.jwt.roles_claim'), or leave 'allowed_roles' empty and "
        "pin a single 'default_role'."
    )


def _require_authenticated_path(name: str, path: str, exclude_paths: tuple[str, ...]) -> None:
    """A mounted SQL path listed in the exclusions would bypass auth (§4)."""
    if path not in exclude_paths:
        return
    raise ConfigError(
        f"SQL connection {name!r}: the endpoint path {path!r} is listed in the "
        "authentication 'exclude_paths', which would serve SQL without "
        "authentication. Remove it from the exclusion list."
    )


def _register_sql_service(container: LoomContainer, service: SqlQueryService) -> None:
    """Register ``SqlQueryService`` (APPLICATION scope) — always present (M5)."""
    container.register(SqlQueryService, lambda: service, scope=Scope.APPLICATION)


def _warn_sql_endpoints(sql_cfg: SqlConfig, auth: _AuthWiring) -> None:
    """Emit the spec §4 startup warnings for enabled SQL endpoints."""
    mechanism = _roles_mechanism(auth.authenticator)
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
        # Narrowed per connection: a global mechanism does not bind the roles of
        # a connection whose allowlist is empty (see _connection_mechanism).
        bound = _connection_mechanism(connection, mechanism)
        warnings.warn(
            f"SQL endpoint mounted at {path} (connection={name!r}, "
            f"readonly={connection.readonly}, auth={endpoint.auth}, "
            f"allowed_roles={len(connection.allowed_roles)}). "
            "'auth' only authenticates the caller; the roles it may use come from the "
            "identity binding: "
            f"{_role_exposure_notice(bound, len(connection.allowed_roles))}"
            f"{_roles_source_detail(auth, bound)}.",
            stacklevel=3,
        )


def _roles_source_detail(auth: _AuthWiring, bound_mechanism: str | None) -> str:
    """Name the JWT claim carrying the roles, when the JWT mechanism is in use.

    The agnostic layers only know "the mechanism binds roles"; the composition
    root knows which claim, and operators need that to debug a denied caller.
    """
    jwt_cfg = auth.jwt_config
    if bound_mechanism is None or jwt_cfg is None or jwt_cfg.roles_claim is None:
        return ""
    return f" (JWT roles claim: {jwt_cfg.roles_claim!r})"


def _mount_authentication(app: FastAPI, auth: _AuthWiring) -> None:
    """Mount :class:`AuthenticationMiddleware` when a mechanism is configured.

    Mounted before the optional middlewares so ``TraceIdMiddleware`` wraps it
    and 401 bodies carry a trace id. A missing ``pyjwt`` extra surfaces at app
    startup as an ``ImportError`` with an install hint (fail-closed).
    """
    if auth.authenticator is None:
        return
    app.add_middleware(
        AuthenticationMiddleware,
        authenticator=auth.authenticator,
        exclude_paths=auth.exclude_paths,
    )


def _compile_discovered_models(discovered: DiscoveryResult) -> None:
    # Local import: SQLAlchemy is an optional dependency (loom-kernel[sqlalchemy])
    # and must not be required by an application with 'persistence.backend: none'.
    from loom.core.backend.sqlalchemy import compile_all, reset_registry

    reset_registry()
    compile_all(*discovered.models)


def _build_sqlalchemy_session_manager(
    db_cfg: _DatabaseConfig,
    echo: bool,
) -> SessionManager:
    # Local import: SQLAlchemy is an optional dependency (loom-kernel[sqlalchemy])
    # and must not be required by an application with 'persistence.backend: none'.
    from loom.core.repository.sqlalchemy.session_manager import SessionManager

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
    rest_cfg: _RestConfig,
    metrics_cfg: PrometheusObservabilityConfig,
    registry: CollectorRegistry | None,
) -> None:
    """Mount the body cap, request tracing, CORS and metrics middlewares.

    Order matters and is the reason this lives in one place.  Each call wraps
    the previous one, so the body cap ends up outside authentication (an
    oversized body is refused before any token is verified) while tracing wraps
    the cap (its ``413`` still carries a trace id).  CORS goes outermost so a
    preflight ``OPTIONS`` is answered without ever reaching the authentication
    middleware, which would refuse it for carrying no credentials.

    Args:
        app: FastAPI application to mutate.
        rest_cfg: Parsed ``app.rest`` section.
        metrics_cfg: Metrics feature config.
        registry: Optional Prometheus registry override.
    """
    app.add_middleware(BodySizeLimitMiddleware, max_bytes=rest_cfg.max_body_bytes)
    app.add_middleware(TraceIdMiddleware)
    if metrics_cfg.enabled:
        _mount_metrics(app, metrics_cfg, registry)
    _mount_cors(app, rest_cfg.cors)


def _mount_cors(app: FastAPI, cors_cfg: CorsConfig | None) -> None:
    """Mount the CORS middleware when ``app.rest.cors`` is configured.

    Absent section means no middleware at all: an application that never
    intended to be called cross-origin does not start answering preflights.
    """
    if cors_cfg is None:
        return
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(cors_cfg.allow_origins),
        allow_origin_regex=cors_cfg.allow_origin_regex,
        allow_methods=list(cors_cfg.allow_methods),
        allow_headers=list(cors_cfg.allow_headers),
        allow_credentials=cors_cfg.allow_credentials,
        expose_headers=list(cors_cfg.expose_headers),
        max_age=cors_cfg.max_age,
    )


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
    authenticator: Authenticator | None = None,
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

    Authentication is mechanism-agnostic: configure ``app.rest.auth.jwt`` for
    the built-in stateless JWT mechanism, or pass *authenticator* for any
    other.  Exactly one of the two may be active.

    The optional ``sql:`` section wires the SQL subsystem: its connections
    open inside the app lifespan and ``SqlQueryService`` is registered in the
    container (a null implementation raising an actionable ``ConfigError``
    when the section is absent). Connections opting in with
    ``sql_endpoint.enabled`` plus an explicit ``sql_endpoint.auth`` mount a
    ``POST /sql/{name}`` endpoint; ``auth: identity`` additionally requires a
    configured authentication mechanism, and a non-empty ``allowed_roles``
    also requires that mechanism to bind roles to the verified caller
    identity (for JWT, ``app.rest.auth.jwt.roles_claim``).

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
        authenticator: Custom authentication mechanism.  Mutually exclusive
            with the ``app.rest.auth.jwt`` config section.

    Returns:
        Configured :class:`fastapi.FastAPI` application, ready to serve.

    Raises:
        ConfigError: When no config path is given, or when both *authenticator*
            and ``app.rest.auth.jwt`` are supplied.

    Example — single config::

        app = create_app("config/app.yaml")

    Example — a mechanism of your own::

        app = create_app("config/app.yaml", authenticator=MyMtlsAuthenticator())

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
    observability_cfg = _load_observability_config(ctx)
    observability_runtime = ObservabilityRuntime.from_config(observability_cfg)
    metrics_cfg = observability_cfg.prometheus

    auth = _resolve_authentication(
        app_cfg,
        authenticator,
        _documentation_paths(app_cfg.rest, metrics_cfg),
    )
    sql = _resolve_sql(ctx, auth)

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
    # Registered, not merely passed: capability spans resolve it from the
    # container, and an unregistered runtime makes every Scope.TOOL span a
    # silent no-op in production while passing every test that injects one.
    result.container.register(
        ObservabilityRuntime, lambda: observability_runtime, scope=Scope.APPLICATION
    )
    _reject_manifest_agents_without_ai_section(ctx, discovered.agent_specs)
    ai = _resolve_ai(
        ctx,
        kernel=result,
        sql_cfg=sql.config,
        code_path=effective_code_path,
        manifest_agent_specs=discovered.agent_specs,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        async with AsyncExitStack() as stack:
            # Registry first (outermost): its clients close even when the
            # persistence lifespan fails to start or to shut down.
            if sql.registry is not None:
                await stack.enter_async_context(sql.registry)
            if ai.runtime is not None:
                await stack.enter_async_context(ai.runtime)
            await stack.enter_async_context(wiring.lifespan_init())
            yield

    app = create_fastapi_app(
        result,
        interfaces=tuple(discovered.interfaces),
        observability_runtime=observability_runtime,
        title=app_cfg.rest.title,
        version=app_cfg.rest.version,
        docs_url=app_cfg.rest.docs_url,
        redoc_url=app_cfg.rest.redoc_url,
        openapi_url=app_cfg.rest.openapi_url,
        lifespan=lifespan,
    )
    _mount_authentication(app, auth)
    _mount_optional_middlewares(app, app_cfg.rest, metrics_cfg, metrics_registry)
    if sql.config is not None:
        bind_sql_endpoints(
            app,
            service=sql.service,
            config=sql.config,
            authenticator=auth.authenticator,
            observability_runtime=observability_runtime,
        )
        _warn_sql_endpoints(sql.config, auth)
    _bind_agent_surface(app, ai, auth, observability_runtime)
    setattr(app.state, INTROSPECTION_STATE_ATTR, _build_introspection(app_cfg, ai))
    # Last: every route the application will ever serve is registered by now,
    # which is what makes the exclusion check meaningful.
    verify_exclusion_paths(app, auth.exclude_paths)
    _warn_anonymous_schema(app_cfg.rest, auth)
    return app


def _reject_manifest_agents_without_ai_section(
    ctx: ConfigContext,
    manifest_agent_specs: tuple[str, ...],
) -> None:
    """Refuse a manifest declaring agents in an application with no ``ai:`` section.

    Raises:
        ConfigError: When ``AGENTS`` names artifacts nothing can compile.
    """
    if manifest_agent_specs and not ctx.has(ConfigKey.AI):
        raise ConfigError(
            "The manifest declares AGENTS but the configuration has no 'ai:' section: "
            "agent artifacts need an engine and its model bindings to compile. Add the "
            "'ai:' section, or remove AGENTS from the manifest."
        )


def _build_introspection(app_cfg: _AppConfig, ai: _AiWiring) -> AppIntrospection:
    """Build the self-description of the application under construction.

    The AI contribution is a *string* reference resolved only when the
    description is asked for, so an application without an ``ai:`` section
    never imports the pillar (FR-050).
    """
    contributors: tuple[ContributorRef, ...] = ()
    if ai.config is not None:
        contributors = (
            ContributorRef(
                section=_AGENTS_SECTION,
                contributor=_AGENTS_CONTRIBUTOR,
                subject=ai.plans,
            ),
        )
    return AppIntrospection(
        name=app_cfg.name,
        version=app_cfg.rest.version,
        contributors=contributors,
    )


def describe_fastapi_app(app: FastAPI) -> dict[str, Any]:
    """Describe an application built by :func:`create_app`.

    The document always carries the application identity under ``"app"``, plus
    one section per wired pillar — ``"agents"`` when an ``ai:`` section is
    configured.  Only publishable values appear: no instructions, no model
    binding, no URL and no credential reference.

    Args:
        app: Application previously built by :func:`create_app`.

    Returns:
        JSON-encodable self-description of the application.

    Raises:
        IntrospectionError: When *app* was not built by :func:`create_app`, or
            when a pillar contribution cannot be resolved.

    Example::

        app = create_app("config/app.yaml")
        describe_fastapi_app(app)["app"]
        # {'name': 'billing', 'version': '1.4.0'}
    """
    introspection = getattr(app.state, INTROSPECTION_STATE_ATTR, None)
    if not isinstance(introspection, AppIntrospection):
        raise IntrospectionError(
            "this application carries no introspection state; only applications built by "
            "'create_app' can describe themselves."
        )
    return describe_app(introspection)


def _warn_anonymous_schema(rest_cfg: _RestConfig, auth: _AuthWiring) -> None:
    """Warn when an authenticated application publishes its schema anonymously.

    The OpenAPI document lists every route, parameter and field: it is the map
    of the attack surface. An application that bothers to authenticate and then
    serves that map without credentials is almost always an oversight.
    """
    if auth.authenticator is None:
        return
    anonymous = [
        path
        for path in (rest_cfg.openapi_url, rest_cfg.docs_url, rest_cfg.redoc_url)
        if path and path in auth.exclude_paths
    ]
    if not anonymous:
        return
    warnings.warn(
        f"Authentication is enabled but {', '.join(anonymous)} "
        f"{'are' if len(anonymous) > 1 else 'is'} served without it: the API schema "
        "describes every route, parameter and field of this service. Set "
        "'app.rest.openapi_url: null' (and the docs urls) in production, or remove "
        "them from the authentication 'exclude_paths'.",
        stacklevel=3,
    )
