"""Worker bootstrap for Celery-backed Job execution.

:func:`bootstrap_worker` is the single entry point for starting a Loom
Celery worker process.  It runs an abbreviated pipeline focused on Job
registration:

1. Load and merge YAML configuration.
2. Create :class:`~loom.core.di.container.LoomContainer` and register the
   raw config as an ``APPLICATION``-scope singleton.
3. Execute user-supplied module callables.
4. Compile all declared ``Job`` subclasses via
   :class:`~loom.core.engine.compiler.UseCaseCompiler`.
5. Register :class:`~loom.core.use_case.factory.UseCaseFactory` in the
   container.
6. Apply per-job ``JobConfig`` overrides from YAML.
7. Build the ``SessionManager`` and ``UnitOfWorkFactory`` (pre-fork; the
   engine is lazy so no TCP connections are opened yet).
8. Build the Celery application and register tasks.
9. Connect Celery worker lifecycle signals for per-process event loop
   management via :class:`~loom.celery.event_loop.WorkerEventLoop`.
10. Validate the container (fail-fast).
11. Return :class:`WorkerBootstrapResult`.

Fork-safety design
------------------
``create_async_engine`` is lazy: it creates Python objects but opens no
TCP connections until the first ``async with session`` call.  It is
therefore safe to create the :class:`~SessionManager` in the parent
process before ``fork()``.  After ``fork()``, each child inherits the
engine object but starts with an empty connection pool — connections are
only opened after :class:`~loom.celery.event_loop.WorkerEventLoop`
starts the background asyncio loop in the child process (via the
``worker_process_init`` Celery signal).

Typical YAML layout (database optional for pure in-memory jobs)::

    celery:
      broker_url: "redis://redis:6379/0"
      result_backend: "redis://redis:6379/1"

    database:
      url: "postgresql+asyncpg://user:pass@db/mydb"  # optional

    jobs:
      RecalcPricesJob:
        queue: "prices.heavy"
        retries: 5
"""

from __future__ import annotations

import importlib
import inspect
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

import msgspec
from celery import Celery  # type: ignore[import-untyped]
from celery.signals import (  # type: ignore[import-untyped]
    worker_process_init,
    worker_process_shutdown,
)

from loom.celery.config import CeleryConfig, JobConfig, apply_job_config, create_celery_app
from loom.celery.constants import WorkerManifestAttr
from loom.celery.event_loop import WorkerEventLoop
from loom.celery.runner import _make_callback_error_task, _make_callback_task, _make_job_task
from loom.core.backend.sqlalchemy import compile_all, reset_registry
from loom.core.bootstrap import create_kernel
from loom.core.config.errors import ConfigError
from loom.core.config.keys import ConfigKey
from loom.core.config.loader import load_config, section
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.discovery._utils import collect_from_modules, import_modules
from loom.core.engine.compilable import Compilable
from loom.core.job.job import Job
from loom.core.logger import LoggerConfig, configure_logging_from_values
from loom.core.model import BaseModel
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from loom.core.uow.abc import UnitOfWorkFactory
from loom.core.use_case.factory import UseCaseFactory
from loom.rest.autocrud import build_auto_routes

if TYPE_CHECKING:
    from omegaconf import DictConfig

    from loom.core.engine.metrics import MetricsAdapter


# ---------------------------------------------------------------------------
# Local config structs — private to this module
# ---------------------------------------------------------------------------


class _WorkerDbConfig(msgspec.Struct, kw_only=True):
    """Database settings for the worker process.

    Attributes:
        url: Async SQLAlchemy connection URL.
        echo: Log all SQL statements when ``True``.
        pool_pre_ping: Test connections before checkout.
    """

    url: str
    echo: bool = False
    pool_pre_ping: bool = True


class _DiscoveryModules(msgspec.Struct, kw_only=True):
    include: list[str] = msgspec.field(default_factory=list)


class _DiscoveryManifest(msgspec.Struct, kw_only=True):
    module: str = ""


class _DiscoveryConfig(msgspec.Struct, kw_only=True):
    mode: Literal["modules", "manifest"] = "modules"
    modules: _DiscoveryModules = msgspec.field(default_factory=_DiscoveryModules)
    manifest: _DiscoveryManifest = msgspec.field(default_factory=_DiscoveryManifest)


class _WorkerAppConfig(msgspec.Struct, kw_only=True):
    discovery: _DiscoveryConfig = msgspec.field(default_factory=_DiscoveryConfig)


# ---------------------------------------------------------------------------
# Worker manifest — public typed contract for manifest-mode discovery
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WorkerManifest:
    """Typed worker manifest for manifest-mode discovery.

    Define a module-level ``MANIFEST`` attribute of this type in your
    manifest module.  :func:`bootstrap_worker` reads it when
    ``app.discovery.mode == "manifest"`` is configured in YAML.

    All fields are optional — a manifest that only declares ``jobs``
    is valid for workers that do not expose any use cases to callbacks.

    Attributes:
        jobs: Job subclasses to register as Celery tasks.
        use_cases: UseCase subclasses to compile in the worker so that
            callbacks can invoke them via ``ApplicationInvoker``.
        interfaces: ``RestInterface`` subclasses whose route use-cases
            are extracted and compiled automatically.
        models: Model classes for which AutoCRUD use cases are compiled.

    Example::

        from loom.celery.bootstrap import WorkerManifest
        from app.product.jobs import SendRestockEmailJob
        from app.product.use_cases import GetProductUseCase
        from app.product.model import Product

        MANIFEST = WorkerManifest(
            jobs=[SendRestockEmailJob],
            use_cases=[GetProductUseCase],
            models=[Product],
        )
    """

    jobs: Sequence[type[Job[Any]]] = field(default_factory=list)
    use_cases: Sequence[type[Compilable]] = field(default_factory=list)
    interfaces: Sequence[type[Any]] = field(default_factory=list)
    models: Sequence[type[Any]] = field(default_factory=list)
    callbacks: Sequence[type[Any]] = field(default_factory=list)


@dataclass(frozen=True)
class _WorkerResolved:
    """Resolved worker graph after discovery/explicit merging."""

    compilables: tuple[type[Compilable], ...]
    jobs: tuple[type[Job[Any]], ...]
    models: tuple[type[BaseModel], ...]
    callbacks: tuple[type[Any], ...]


@dataclass(frozen=True)
class _RepoToken:
    model: type[BaseModel]


# ---------------------------------------------------------------------------
# Celery worker lifecycle signals
# ---------------------------------------------------------------------------


def _connect_worker_signals(session_manager: SessionManager) -> None:
    """Connect Celery worker lifecycle signals for per-process loop management.

    Registers two signal handlers:

    * ``worker_process_init`` — starts :class:`~WorkerEventLoop` in the
      forked child process.
    * ``worker_process_shutdown`` — disposes the session manager through
      the event loop, then shuts the loop down.

    Must be called once during :func:`bootstrap_worker`.

    Args:
        session_manager: The shared :class:`~SessionManager` instance
            (created pre-fork with a lazy engine).  Disposed per-process
            on shutdown.
    """

    def _on_init(**kwargs: Any) -> None:
        WorkerEventLoop.initialize()

    def _on_shutdown(**kwargs: Any) -> None:
        dispose_coro: Any | None = None
        try:
            if WorkerEventLoop.is_initialized():
                dispose_coro = session_manager.dispose()
                WorkerEventLoop.run(dispose_coro)
        except Exception:
            if dispose_coro is not None:
                close = getattr(dispose_coro, "close", None)
                if callable(close):
                    close()
            raise
        finally:
            WorkerEventLoop.shutdown()

    worker_process_init.connect(_on_init, weak=False)
    worker_process_shutdown.connect(_on_shutdown, weak=False)


def _resolve_uow_factory(raw: DictConfig) -> tuple[UnitOfWorkFactory | None, SessionManager | None]:
    """Return SQLAlchemy UoW factory when database config is present.

    This keeps worker bootstrap lightweight for pure jobs that do not use DB.
    """
    try:
        db_cfg = section(raw, ConfigKey.DATABASE, _WorkerDbConfig)
    except ConfigError:
        return None, None

    # SessionManager is created pre-fork. The async engine is lazy — no TCP
    # connections are opened until the first async context manager usage in
    # the child process after WorkerEventLoop.initialize() runs.
    session_manager = SessionManager(
        db_cfg.url, echo=db_cfg.echo, pool_pre_ping=db_cfg.pool_pre_ping
    )
    return SQLAlchemyUnitOfWorkFactory(session_manager), session_manager


# ---------------------------------------------------------------------------
# Job config override helper
# ---------------------------------------------------------------------------


def _apply_job_config_if_present(raw: DictConfig, job_type: type[Job[Any]]) -> None:
    """Apply ``JobConfig`` overrides from YAML to ``job_type``'s ClassVars.

    Silently skips when the ``jobs.<JobTypeName>`` section is absent — the
    Job's ClassVar defaults remain unchanged.

    Args:
        raw: Root :class:`omegaconf.DictConfig` returned by
            :func:`~loom.core.config.loader.load_config`.
        job_type: Concrete ``Job`` subclass to configure.
    """
    try:
        cfg = section(raw, f"{ConfigKey.JOBS}.{job_type.__name__}", JobConfig)
        apply_job_config(job_type, cfg)
    except ConfigError:
        pass  # no override for this job — use ClassVar defaults


def _use_cases_from_interfaces(interfaces: Sequence[type[Any]]) -> tuple[type[Compilable], ...]:
    """Extract UseCase classes declared on RestInterface routes.

    Each ``RestInterface.routes`` entry carries a ``use_case`` class.
    This helper flattens all routes across the given interfaces and
    returns the unique use-case types so they can be compiled in the
    worker process alongside explicit jobs.

    Args:
        interfaces: Sequence of ``RestInterface`` subclasses.

    Returns:
        Tuple of UseCase classes found across all route declarations.
    """
    return tuple(
        route.use_case
        for iface in interfaces
        for route in getattr(iface, "routes", ())
        if getattr(route, "use_case", None) is not None
    )


def _merge_compilables(
    use_cases: Sequence[type[Compilable]],
    jobs: Sequence[type[Job[Any]]],
) -> tuple[type[Compilable], ...]:
    merged: list[type[Compilable]] = []
    seen: set[type[Compilable]] = set()
    for item in (*use_cases, *jobs):
        if item in seen:
            continue
        merged.append(item)
        seen.add(item)
    return tuple(merged)


def _merge_callbacks(
    first: Sequence[type[Any]],
    second: Sequence[type[Any]],
) -> tuple[type[Any], ...]:
    """Merge callback classes preserving declaration order and uniqueness."""
    merged: list[type[Any]] = []
    seen: set[type[Any]] = set()
    for item in (*first, *second):
        if item in seen:
            continue
        merged.append(item)
        seen.add(item)
    return tuple(merged)


def _is_callback_class(value: type[Any]) -> bool:
    """Return ``True`` when *value* looks like a job callback class."""
    if value.__name__ == "NullJobCallback":
        return False
    on_success = getattr(value, "on_success", None)
    on_failure = getattr(value, "on_failure", None)
    return callable(on_success) and callable(on_failure)


def _discover_callbacks_from_modules(modules: list[Any]) -> tuple[type[Any], ...]:
    """Collect local callback classes from imported modules."""
    discovered: list[type[Any]] = []
    seen: set[type[Any]] = set()
    for module in modules:
        module_name = module.__name__
        for _, cls in inspect.getmembers(module, inspect.isclass):
            if cls.__module__ != module_name:
                continue
            if not _is_callback_class(cls):
                continue
            if cls in seen:
                continue
            discovered.append(cls)
            seen.add(cls)
    return tuple(discovered)


def _read_manifest_struct(module: Any, module_path: str) -> WorkerManifest | None:
    """Reject private typed MANIFEST for worker discovery."""
    manifest = getattr(module, WorkerManifestAttr.MANIFEST, None)
    if manifest is None:
        return None
    raise ValueError(
        f"{module_path!r}.{WorkerManifestAttr.MANIFEST} is not part of the public manifest API. "
        f"Declare components via {WorkerManifestAttr.MODELS}/{WorkerManifestAttr.USE_CASES}/"
        f"{WorkerManifestAttr.INTERFACES}/{WorkerManifestAttr.JOBS}/"
        f"{WorkerManifestAttr.CALLBACKS}."
    )


def _read_manifest_lists(module: Any, module_path: str) -> WorkerManifest:
    """Read manifest using public list attributes (MODELS/USE_CASES/JOBS/...)."""
    models = tuple(getattr(module, WorkerManifestAttr.MODELS, ()))
    use_cases = tuple(getattr(module, WorkerManifestAttr.USE_CASES, ()))
    interfaces = tuple(getattr(module, WorkerManifestAttr.INTERFACES, ()))
    jobs = tuple(getattr(module, WorkerManifestAttr.JOBS, ()))
    callbacks = tuple(getattr(module, WorkerManifestAttr.CALLBACKS, ()))

    if not (models or use_cases or interfaces or jobs or callbacks):
        expected = (
            f"{WorkerManifestAttr.MODELS}/{WorkerManifestAttr.USE_CASES}/"
            f"{WorkerManifestAttr.INTERFACES}/{WorkerManifestAttr.JOBS}/"
            f"{WorkerManifestAttr.CALLBACKS}"
        )
        raise ValueError(
            f"Manifest module {module_path!r} exposes no components. Expected {expected}."
        )

    return WorkerManifest(
        models=models,
        use_cases=use_cases,
        interfaces=interfaces,
        jobs=jobs,
        callbacks=callbacks,
    )


def _read_worker_manifest(module_path: str) -> WorkerManifest:
    """Read a :class:`WorkerManifest` from a manifest module.

    Args:
        module_path: Dotted Python import path of the manifest module.

    Returns:
        A normalized :class:`WorkerManifest`.
    """
    module = importlib.import_module(module_path)
    _read_manifest_struct(module, module_path)
    return _read_manifest_lists(module, module_path)


def _autocrud_use_cases_from_models(
    models: Sequence[type[Any]],
) -> tuple[type[Compilable], ...]:
    """Extract AutoCRUD use-case classes for the given model types.

    Calls ``build_auto_routes(model, ())`` for each model to generate
    all five standard CRUD routes and returns their use-case classes.

    Args:
        models: Model classes to extract AutoCRUD use cases from.

    Returns:
        Unique use-case types from all generated routes.
    """
    seen: set[type[Compilable]] = set()
    result: list[type[Compilable]] = []
    for model in models:
        for route in build_auto_routes(model, ()):
            uc: type[Compilable] = route.use_case
            if uc not in seen:
                result.append(uc)
                seen.add(uc)
    return tuple(result)


def _build_worker_compilables(manifest: WorkerManifest) -> tuple[type[Compilable], ...]:
    """Build the full compilables set from a :class:`WorkerManifest`.

    Merges use cases from explicit declarations, interface routes, and
    AutoCRUD model generation — then folds in the job classes.

    Args:
        manifest: Typed worker manifest with all component declarations.

    Returns:
        Deduplicated tuple of all compilable types (use cases + jobs).
    """
    use_cases: list[type[Compilable]] = list(manifest.use_cases)
    seen: set[type[Compilable]] = set(use_cases)

    for uc in _use_cases_from_interfaces(manifest.interfaces):
        if uc not in seen:
            use_cases.append(uc)
            seen.add(uc)

    for uc in _autocrud_use_cases_from_models(manifest.models):
        if uc not in seen:
            use_cases.append(uc)
            seen.add(uc)

    return _merge_compilables(use_cases, manifest.jobs)


def _resolved_from_parts(
    *,
    models: Sequence[type[Any]],
    use_cases: Sequence[type[Compilable]],
    interfaces: Sequence[type[Any]],
    jobs: Sequence[type[Job[Any]]],
    callbacks: Sequence[type[Any]],
) -> _WorkerResolved:
    """Build a normalized worker graph from discovered component collections."""
    manifest = WorkerManifest(
        models=tuple(models),
        use_cases=tuple(use_cases),
        interfaces=tuple(interfaces),
        jobs=tuple(jobs),
        callbacks=tuple(callbacks),
    )
    return _WorkerResolved(
        compilables=_build_worker_compilables(manifest),
        jobs=tuple(jobs),
        models=tuple(models),
        callbacks=tuple(callbacks),
    )


def _discover_from_modules(discovery_cfg: _DiscoveryConfig) -> _WorkerResolved:
    """Discover worker components from module include paths."""
    modules = import_modules(discovery_cfg.modules.include)
    models, use_cases, interfaces, discovered_jobs = collect_from_modules(modules)
    callbacks = _discover_callbacks_from_modules(modules)
    return _resolved_from_parts(
        models=models,
        use_cases=use_cases,
        interfaces=interfaces,
        jobs=discovered_jobs,
        callbacks=callbacks,
    )


def _discover_from_manifest(discovery_cfg: _DiscoveryConfig) -> _WorkerResolved:
    """Discover worker components from a manifest module."""
    manifest = _read_worker_manifest(discovery_cfg.manifest.module)
    return _resolved_from_parts(
        models=manifest.models,
        use_cases=manifest.use_cases,
        interfaces=manifest.interfaces,
        jobs=manifest.jobs,
        callbacks=manifest.callbacks,
    )


def _discover_compilables_from_config(
    discovery_cfg: _DiscoveryConfig,
) -> _WorkerResolved:
    if discovery_cfg.mode == "modules":
        return _discover_from_modules(discovery_cfg)
    # mode == "manifest" — Literal type guarantees no other value is possible
    return _discover_from_manifest(discovery_cfg)


def _supplement_use_cases_from_discovery(
    raw: DictConfig,
    compilables: tuple[type[Compilable], ...],
) -> tuple[type[Compilable], ...]:
    """Add use-cases discovered via ``app.discovery`` to an explicit compilables set.

    When explicit ``jobs=`` are provided to :func:`bootstrap_worker` the
    discovery path is skipped, but callbacks may still require use-cases
    registered in the ``UseCaseRegistry``.  When ``app.discovery`` is
    present in the YAML (e.g. inherited from a base config), this helper
    supplements the explicit compilables with the discovered use-cases so
    callbacks can call ``ApplicationInvoker`` without any code change.

    Silently returns *compilables* unchanged if ``app.discovery`` is absent
    or fails to load.

    Args:
        raw: Root :class:`omegaconf.DictConfig` from :func:`load_config`.
        compilables: Explicit compilables already resolved.

    Returns:
        Extended compilables tuple with discovered use-cases appended.
    """
    try:
        app_cfg = section(raw, ConfigKey.APP, _WorkerAppConfig)
        discovered = _discover_compilables_from_config(app_cfg.discovery)
    except (ConfigError, RuntimeError, ValueError, TypeError):
        return compilables
    seen = set(compilables)
    return (*compilables, *(c for c in discovered.compilables if c not in seen))


def _discover_worker_components_or_empty(raw: DictConfig) -> _WorkerResolved:
    """Best-effort discovery helper used to supplement explicit bootstrap."""
    try:
        app_cfg = section(raw, ConfigKey.APP, _WorkerAppConfig)
        return _discover_compilables_from_config(app_cfg.discovery)
    except (ConfigError, RuntimeError, ValueError, TypeError):
        return _WorkerResolved(compilables=(), jobs=(), models=(), callbacks=())


def _resolve_compilables_and_jobs(
    raw: DictConfig,
    explicit_jobs: Sequence[type[Job[Any]]],
    explicit_use_cases: Sequence[type[Compilable]],
) -> _WorkerResolved:
    """Resolve worker components from explicit args and/or discovery config."""
    if explicit_jobs or explicit_use_cases:
        jobs = tuple(explicit_jobs)
        compilables = _merge_compilables(explicit_use_cases, jobs)
        discovered = _discover_worker_components_or_empty(raw)
        compilables = _supplement_use_cases_from_discovery(raw, compilables)
        return _WorkerResolved(
            compilables=compilables,
            jobs=jobs,
            models=discovered.models,
            callbacks=discovered.callbacks,
        )

    try:
        app_cfg = section(raw, ConfigKey.APP, _WorkerAppConfig)
    except ConfigError as exc:
        raise RuntimeError(
            "No jobs provided and no app.discovery configured for worker bootstrap."
        ) from exc
    discovered = _discover_compilables_from_config(app_cfg.discovery)
    if not discovered.jobs:
        raise RuntimeError(
            "No Job classes discovered. "
            "Add JOBS to manifest or include job modules in app.discovery."
        )
    return discovered


def _register_repositories(
    session_manager: SessionManager,
    models: Sequence[type[BaseModel]],
) -> Callable[[LoomContainer], None]:
    """Build a container module that registers SQLAlchemy repositories."""
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
        container.register(SessionManager, lambda: session_manager, scope=Scope.APPLICATION)

    return register


def _compile_models(models: Sequence[type[BaseModel]]) -> tuple[type[BaseModel], ...]:
    """Compile and normalize discovered models for SQLAlchemy repositories."""
    if not models:
        return ()
    ordered = tuple(dict.fromkeys(models))
    reset_registry()
    compile_all(*ordered)
    return ordered


def _emit_worker_init_graph(components: _WorkerResolved) -> None:
    """Emit a concise worker graph summary (Celery-style init visibility)."""
    from loom.core.logger import get_logger

    logger = get_logger("loom.celery.bootstrap")
    logger.info(
        "[BOOT] Worker init graph",
        jobs=len(components.jobs),
        use_cases=len(components.compilables) - len(components.jobs),
        callbacks=len(components.callbacks),
        models=len(components.models),
    )


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WorkerBootstrapResult:
    """Result of a successful :func:`bootstrap_worker` call.

    Attributes:
        container: Validated DI container with all bindings resolved.
        factory: Factory for constructing Job instances per task execution.
        celery_app: Celery application with all Job tasks registered.

    Example::

        result = bootstrap_worker("config/worker.yaml", jobs=[SendEmailJob])
        result.celery_app.start()
    """

    container: LoomContainer
    factory: UseCaseFactory
    celery_app: Celery


# ---------------------------------------------------------------------------
# bootstrap_worker
# ---------------------------------------------------------------------------


def bootstrap_worker(
    *config_paths: str,
    jobs: Sequence[type[Job[Any]]] = (),
    use_cases: Sequence[type[Compilable]] = (),
    interfaces: Sequence[type[Any]] = (),
    callbacks: Sequence[type[Any]] = (),
    modules: Sequence[Callable[[LoomContainer], None]] = (),
    metrics: MetricsAdapter | None = None,
) -> WorkerBootstrapResult:
    """Bootstrap a Celery worker process and register all Job tasks.

    Loads configuration from ``config_paths``, compiles all ``Job``
    subclasses, registers Celery tasks, and connects worker lifecycle
    signals for per-process event loop management.

    This function is **independent** of
    :func:`~loom.core.bootstrap.bootstrap.bootstrap_app` — it targets the
    worker process only and does not perform REST discovery or model
    compilation.

    Fork-safety note: the :class:`~SessionManager` is created here
    (pre-fork) using a lazy async engine.  No TCP connections are
    established until the background asyncio loop is started in each
    child process via ``worker_process_init``.

    Database note: the ``database`` section is optional. When absent, no
    SQLAlchemy ``SessionManager``/UoW is created; this is suitable for
    pure jobs that don't use repository-backed markers or transactional
    persistence.

    Args:
        *config_paths: One or more paths to YAML configuration files.
            Files are merged left-to-right.  Later files override earlier
            ones.
        jobs: Concrete ``Job`` subclasses to compile/register.  When
            omitted, jobs are discovered from ``app.discovery`` using
            mode ``modules`` or ``manifest``.
        use_cases: Additional ``UseCase`` subclasses to compile alongside
            ``jobs``.  Required when callbacks invoke
            :class:`~loom.core.use_case.invoker.ApplicationInvoker`
            (e.g. ``app.entity(Product).get(...)``), so that the
            corresponding use-case keys are registered in the worker
            process.  Ignored when jobs are discovered via ``app.discovery``
            (discovery already includes all compiled use-cases).
        interfaces: ``RestInterface`` subclasses whose route use-cases
            should be compiled in the worker.  Equivalent to passing the
            same classes via ``use_cases=`` but without having to enumerate
            individual use-case types — useful when callbacks interact with
            AutoCRUD-generated use-cases that are not importable by name.
        callbacks: Concrete ``JobCallback`` subclasses to register as
            Celery callback (success / error) tasks.
        modules: Callables that receive the container and register
            infrastructure bindings (repositories, services, etc.).
            Executed in declaration order before compilation.
        metrics: Optional metrics adapter forwarded to the compiler and
            executor.

    Returns:
        :class:`WorkerBootstrapResult` with the container, factory, and
        configured Celery application.

    Raises:
        ConfigError: If a required configuration section is missing or
            fails validation.

    Example::

        result = bootstrap_worker(
            "config/base.yaml",
            "config/worker.yaml",
            jobs=[SendEmailJob, RecalcPricesJob],
            interfaces=[ProductInterface],
            callbacks=[EmailSentCallback],
        )
        result.celery_app.start()
    """
    raw = load_config(*config_paths)
    celery_cfg = section(raw, ConfigKey.CELERY, CeleryConfig)

    try:
        logger_cfg = section(raw, ConfigKey.LOGGER, LoggerConfig)
    except ConfigError:
        logger_cfg = LoggerConfig()
    configure_logging_from_values(
        name=logger_cfg.name,
        environment=logger_cfg.environment,
        renderer=logger_cfg.renderer,
        colors=logger_cfg.colors,
        level=logger_cfg.level,
        handlers=logger_cfg.handlers,
    )

    resolved = _resolve_compilables_and_jobs(raw, jobs, use_cases)
    interface_use_cases = _use_cases_from_interfaces(interfaces)
    if interface_use_cases:
        seen = set(resolved.compilables)
        resolved = _WorkerResolved(
            compilables=(
                *resolved.compilables,
                *(uc for uc in interface_use_cases if uc not in seen),
            ),
            jobs=resolved.jobs,
            models=resolved.models,
            callbacks=resolved.callbacks,
        )

    for job_type in resolved.jobs:
        _apply_job_config_if_present(raw, job_type)

    uow_factory, session_manager = _resolve_uow_factory(raw)
    normalized_models: tuple[type[BaseModel], ...] = ()
    runtime_modules: tuple[Callable[[LoomContainer], None], ...] = tuple(modules)
    if session_manager is not None:
        normalized_models = _compile_models(resolved.models)
        if normalized_models:
            runtime_modules = (
                *runtime_modules,
                _register_repositories(session_manager, normalized_models),
            )
            resolved = _WorkerResolved(
                compilables=resolved.compilables,
                jobs=resolved.jobs,
                models=normalized_models,
                callbacks=resolved.callbacks,
            )

    merged_callbacks = _merge_callbacks(resolved.callbacks, callbacks)
    resolved = _WorkerResolved(
        compilables=resolved.compilables,
        jobs=resolved.jobs,
        models=resolved.models,
        callbacks=merged_callbacks,
    )
    _emit_worker_init_graph(resolved)

    kernel = create_kernel(
        config=raw,
        use_cases=resolved.compilables,
        modules=runtime_modules,
        metrics=metrics,
        uow_factory=uow_factory,
    )
    celery_app = create_celery_app(celery_cfg)

    for job_type in resolved.jobs:
        _make_job_task(celery_app, job_type, kernel.factory, kernel.executor, metrics)

    for callback_type in resolved.callbacks:
        _make_callback_task(celery_app, callback_type, kernel.factory)
        _make_callback_error_task(celery_app, callback_type, kernel.factory)

    if session_manager is not None:
        _connect_worker_signals(session_manager)

    return WorkerBootstrapResult(
        container=kernel.container,
        factory=kernel.factory,
        celery_app=celery_app,
    )
