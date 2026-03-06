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
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast

import msgspec
from celery import Celery  # type: ignore[import-untyped]

from loom.celery.config import CeleryConfig, JobConfig, apply_job_config, create_celery_app
from loom.celery.event_loop import WorkerEventLoop
from loom.celery.runner import _make_callback_error_task, _make_callback_task, _make_job_task
from loom.core.bootstrap import create_kernel
from loom.core.config.errors import ConfigError
from loom.core.config.loader import load_config, section
from loom.core.di.container import LoomContainer
from loom.core.discovery._utils import collect_from_modules, import_modules
from loom.core.discovery.manifest import ManifestDiscoveryEngine
from loom.core.engine.compilable import Compilable
from loom.core.logger import LoggerConfig, configure_logging_from_values
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from loom.core.uow.abc import UnitOfWorkFactory
from loom.core.use_case.factory import UseCaseFactory

if TYPE_CHECKING:
    from omegaconf import DictConfig

    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.job.job import Job


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
    from celery.signals import (  # type: ignore[import-untyped]
        worker_process_init,
        worker_process_shutdown,
    )

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
        db_cfg = section(raw, "database", _WorkerDbConfig)
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
        cfg = section(raw, f"jobs.{job_type.__name__}", JobConfig)
        apply_job_config(job_type, cfg)
    except ConfigError:
        pass  # no override for this job — use ClassVar defaults


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


def _load_jobs_from_manifest(module_path: str) -> tuple[type[Job[Any]], ...]:
    module = importlib.import_module(module_path)
    raw_jobs = cast(list[Any], getattr(module, "JOBS", []))
    return tuple(cast(type[Job[Any]], item) for item in raw_jobs)


def _discover_compilables_from_config(
    discovery_cfg: _DiscoveryConfig,
) -> tuple[tuple[type[Compilable], ...], tuple[type[Job[Any]], ...]]:
    if discovery_cfg.mode == "modules":
        modules = import_modules(discovery_cfg.modules.include)
        _, use_cases, _, discovered_jobs = collect_from_modules(modules)
        return _merge_compilables(use_cases, discovered_jobs), tuple(discovered_jobs)
    # mode == "manifest" — Literal type guarantees no other value is possible
    discovered = ManifestDiscoveryEngine(discovery_cfg.manifest.module).discover()
    jobs = _load_jobs_from_manifest(discovery_cfg.manifest.module)
    return _merge_compilables(discovered.use_cases, jobs), jobs


def _resolve_compilables_and_jobs(
    raw: DictConfig,
    explicit_jobs: Sequence[type[Job[Any]]],
) -> tuple[tuple[type[Compilable], ...], tuple[type[Job[Any]], ...]]:
    if explicit_jobs:
        jobs = tuple(explicit_jobs)
        return tuple(jobs), jobs

    try:
        app_cfg = section(raw, "app", _WorkerAppConfig)
    except ConfigError as exc:
        raise RuntimeError(
            "No jobs provided and no app.discovery configured for worker bootstrap."
        ) from exc
    compilables, jobs = _discover_compilables_from_config(app_cfg.discovery)
    if not jobs:
        raise RuntimeError(
            "No Job classes discovered. "
            "Add JOBS to manifest or include job modules in app.discovery."
        )
    return compilables, jobs


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
            callbacks=[EmailSentCallback],
        )
        result.celery_app.start()
    """
    raw = load_config(*config_paths)
    celery_cfg = section(raw, "celery", CeleryConfig)

    try:
        logger_cfg = section(raw, "logger", LoggerConfig)
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

    compilables, resolved_jobs = _resolve_compilables_and_jobs(raw, jobs)

    for job_type in resolved_jobs:
        _apply_job_config_if_present(raw, job_type)

    uow_factory, session_manager = _resolve_uow_factory(raw)

    kernel = create_kernel(
        config=raw,
        use_cases=compilables,
        modules=modules,
        metrics=metrics,
        uow_factory=uow_factory,
    )
    celery_app = create_celery_app(celery_cfg)

    for job_type in resolved_jobs:
        _make_job_task(celery_app, job_type, kernel.factory, kernel.executor, metrics)

    for callback_type in callbacks:
        _make_callback_task(celery_app, callback_type, kernel.factory)
        _make_callback_error_task(celery_app, callback_type, kernel.factory)

    if session_manager is not None:
        _connect_worker_signals(session_manager)

    return WorkerBootstrapResult(
        container=kernel.container,
        factory=kernel.factory,
        celery_app=celery_app,
    )
