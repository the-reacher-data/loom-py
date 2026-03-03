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
7. Build the Celery application and register tasks.
8. Connect Celery worker lifecycle signals for per-process
   :class:`~SessionManager` management.
9. Validate the container (fail-fast).
10. Return :class:`WorkerBootstrapResult`.

The critical design constraint is Celery's ``prefork`` pool: after
``fork()``, the parent process's asyncio event loop and SQLAlchemy
connections are unusable in child processes.  The ``worker_process_init``
and ``worker_process_shutdown`` signals create and dispose a fresh
:class:`~SessionManager` in each forked worker process via the
module-level :data:`_worker_session` state.

Typical YAML layout::

    celery:
      broker_url: "redis://redis:6379/0"
      result_backend: "redis://redis:6379/1"

    database:
      url: "postgresql+asyncpg://user:pass@db/mydb"

    jobs:
      RecalcPricesJob:
        queue: "prices.heavy"
        retries: 5
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import msgspec
from celery import Celery  # type: ignore[import-untyped]

from loom.celery.config import CeleryConfig, JobConfig, apply_job_config, create_celery_app
from loom.celery.runner import _make_callback_error_task, _make_callback_task, _make_job_task
from loom.core.config.errors import ConfigError
from loom.core.config.loader import load_config, section
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.logger import (
    Environment,
    HandlerConfig,
    LogConfig,
    Renderer,
    configure_logging,
)
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from loom.core.uow.abc import UnitOfWork
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


class _LoggerConfig(msgspec.Struct, kw_only=True):
    """Logger settings for the worker process.

    Attributes:
        name: Logger name.
        environment: Deployment environment string (``"dev"``, ``"prod"``).
        renderer: Log renderer (``"json"`` or ``"console"``).
        colors: Enable ANSI colours in console output.
        level: Minimum log level (``"DEBUG"``, ``"INFO"``, etc.).
        handlers: Additional handler configurations.
    """

    name: str = ""
    environment: str = ""
    renderer: str | None = None
    colors: bool | None = None
    level: str = "INFO"
    handlers: list[HandlerConfig] = msgspec.field(default_factory=list)


# ---------------------------------------------------------------------------
# Per-process worker state
# ---------------------------------------------------------------------------


@dataclass
class _WorkerSession:
    """Holds the per-process session manager and UoW factory.

    One instance lives at module level.  After ``fork()``, each child
    process gets its own copy with ``session_manager = None``.  The
    ``worker_process_init`` signal handler populates it; the
    ``worker_process_shutdown`` handler disposes and clears it.
    """

    session_manager: SessionManager | None = None
    uow_factory: SQLAlchemyUnitOfWorkFactory | None = None


_worker_session = _WorkerSession()


# ---------------------------------------------------------------------------
# _DeferredUoWFactory
# ---------------------------------------------------------------------------


class _DeferredUoWFactory:
    """Proxy that delegates to the per-process UoW factory.

    Allows passing a stable factory object to :func:`_make_job_task` at
    bootstrap time — before the child process is forked and
    ``worker_process_init`` populates :data:`_worker_session`.

    The factory itself carries no state; every :meth:`create` call reads
    from :data:`_worker_session` at invocation time.

    Example::

        deferred = _DeferredUoWFactory()
        executor = RuntimeExecutor(compiler, uow_factory=deferred)
        # after fork, worker_process_init sets _worker_session.uow_factory
        uow = deferred.create()  # delegates to the real factory
    """

    def create(self) -> UnitOfWork:
        """Return a fresh UoW from the per-process factory.

        Returns:
            A new :class:`~loom.core.uow.abc.UnitOfWork` instance.

        Raises:
            RuntimeError: If the worker has not been initialised by the
                ``worker_process_init`` signal.
        """
        if _worker_session.uow_factory is None:
            raise RuntimeError("Worker not initialized. Ensure worker_process_init signal fired.")
        return _worker_session.uow_factory.create()  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Logger helpers
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Celery worker lifecycle signals
# ---------------------------------------------------------------------------


def _connect_worker_signals(db_url: str, echo: bool, pool_pre_ping: bool) -> None:
    """Connect Celery worker lifecycle signals for per-process session management.

    Registers two signal handlers:

    * ``worker_process_init`` — creates a fresh :class:`~SessionManager`
      and :class:`~SQLAlchemyUnitOfWorkFactory` in the forked child process
      and stores them in :data:`_worker_session`.
    * ``worker_process_shutdown`` — disposes the session manager and clears
      :data:`_worker_session`.

    Must be called once during :func:`bootstrap_worker`.

    Args:
        db_url: Database connection URL forwarded to ``SessionManager``.
        echo: Echo SQL statements when ``True``.
        pool_pre_ping: Test connections before checkout.
    """
    from celery.signals import (  # type: ignore[import-untyped]
        worker_process_init,
        worker_process_shutdown,
    )

    def _on_init(**kwargs: Any) -> None:
        _worker_session.session_manager = SessionManager(
            db_url, echo=echo, pool_pre_ping=pool_pre_ping
        )
        _worker_session.uow_factory = SQLAlchemyUnitOfWorkFactory(_worker_session.session_manager)

    def _on_shutdown(**kwargs: Any) -> None:
        if _worker_session.session_manager is not None:
            asyncio.run(_worker_session.session_manager.dispose())
            _worker_session.session_manager = None
            _worker_session.uow_factory = None

    worker_process_init.connect(_on_init, weak=False)
    worker_process_shutdown.connect(_on_shutdown, weak=False)


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
    jobs: Sequence[type[Job[Any]]],
    callbacks: Sequence[type[Any]] = (),
    modules: Sequence[Callable[[LoomContainer], None]] = (),
    metrics: MetricsAdapter | None = None,
) -> WorkerBootstrapResult:
    """Bootstrap a Celery worker process and register all Job tasks.

    Loads configuration from ``config_paths``, compiles all ``Job``
    subclasses, registers Celery tasks, and connects worker lifecycle
    signals for per-process session management.

    This function is **independent** of
    :func:`~loom.core.bootstrap.bootstrap.bootstrap_app` — it targets the
    worker process only and does not perform REST discovery or model
    compilation.

    Args:
        *config_paths: One or more paths to YAML configuration files.
            Files are merged left-to-right.  Later files override earlier
            ones.
        jobs: Concrete ``Job`` subclasses to compile and register as
            Celery tasks.
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
    db_cfg = section(raw, "database", _WorkerDbConfig)

    try:
        logger_cfg = section(raw, "logger", _LoggerConfig)
    except ConfigError:
        logger_cfg = _LoggerConfig()
    _configure_logger(logger_cfg)

    container = LoomContainer()
    container.register(type(raw), lambda: raw, scope=Scope.APPLICATION)

    for module in modules:
        module(container)

    compiler = UseCaseCompiler(metrics=metrics)
    for job_type in jobs:
        compiler.compile(job_type)

    factory = UseCaseFactory(container)
    for job_type in jobs:
        factory.register(job_type)  # type: ignore[arg-type]
    container.register(UseCaseFactory, lambda: factory, scope=Scope.APPLICATION)

    for job_type in jobs:
        _apply_job_config_if_present(raw, job_type)

    celery_app = create_celery_app(celery_cfg)
    deferred = _DeferredUoWFactory()
    executor = RuntimeExecutor(compiler, uow_factory=deferred, metrics=metrics)

    for job_type in jobs:
        _make_job_task(celery_app, job_type, factory, deferred, executor, metrics)

    for callback_type in callbacks:
        _make_callback_task(celery_app, callback_type, factory, deferred)
        _make_callback_error_task(celery_app, callback_type, factory)

    _connect_worker_signals(db_cfg.url, db_cfg.echo, db_cfg.pool_pre_ping)

    container.validate()

    return WorkerBootstrapResult(
        container=container,
        factory=factory,
        celery_app=celery_app,
    )
