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

import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import msgspec
from celery import Celery  # type: ignore[import-untyped]

from loom.celery.config import CeleryConfig, JobConfig, apply_job_config, create_celery_app
from loom.celery.event_loop import WorkerEventLoop
from loom.celery.runner import _make_callback_error_task, _make_callback_task, _make_job_task
from loom.core.config.errors import ConfigError
from loom.core.config.loader import load_config, section
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.logger import (
    HandlerConfig,
    configure_logging_from_values,
)
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
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


def _configure_logger(logger_cfg: _LoggerConfig) -> None:
    configure_logging_from_values(
        name=logger_cfg.name,
        environment=logger_cfg.environment or os.getenv("ENVIRONMENT", "dev"),
        renderer=logger_cfg.renderer,
        colors=logger_cfg.colors,
        level=logger_cfg.level,
        handlers=logger_cfg.handlers,
    )


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
    signals for per-process event loop management.

    This function is **independent** of
    :func:`~loom.core.bootstrap.bootstrap.bootstrap_app` — it targets the
    worker process only and does not perform REST discovery or model
    compilation.

    Fork-safety note: the :class:`~SessionManager` is created here
    (pre-fork) using a lazy async engine.  No TCP connections are
    established until the background asyncio loop is started in each
    child process via ``worker_process_init``.

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

    # SessionManager is created pre-fork. The async engine is lazy — no TCP
    # connections are opened until the first async context manager usage in
    # the child process after WorkerEventLoop.initialize() runs.
    session_manager = SessionManager(
        db_cfg.url, echo=db_cfg.echo, pool_pre_ping=db_cfg.pool_pre_ping
    )
    uow_factory = SQLAlchemyUnitOfWorkFactory(session_manager)

    celery_app = create_celery_app(celery_cfg)
    executor = RuntimeExecutor(compiler, uow_factory=uow_factory, metrics=metrics)

    for job_type in jobs:
        _make_job_task(celery_app, job_type, factory, executor, metrics)

    for callback_type in callbacks:
        _make_callback_task(celery_app, callback_type, factory)
        _make_callback_error_task(celery_app, callback_type, factory)

    _connect_worker_signals(session_manager)

    container.validate()

    return WorkerBootstrapResult(
        container=container,
        factory=factory,
        celery_app=celery_app,
    )
