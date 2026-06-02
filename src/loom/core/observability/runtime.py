"""ObservabilityRuntime — shared fan-out engine for all Loom modules."""

from __future__ import annotations

import logging
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from time import perf_counter
from typing import Self

from loom.core.logger.config import configure_logging_from_values
from loom.core.observability.config import ObservabilityConfig, PrometheusObservabilityConfig
from loom.core.observability.event import (
    LifecycleEvent,
    LifecycleStatus,
    Scope,
)
from loom.core.observability.observer.noop import NoopObserver
from loom.core.observability.observer.otel import (
    OtelLifecycleObserver,
    build_log_correlation_processor,
    install_otel_log_export,
)
from loom.core.observability.observer.structlog import StructlogLifecycleObserver
from loom.core.observability.protocol import LifecycleObserver
from loom.prometheus.lifecycle import PrometheusLifecycleAdapter

try:
    from prometheus_client import start_http_server as _start_http_server
except ImportError:
    _start_http_server = None  # type: ignore[assignment]


def _resolve_scrape_port(cfg: PrometheusObservabilityConfig) -> int | None:
    """Return the scrape server port for streaming processes, or None.

    Returns None when Prometheus is disabled, a Pushgateway is configured
    (batch/ETL mode), or no port is declared in the config.
    """
    if not cfg.enabled:
        return None
    if cfg.pushgateway_url is not None:
        return None
    if cfg.config is None:
        return None
    return cfg.config.port


def _resolve_scrape_addr(cfg: PrometheusObservabilityConfig) -> str:
    """Return the bind address for the standalone scrape server."""
    if cfg.config is None:
        return "127.0.0.1"
    return cfg.config.bind_address


def _configure_structlog_logging(config: ObservabilityConfig) -> None:
    """Configure structlog when the observability config owns logging setup."""
    logger_config = config.log.config
    if logger_config is None:
        return
    extra_processors: tuple[object, ...] = ()
    if config.otel.enabled and config.otel.export_logs:
        extra_processors = (build_log_correlation_processor(),)
    configure_logging_from_values(
        name=logger_config.name,
        environment=logger_config.environment,
        renderer=logger_config.renderer,
        colors=logger_config.colors,
        level=logger_config.level,
        named_levels=logger_config.named_levels,
        handlers=logger_config.handlers,
        fields=logger_config.fields,
        extra_processors=extra_processors,
    )
    if config.otel.enabled and config.otel.export_logs:
        if config.otel.config is None:
            raise ValueError(
                "observability.otel.export_logs requires observability.otel.config to be provided."
            )
        install_otel_log_export(config.otel.config)


def _build_observers(config: ObservabilityConfig) -> list[LifecycleObserver]:
    """Build the observer chain declared by the observability config."""
    observers: list[LifecycleObserver] = []
    if config.log.enabled:
        _configure_structlog_logging(config)
        observers.append(StructlogLifecycleObserver())
    if config.otel.enabled and config.otel.config is not None:
        observers.append(OtelLifecycleObserver(config=config.otel.config))
    if config.prometheus.enabled:
        observers.append(
            PrometheusLifecycleAdapter(
                pushgateway_url=config.prometheus.pushgateway_url,
            )
        )
    return observers


class ObservabilityRuntime:
    """Shared observability runtime for all Loom modules.

    Emits :class:`~loom.core.observability.event.LifecycleEvent` to every
    registered observer. Observer failures are logged and discarded — they
    never interrupt the main execution path.

    Use :meth:`from_config` to build an instance from YAML-parsed config.
    Use :meth:`noop` in tests and environments without observability.

    Args:
        observers: Sequence of lifecycle observers to fan events out to.

    Example::

        runtime = ObservabilityRuntime.from_config(config.observability)

        with runtime.span(Scope.USE_CASE, "CreateOrder", trace_id=tid):
            result = use_case.execute(command)
    """

    def __init__(
        self,
        observers: Sequence[LifecycleObserver],
        *,
        _scrape_port: int | None = None,
        _scrape_addr: str = "127.0.0.1",
    ) -> None:
        self._observers = tuple(observers)
        self._scrape_port = _scrape_port
        self._scrape_addr = _scrape_addr
        self._scrape_server_started = False
        self._log = logging.getLogger(__name__)

    @property
    def observers(self) -> tuple[LifecycleObserver, ...]:
        """Return the configured observer chain."""
        return self._observers

    def start_scrape_server(self) -> None:
        """Start a standalone Prometheus HTTP scrape server on the configured port.

        No-op when no port is configured or the server is already running.
        Intended for long-running streaming processes that have no existing
        HTTP server to mount ``/metrics`` on. Safe to call multiple times.

        Raises:
            ImportError: If ``prometheus-client`` is not installed.
            OSError: If the port is already in use by another process.
        """
        if self._scrape_port is None or self._scrape_server_started:
            return
        if _start_http_server is None:
            raise ImportError(
                "Prometheus scrape server requires 'prometheus-client'. "
                "Install it with: pip install 'loom-py[prometheus]'"
            )
        _start_http_server(self._scrape_port, addr=self._scrape_addr)
        self._scrape_server_started = True

    def emit(self, event: LifecycleEvent) -> None:
        """Emit one lifecycle event to all registered observers.

        Observer failures are caught, logged at WARNING, and discarded.

        Args:
            event: Lifecycle event to dispatch.
        """
        self._dispatch(event)

    def _dispatch(self, event: LifecycleEvent) -> None:
        """Forward one event to each observer with isolated failures."""
        for obs in self._observers:
            try:
                obs.on_event(event)
            except Exception:
                self._log.warning(
                    "observer_error",
                    extra={"observer": type(obs).__name__, "scope": event.scope},
                    exc_info=True,
                )

    @contextmanager
    def span(
        self,
        scope: Scope,
        name: str,
        *,
        trace_id: str | None = None,
        correlation_id: str | None = None,
        **meta: object,
    ) -> Generator[None, None, None]:
        """Context manager that emits ``START`` on entry and ``END`` or ``ERROR`` on exit.

        Duration is measured with ``perf_counter`` and attached to the closing event.
        If the body raises, ``ERROR`` is emitted and the exception re-raised.

        Args:
            scope: Logical unit of work — one of the values in
                :class:`~loom.core.observability.event.Scope`.
            name: Operation name within the scope.
            trace_id: Trace identifier propagated to both events.
            correlation_id: Business lineage identifier propagated to both events.
            **meta: Domain-specific fields forwarded as top-level keys in ``event.meta``.

        Example::

            with runtime.span(Scope.NODE, "transform", trace_id=tid, flow="ingest"):
                result = transform(message)
        """
        start = perf_counter()
        meta_dict = dict(meta)
        self.emit(
            LifecycleEvent.start(
                scope=scope,
                name=name,
                trace_id=trace_id,
                correlation_id=correlation_id,
                meta=meta_dict,
            )
        )
        try:
            yield
        except Exception as exc:
            self.emit(
                LifecycleEvent.exception(
                    scope=scope,
                    name=name,
                    trace_id=trace_id,
                    correlation_id=correlation_id,
                    duration_ms=(perf_counter() - start) * 1000,
                    error=str(exc),
                    meta={**meta_dict, "error_type": type(exc).__name__},
                )
            )
            raise
        else:
            self.emit(
                LifecycleEvent.end(
                    scope=scope,
                    name=name,
                    trace_id=trace_id,
                    correlation_id=correlation_id,
                    duration_ms=(perf_counter() - start) * 1000,
                    status=LifecycleStatus.SUCCESS,
                    meta=meta_dict,
                )
            )

    @classmethod
    def from_config(cls, config: ObservabilityConfig) -> Self:
        """Build an ``ObservabilityRuntime`` from an ``ObservabilityConfig``.

        Observers are instantiated in order: structlog → OTEL → Prometheus.
        When no backend is enabled, a :class:`~loom.core.observability.observer.noop.NoopObserver`
        is used so the runtime is always safe to call.

        Calling ``configure_logging_from_values`` from inside this method
        guarantees that the structlog pipeline is configured before any
        observer emits its first event.

        Args:
            config: Unified observability configuration.

        Returns:
            Configured ``ObservabilityRuntime`` ready for use.

        Raises:
            ValueError: If OTEL config is invalid (wrong protocol, missing exporter).
            ValueError: If OTEL log export is enabled without a logger config.
        """
        if (
            config.otel.enabled
            and config.otel.export_logs
            and (not config.log.enabled or config.log.config is None)
        ):
            raise ValueError(
                "observability.otel.export_logs requires observability.log.enabled=True "
                "and observability.log.config to be provided."
            )
        if config.otel.enabled and config.otel.export_logs and config.otel.config is None:
            raise ValueError(
                "observability.otel.export_logs requires observability.otel.config to be provided."
            )

        observers = _build_observers(config)
        scrape_port = _resolve_scrape_port(config.prometheus)
        scrape_addr = _resolve_scrape_addr(config.prometheus)
        return cls(
            observers or [NoopObserver()], _scrape_port=scrape_port, _scrape_addr=scrape_addr
        )

    @classmethod
    def noop(cls) -> Self:
        """Build a no-op runtime for tests and environments without observability.

        Returns:
            ``ObservabilityRuntime`` backed by a single ``NoopObserver``.
        """
        return cls([NoopObserver()])


__all__ = ["ObservabilityRuntime"]
