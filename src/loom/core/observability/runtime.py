"""ObservabilityRuntime — shared fan-out engine for all Loom modules."""

from __future__ import annotations

import logging
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from time import perf_counter
from typing import TYPE_CHECKING, Self

from loom.core.observability.event import EventKind, LifecycleEvent
from loom.core.observability.protocol import LifecycleObserver

if TYPE_CHECKING:
    from loom.core.observability.config import ObservabilityConfig

_logger = logging.getLogger(__name__)


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

        with runtime.span("use_case", "CreateOrder", trace_id=tid):
            result = use_case.execute(command)
    """

    def __init__(self, observers: Sequence[LifecycleObserver]) -> None:
        self._observers = tuple(observers)

    def emit(self, event: LifecycleEvent) -> None:
        """Emit one lifecycle event to all registered observers.

        Observer failures are caught, logged at WARNING, and discarded.

        Args:
            event: Lifecycle event to dispatch.
        """
        for obs in self._observers:
            try:
                obs.on_event(event)
            except Exception:
                _logger.warning(
                    "observer_error",
                    extra={"observer": type(obs).__name__, "scope": event.scope},
                    exc_info=True,
                )

    @contextmanager
    def span(
        self,
        scope: str,
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
            scope: Logical unit of work (``"node"``, ``"use_case"``, …).
            name: Operation name within the scope.
            trace_id: Trace identifier propagated to both events.
            correlation_id: Business lineage identifier propagated to both events.
            **meta: Additional key-value pairs forwarded in ``event.meta``.

        Example::

            with runtime.span("node", "transform", trace_id=tid):
                result = transform(message)
        """
        start = perf_counter()
        self.emit(
            LifecycleEvent(
                scope=scope,
                name=name,
                kind=EventKind.START,
                trace_id=trace_id,
                correlation_id=correlation_id,
                meta=dict(meta),
            )
        )
        try:
            yield
        except Exception as exc:
            self.emit(
                LifecycleEvent(
                    scope=scope,
                    name=name,
                    kind=EventKind.ERROR,
                    trace_id=trace_id,
                    correlation_id=correlation_id,
                    duration_ms=(perf_counter() - start) * 1000,
                    error=str(exc),
                    meta=dict(meta),
                )
            )
            raise
        else:
            self.emit(
                LifecycleEvent(
                    scope=scope,
                    name=name,
                    kind=EventKind.END,
                    trace_id=trace_id,
                    correlation_id=correlation_id,
                    duration_ms=(perf_counter() - start) * 1000,
                    status="ok",
                    meta=dict(meta),
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
        """
        from loom.core.observability.observer.noop import NoopObserver
        from loom.core.observability.observer.structlog import StructlogLifecycleObserver

        observers: list[LifecycleObserver] = []

        if config.log.enabled:
            if config.log.config is not None:
                from loom.core.logger.config import configure_logging_from_values

                cfg = config.log.config
                configure_logging_from_values(
                    name=cfg.name,
                    environment=cfg.environment,
                    renderer=cfg.renderer,
                    colors=cfg.colors,
                    level=cfg.level,
                    named_levels=cfg.named_levels,
                    handlers=cfg.handlers,
                    fields=cfg.fields,
                )
            observers.append(StructlogLifecycleObserver())

        if config.otel.enabled and config.otel.config is not None:
            from loom.core.observability.observer.otel import OtelLifecycleObserver

            observers.append(
                OtelLifecycleObserver(
                    config=config.otel.config,
                    export_logs=config.otel.export_logs,
                )
            )

        if config.prometheus.enabled:
            from loom.prometheus.lifecycle import PrometheusLifecycleAdapter

            observers.append(
                PrometheusLifecycleAdapter(
                    pushgateway_url=config.prometheus.pushgateway_url,
                )
            )

        return cls(observers or [NoopObserver()])

    @classmethod
    def noop(cls) -> Self:
        """Build a no-op runtime for tests and environments without observability.

        Returns:
            ``ObservabilityRuntime`` backed by a single ``NoopObserver``.
        """
        from loom.core.observability.observer.noop import NoopObserver

        return cls([NoopObserver()])


__all__ = ["ObservabilityRuntime"]
