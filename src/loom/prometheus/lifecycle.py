"""Prometheus lifecycle adapter — records LifecycleEvent as Prometheus instruments."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from loom.core.observability.event import EventKind, LifecycleEvent

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry, Counter, Histogram

_GLOBAL_INSTRUMENTS: tuple[Histogram, Counter] | None = None


def _create_instruments(
    registry: CollectorRegistry | None,
) -> tuple[Histogram, Counter]:
    from prometheus_client import Counter, Histogram

    reg: dict[str, Any] = {"registry": registry} if registry is not None else {}
    duration: Histogram = Histogram(
        "lifecycle_duration_seconds",
        "Lifecycle span wall-clock duration in seconds.",
        ["scope", "name"],
        **reg,
    )
    errors: Counter = Counter(
        "lifecycle_errors_total",
        "Total lifecycle span errors by scope and name.",
        ["scope", "name"],
        **reg,
    )
    return duration, errors


def _get_instruments(
    registry: CollectorRegistry | None,
) -> tuple[Histogram, Counter]:
    global _GLOBAL_INSTRUMENTS
    if registry is not None:
        return _create_instruments(registry)
    if _GLOBAL_INSTRUMENTS is None:
        _GLOBAL_INSTRUMENTS = _create_instruments(None)
    return _GLOBAL_INSTRUMENTS


class PrometheusLifecycleAdapter:
    """Lifecycle observer that records span durations and errors as Prometheus metrics.

    Instruments:

    - ``lifecycle_duration_seconds`` — Histogram, labels: ``scope``, ``name``.
      Observed on ``END`` events when ``duration_ms`` is present.
    - ``lifecycle_errors_total`` — Counter, labels: ``scope``, ``name``.
      Incremented on ``ERROR`` events.

    For long-running services (streaming, REST), metrics are served at ``/metrics``
    via the standard ``prometheus_client`` default registry. For batch jobs (ETL),
    pass ``pushgateway_url`` to push metrics to a Pushgateway at runner shutdown.

    Args:
        registry: Optional custom ``CollectorRegistry``. Pass an isolated registry
            in tests to prevent cross-test pollution.
        pushgateway_url: Pushgateway URL for batch-job metric delivery.
            Call :meth:`flush` explicitly at runner shutdown when set.

    Example::

        adapter = PrometheusLifecycleAdapter()
        runtime = ObservabilityRuntime([adapter])

        with runtime.span(Scope.USE_CASE, "CreateOrder"):
            ...

        # For ETL batch jobs:
        adapter.flush(job_name="ingest_orders")
    """

    def __init__(
        self,
        registry: CollectorRegistry | None = None,
        *,
        pushgateway_url: str | None = None,
    ) -> None:
        self._duration, self._errors = _get_instruments(registry)
        self._pushgateway_url = pushgateway_url
        self._registry = registry

    def on_event(self, event: LifecycleEvent) -> None:
        """Record one lifecycle event on Prometheus instruments.

        Args:
            event: Lifecycle event from the runtime.
        """
        if event.kind is EventKind.END and event.duration_ms is not None:
            self._duration.labels(scope=event.scope, name=event.name).observe(
                event.duration_ms / 1000
            )
        elif event.kind is EventKind.ERROR:
            self._errors.labels(scope=event.scope, name=event.name).inc()

    def flush(self, *, job_name: str = "loom_etl") -> None:
        """Push accumulated metrics to the configured Pushgateway.

        A no-op when no ``pushgateway_url`` was provided at construction.

        Args:
            job_name: Pushgateway job label. Defaults to ``"loom_etl"``.

        Raises:
            RuntimeError: If ``prometheus_client.push_to_gateway`` fails.
        """
        if self._pushgateway_url is None:
            return
        from prometheus_client import push_to_gateway

        registry = self._registry
        if registry is None:
            from prometheus_client import REGISTRY

            registry = REGISTRY
        push_to_gateway(self._pushgateway_url, job=job_name, registry=registry)


__all__ = ["PrometheusLifecycleAdapter"]
