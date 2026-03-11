"""Prometheus metrics adapter for Loom use-case events.

Implements the :class:`~loom.core.engine.metrics.MetricsAdapter` protocol
using ``prometheus_client`` instruments.  Install the optional dependency
with::

    pip install "loom-py[prometheus]"

Usage::

    from loom.prometheus import PrometheusMetricsAdapter

    adapter = PrometheusMetricsAdapter()

    result = bootstrap_app(
        config=cfg,
        use_cases=[CreateOrderUseCase],
        modules=[register_repositories],
        metrics=adapter,
    )
    app = create_fastapi_app(result, interfaces=[OrderInterface])
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from loom.core.engine.events import EventKind, RuntimeEvent

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry, Counter, Histogram

# Singleton instruments for the global default registry.  A module-level
# cache prevents duplicate-collector errors when more than one adapter
# instance is created in the same process (e.g. multiple apps, dev reload,
# or tests that omit an explicit registry).  Adapters that pass a custom
# registry always receive fresh instruments — test isolation is unaffected.
_GLOBAL_INSTRUMENTS: tuple[Counter, Histogram, Counter] | None = None


def _create_instruments(
    registry: CollectorRegistry | None,
) -> tuple[Counter, Histogram, Counter]:
    """Create Prometheus instruments bound to *registry*.

    Args:
        registry: Target ``CollectorRegistry``.  ``None`` uses the
            prometheus_client global default registry.

    Returns:
        Triple of ``(requests_total, duration_seconds, errors_total)``.
    """
    from prometheus_client import Counter, Histogram

    reg: dict[str, Any] = {"registry": registry} if registry is not None else {}
    requests_total: Counter = Counter(
        "loom_usecase_requests",
        "Total number of use-case executions by outcome.",
        ["usecase", "status"],
        **reg,
    )
    duration_seconds: Histogram = Histogram(
        "loom_usecase_duration_seconds",
        "Use-case execution wall-clock time in seconds.",
        ["usecase"],
        **reg,
    )
    errors_total: Counter = Counter(
        "loom_usecase_errors",
        "Total number of use-case execution errors by error kind.",
        ["usecase", "error_kind"],
        **reg,
    )
    return requests_total, duration_seconds, errors_total


def _get_instruments(
    registry: CollectorRegistry | None,
) -> tuple[Counter, Histogram, Counter]:
    """Return instruments, creating them if needed.

    When *registry* is ``None`` (global default), instruments are created
    once and reused for the lifetime of the process.  When a custom
    registry is provided, fresh instruments are always returned so that
    test suites with isolated registries do not share state.

    Args:
        registry: Custom registry or ``None`` for the global default.

    Returns:
        Triple of ``(requests_total, duration_seconds, errors_total)``.
    """
    global _GLOBAL_INSTRUMENTS
    if registry is not None:
        return _create_instruments(registry)
    if _GLOBAL_INSTRUMENTS is None:
        _GLOBAL_INSTRUMENTS = _create_instruments(None)
    return _GLOBAL_INSTRUMENTS


class PrometheusMetricsAdapter:
    """Prometheus implementation of the ``MetricsAdapter`` protocol.

    Records three instruments per use-case:

    - ``loom_usecase_requests_total`` — Counter, labels: ``usecase``, ``status``.
    - ``loom_usecase_duration_seconds`` — Histogram, labels: ``usecase``.
    - ``loom_usecase_errors_total`` — Counter, labels: ``usecase``, ``error_kind``.

    Event mapping:

    ============  ============================================================
    EventKind     Action
    ============  ============================================================
    EXEC_START    noop — duration is measured end-to-end and arrives in EXEC_DONE
    EXEC_DONE     increment ``requests_total{status="success"}``; observe histogram
    EXEC_ERROR    increment ``requests_total{status=<event.status>}``; increment
                  ``errors_total{error_kind=<type(error).__name__>}``
    others        noop
    ============  ============================================================

    Args:
        registry: Optional ``CollectorRegistry``.  Defaults to
            ``prometheus_client.REGISTRY`` (the global default registry).
            Pass a custom registry in tests to avoid cross-test pollution.
            Multiple adapter instances sharing the default registry reuse
            the same instruments — no duplicate-collector errors.

    Example::

        from loom.prometheus import PrometheusMetricsAdapter

        adapter = PrometheusMetricsAdapter()
        result  = bootstrap_app(config=cfg, use_cases=[...], metrics=adapter)
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self._requests_total, self._duration_seconds, self._errors_total = _get_instruments(
            registry
        )

    def on_event(self, event: RuntimeEvent) -> None:
        """Process a runtime event and update Prometheus instruments.

        Args:
            event: Immutable event from the executor or compiler.
        """
        if event.kind == EventKind.EXEC_DONE:
            self._requests_total.labels(
                usecase=event.use_case_name,
                status=event.status or "success",
            ).inc()
            if event.duration_ms is not None:
                self._duration_seconds.labels(
                    usecase=event.use_case_name,
                ).observe(event.duration_ms / 1000)

        elif event.kind == EventKind.EXEC_ERROR:
            self._requests_total.labels(
                usecase=event.use_case_name,
                status=event.status or "failure",
            ).inc()
            error_kind = type(event.error).__name__ if event.error is not None else "unknown"
            self._errors_total.labels(
                usecase=event.use_case_name,
                error_kind=error_kind,
            ).inc()
