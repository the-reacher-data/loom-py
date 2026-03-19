"""Prometheus observability for Loom.

Optional module — requires ``prometheus-client``::

    pip install "loom-py[prometheus]"

Provides:

- :class:`PrometheusMetricsAdapter` — wires into the bootstrap pipeline to
  record use-case level metrics (request counts, durations, errors).
- :class:`PrometheusMiddleware` — pure ASGI middleware that records
  HTTP-level metrics (request counts, durations by path template).

Example::

    import prometheus_client
    from loom.prometheus import PrometheusMetricsAdapter, PrometheusMiddleware
    from loom.rest.middleware import TraceIdMiddleware

    adapter = PrometheusMetricsAdapter()

    result = bootstrap_app(
        config=cfg,
        use_cases=[CreateOrderUseCase, GetOrderUseCase],
        modules=[register_repositories],
        metrics=adapter,
    )
    app = create_fastapi_app(
        result,
        interfaces=[OrderRestInterface],
        middleware=[TraceIdMiddleware, PrometheusMiddleware],
    )
    # Expose /metrics as an exact route in the FastAPI app.
"""

from loom.prometheus.adapter import PrometheusMetricsAdapter
from loom.prometheus.middleware import PrometheusMiddleware

__all__ = [
    "PrometheusMetricsAdapter",
    "PrometheusMiddleware",
]
