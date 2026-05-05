"""Create observers from configuration — equivalent to ETL ``make_observers``."""

from __future__ import annotations

from loom.streaming.observability.config import StreamingObservabilityConfig
from loom.streaming.observability.observers.composite import CompositeFlowObserver
from loom.streaming.observability.observers.noop import NoopFlowObserver
from loom.streaming.observability.observers.otel import build_otel_observer
from loom.streaming.observability.observers.protocol import StreamingFlowObserver
from loom.streaming.observability.observers.structlog import StructlogFlowObserver


def make_flow_observers(
    config: StreamingObservabilityConfig | None = None,
    *,
    extras: list[StreamingFlowObserver] | None = None,
) -> StreamingFlowObserver:
    """Build a composite flow observer from streaming observability config.

    Args:
        config: Optional YAML-loaded streaming observability config. When
            omitted, defaults to :class:`StreamingObservabilityConfig`.
        extras: Additional user-provided observers.

    Returns:
        A single observer (composite or noop) ready for the runner.
    """
    resolved = config or StreamingObservabilityConfig()

    observers: list[StreamingFlowObserver] = []
    if resolved.log:
        observers.append(
            StructlogFlowObserver(slow_node_threshold_ms=resolved.slow_node_threshold_ms)
        )
    if resolved.otel or resolved.otel_config is not None:
        observers.append(build_otel_observer(resolved.otel_config))
    if extras:
        observers.extend(extras)

    if not observers:
        return NoopFlowObserver()
    if len(observers) == 1:
        return observers[0]
    return CompositeFlowObserver(observers)


__all__ = ["make_flow_observers"]
