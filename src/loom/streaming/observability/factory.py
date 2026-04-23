"""Create observers from configuration — equivalent to ETL ``make_observers``."""

from __future__ import annotations

from loom.streaming.observability.observers.composite import CompositeFlowObserver
from loom.streaming.observability.observers.noop import NoopFlowObserver
from loom.streaming.observability.observers.protocol import StreamingFlowObserver
from loom.streaming.observability.observers.structlog import StructlogFlowObserver


def make_flow_observers(
    enabled: bool = True,
    structlog: bool = True,
    extras: list[StreamingFlowObserver] | None = None,
) -> StreamingFlowObserver:
    """Build a composite flow observer from feature flags.

    Args:
        enabled: If ``False``, returns a :class:`NoopFlowObserver`.
        structlog: Include :class:`StructlogFlowObserver` in the composite.
        extras: Additional user-provided observers.

    Returns:
        A single observer (composite or noop) ready for the runner.
    """
    if not enabled:
        return NoopFlowObserver()

    observers: list[StreamingFlowObserver] = []
    if structlog:
        observers.append(StructlogFlowObserver())
    if extras:
        observers.extend(extras)

    if not observers:
        return NoopFlowObserver()
    if len(observers) == 1:
        return observers[0]
    return CompositeFlowObserver(observers)
