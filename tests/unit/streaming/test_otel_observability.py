"""Tests for streaming OpenTelemetry wiring through core observers."""

from __future__ import annotations

from loom.core.config.observability import OtelConfig
from loom.core.observability.config import ObservabilityConfig
from loom.core.observability.event import LifecycleEvent, LifecycleStatus, Scope
from loom.core.observability.observer.otel import OtelLifecycleObserver


def test_observability_config_defaults() -> None:
    cfg = ObservabilityConfig()

    assert cfg.log.enabled is True
    assert cfg.otel.enabled is False
    assert cfg.otel.config is None
    assert cfg.prometheus.enabled is False


def test_otel_lifecycle_observer_accepts_streaming_lifecycle_events() -> None:
    observer = OtelLifecycleObserver(config=OtelConfig(endpoint="", service_name="loom"))

    observer.on_event(LifecycleEvent.start(scope=Scope.POLL_CYCLE, name="orders"))
    observer.on_event(
        LifecycleEvent.end(
            scope=Scope.POLL_CYCLE,
            name="orders",
            duration_ms=50,
            status=LifecycleStatus.SUCCESS,
        )
    )

    assert isinstance(observer, OtelLifecycleObserver)
