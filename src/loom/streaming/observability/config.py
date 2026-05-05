"""YAML-loadable observability configuration for streaming runtimes."""

from __future__ import annotations

import msgspec

from loom.core.config.observability import OtelConfig


class StreamingObservabilityConfig(msgspec.Struct, frozen=True):
    """Observability config loaded from the ``streaming.observability`` YAML section.

    Args:
        log: Enables structured runtime logs via :class:`StructlogFlowObserver`.
        otel: Enables OpenTelemetry tracing via :class:`OtelFlowObserver`.
              Requires the ``streaming-otel`` extra.
        otel_config: Optional OTel SDK/exporter config. When set, OTel tracing
            is enabled even if ``otel=False``.
        slow_node_threshold_ms: Optional slow-node warning threshold.
    """

    log: bool = True
    otel: bool = False
    otel_config: OtelConfig | None = None
    slow_node_threshold_ms: int | None = None


__all__ = ["StreamingObservabilityConfig"]
