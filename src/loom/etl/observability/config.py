"""YAML-loadable observability configuration."""

from __future__ import annotations

from typing import Any

import msgspec


class ExecutionRecordStoreConfig(msgspec.Struct, frozen=True):
    """Configuration for persisted execution records.

    Args:
        root: Path/URI destination for path-based table mode.
        database: Database/schema destination for catalog mode.
        storage_options: Cloud credentials for path mode.
        writer: Writer options for path mode.
        delta_config: Delta table properties for path mode.
        commit: Commit metadata for path mode.
    """

    root: str = ""
    database: str = ""
    storage_options: dict[str, str] = {}
    writer: dict[str, Any] = {}
    delta_config: dict[str, str | None] = {}
    commit: dict[str, Any] = {}

    def validate(self) -> None:
        """Validate that exactly one destination mode is configured.

        Raises:
            ValueError: If both or neither of ``root`` and ``database`` are set.
        """
        has_root = bool(self.root.strip())
        has_database = bool(self.database.strip())
        if has_root == has_database:
            raise ValueError(
                "observability.record_store requires exactly one destination: "
                "'root' (path mode) or 'database' (catalog mode)."
            )


class OtelConfig(msgspec.Struct, frozen=True):
    """OpenTelemetry SDK/exporter configuration.

    Args:
        service_name: Resource attribute ``service.name``.
        tracer_name: Tracer instrumentation name.
        tracer_version: Optional tracer instrumentation version.
        protocol: OTLP protocol (``http/protobuf`` or ``grpc``).
        endpoint: OTLP endpoint URI. When empty, uses global OTel runtime defaults.
        insecure: Exporter transport mode when supported by protocol/exporter.
        headers: Exporter request headers (vendor auth/tags).
        resource_attributes: Additional OTel resource attributes.
        span_attributes: Static span attributes added to all ETL spans.
        exporter_kwargs: Extra keyword args passed through to OTLP exporter.
        span_processor_kwargs: Extra keyword args passed through to BatchSpanProcessor.
    """

    service_name: str = "loom-etl"
    tracer_name: str = "loom.etl"
    tracer_version: str = ""
    protocol: str = "http/protobuf"
    endpoint: str = ""
    insecure: bool = True
    headers: dict[str, str] = {}
    resource_attributes: dict[str, str] = {}
    span_attributes: dict[str, str] = {}
    exporter_kwargs: dict[str, Any] = {}
    span_processor_kwargs: dict[str, Any] = {}

    def validate(self) -> None:
        """Validate protocol field.

        Raises:
            ValueError: If protocol is not supported.
        """
        if self.protocol not in {"http/protobuf", "grpc"}:
            raise ValueError(
                "observability.otel_config.protocol must be either 'http/protobuf' or 'grpc'."
            )


class ObservabilityConfig(msgspec.Struct, frozen=True):
    """Observability config loaded from the ``observability`` YAML section.

    Args:
        log: Enables structured runtime logs via :class:`StructlogRunObserver`.
        otel: Enables OpenTelemetry tracing via :class:`OtelRunObserver`.
              Requires the ``etl-otel`` extra.
        otel_config: Optional OTel SDK/exporter config.
                     When set, OTel tracing is enabled even if ``otel=False``.
        record_store: Enables persisted execution records via
            :class:`ExecutionRecordsObserver`.
        slow_step_threshold_ms: Optional slow-step warning threshold.
    """

    log: bool = True
    otel: bool = False
    otel_config: OtelConfig | None = None
    record_store: ExecutionRecordStoreConfig | None = None
    slow_step_threshold_ms: int | None = None


__all__ = ["ExecutionRecordStoreConfig", "ObservabilityConfig", "OtelConfig"]
