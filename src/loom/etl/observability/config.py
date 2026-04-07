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


class ObservabilityConfig(msgspec.Struct, frozen=True):
    """Observability config loaded from the ``observability`` YAML section.

    Args:
        log: Enables structured runtime logs via :class:`StructlogRunObserver`.
        otel: Enables OpenTelemetry tracing via :class:`OtelRunObserver`.
              Requires the ``etl-otel`` extra (``opentelemetry-api``).
        record_store: Enables persisted execution records via
            :class:`ExecutionRecordsObserver`.
        slow_step_threshold_ms: Optional slow-step warning threshold.
    """

    log: bool = True
    otel: bool = False
    record_store: ExecutionRecordStoreConfig | None = None
    slow_step_threshold_ms: int | None = None


__all__ = ["ExecutionRecordStoreConfig", "ObservabilityConfig"]
