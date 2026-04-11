"""Observability factory for ETL runtime observers."""

from __future__ import annotations

from loom.etl.observability.config import ObservabilityConfig
from loom.etl.observability.observers.otel import build_otel_observer
from loom.etl.observability.observers.protocol import ETLRunObserver
from loom.etl.observability.observers.structlog import StructlogRunObserver
from loom.etl.observability.recording._recorder import ExecutionRecordsObserver
from loom.etl.observability.sinks._protocol import ExecutionRecordWriter
from loom.etl.observability.sinks._table import TableExecutionRecordStore


def make_observers(
    config: ObservabilityConfig,
    *,
    record_writer: ExecutionRecordWriter | None = None,
) -> list[ETLRunObserver]:
    """Build runtime observers from observability config and optional record writer."""
    observers = _build_event_observers(config)
    recording = _build_recording_observer(config, record_writer)
    if recording is not None:
        observers.append(recording)
    return observers


def _build_event_observers(config: ObservabilityConfig) -> list[ETLRunObserver]:
    observers: list[ETLRunObserver] = []
    if config.log:
        observers.append(StructlogRunObserver(slow_step_threshold_ms=config.slow_step_threshold_ms))
    if config.otel or config.otel_config is not None:
        observers.append(build_otel_observer(config.otel_config))
    return observers


def _build_recording_observer(
    config: ObservabilityConfig,
    record_writer: ExecutionRecordWriter | None,
) -> ExecutionRecordsObserver | None:
    store_cfg = config.record_store
    if store_cfg is None:
        return None
    if record_writer is None:
        raise ValueError(
            "make_observers: record_writer is required when observability.record_store is enabled."
        )
    store_cfg.validate()
    store = TableExecutionRecordStore(writer=record_writer, database=store_cfg.database)
    return ExecutionRecordsObserver(store)


__all__ = ["make_observers"]
