"""Observability factory for ETL runtime observers."""

from __future__ import annotations

from typing import Any

from loom.etl.backends.polars._writer import PolarsTargetWriter
from loom.etl.backends.spark._writer import SparkTargetWriter
from loom.etl.observability.config import ExecutionRecordStoreConfig, ObservabilityConfig
from loom.etl.observability.observers.otel import build_otel_observer
from loom.etl.observability.observers.protocol import ETLRunObserver
from loom.etl.observability.observers.structlog import StructlogRunObserver
from loom.etl.observability.recording._recorder import ExecutionRecordsObserver
from loom.etl.observability.sinks._polars import PolarsExecutionRecordWriter
from loom.etl.observability.sinks._protocol import ExecutionRecordWriter
from loom.etl.observability.sinks._spark import SparkExecutionRecordWriter
from loom.etl.observability.sinks._table import TableExecutionRecordStore
from loom.etl.storage._config import StorageConfig
from loom.etl.storage._locator import PrefixLocator


def make_observers(
    config: ObservabilityConfig,
    storage: StorageConfig | None = None,
    spark: Any = None,
) -> list[ETLRunObserver]:
    """Build runtime observers from observability and storage configs."""
    observers = _build_event_observers(config)
    recording = _build_recording_observer(config, storage, spark)
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
    storage: StorageConfig | None,
    spark: Any,
) -> ExecutionRecordsObserver | None:
    store_cfg = config.record_store
    if store_cfg is None:
        return None
    if storage is None:
        raise ValueError(
            "make_observers: storage config is required when observability.record_store is enabled."
        )
    store_cfg.validate()
    writer = _make_execution_record_writer(storage, store_cfg, spark)
    store = TableExecutionRecordStore(writer=writer, database=store_cfg.database)
    return ExecutionRecordsObserver(store)


def _make_execution_record_writer(
    storage: StorageConfig,
    config: ExecutionRecordStoreConfig,
    spark: Any,
) -> ExecutionRecordWriter:
    if config.database:
        return _make_database_record_writer(storage, spark)
    return _make_path_record_writer(storage, config, spark)


def _make_database_record_writer(storage: StorageConfig, spark: Any) -> ExecutionRecordWriter:
    if storage.engine != "spark":
        raise ValueError(
            "observability.record_store.database is only supported "
            "with storage.engine='spark'. "
            "For storage.engine='polars', configure observability.record_store.root."
        )
    if spark is None:
        raise ValueError(
            "A SparkSession is required to configure observability.record_store "
            "when database destination is enabled."
        )
    return SparkExecutionRecordWriter(spark, SparkTargetWriter(spark, None))


def _make_path_record_writer(
    storage: StorageConfig,
    config: ExecutionRecordStoreConfig,
    spark: Any,
) -> ExecutionRecordWriter:
    locator = PrefixLocator(
        root=config.root,
        storage_options=config.storage_options or None,
        writer=config.writer or None,
        delta_config=config.delta_config or None,
        commit=config.commit or None,
    )
    writers = {
        "polars": _build_polars_path_record_writer,
        "spark": _build_spark_path_record_writer,
    }
    builder = writers.get(storage.engine)
    if builder is None:
        raise ValueError(
            f"Unsupported storage.engine for observability.record_store: {storage.engine}"
        )
    return builder(locator, spark)


def _build_polars_path_record_writer(locator: PrefixLocator, _spark: Any) -> ExecutionRecordWriter:
    return PolarsExecutionRecordWriter(PolarsTargetWriter(locator))


def _build_spark_path_record_writer(locator: PrefixLocator, spark: Any) -> ExecutionRecordWriter:
    if spark is None:
        raise ValueError(
            "A SparkSession is required to configure observability.record_store "
            "when storage.engine='spark'."
        )
    return SparkExecutionRecordWriter(spark, SparkTargetWriter(spark, locator))


__all__ = ["make_observers"]
