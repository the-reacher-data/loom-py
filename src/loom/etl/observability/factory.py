"""Observability factory for ETL runtime observers."""

from __future__ import annotations

from typing import Any

from loom.etl.observability.config import ExecutionRecordStoreConfig, ObservabilityConfig
from loom.etl.observability.observers.execution_records import ExecutionRecordsObserver
from loom.etl.observability.observers.protocol import ETLRunObserver
from loom.etl.observability.observers.structlog import StructlogRunObserver
from loom.etl.observability.stores.table import TableExecutionRecordStore
from loom.etl.observability.writers.polars import PolarsExecutionRecordWriter
from loom.etl.observability.writers.protocol import ExecutionRecordWriter
from loom.etl.observability.writers.spark import SparkExecutionRecordWriter
from loom.etl.storage._config import DeltaConfig, StorageConfig, UnityCatalogConfig


def make_observers(
    config: ObservabilityConfig,
    storage: StorageConfig | None = None,
    spark: Any = None,
) -> list[ETLRunObserver]:
    """Build runtime observers from observability and storage configs."""
    observers: list[ETLRunObserver] = []
    if config.log:
        observers.append(StructlogRunObserver(slow_step_threshold_ms=config.slow_step_threshold_ms))
    if config.otel:
        from loom.etl.observability.observers.otel import OtelRunObserver

        observers.append(OtelRunObserver())
    if config.record_store is None:
        return observers
    if storage is None:
        raise ValueError(
            "make_observers: storage config is required when observability.record_store is enabled."
        )
    store_cfg = config.record_store
    store_cfg.validate()
    writer = _make_execution_record_writer(storage, store_cfg, spark)
    observers.append(
        ExecutionRecordsObserver(
            TableExecutionRecordStore(writer=writer, database=store_cfg.database)
        )
    )
    return observers


def _make_execution_record_writer(
    storage: StorageConfig,
    config: ExecutionRecordStoreConfig,
    spark: Any,
) -> ExecutionRecordWriter:
    from loom.etl.storage._locator import PrefixLocator

    match storage:
        case DeltaConfig():
            if config.database:
                raise ValueError(
                    "observability.record_store.database is only supported "
                    "with Spark/Unity Catalog. "
                    "For Delta/Polars storage, configure "
                    "observability.record_store.root."
                )
            from loom.etl.backends.polars import PolarsTargetWriter

            locator = PrefixLocator(
                root=config.root,
                storage_options=config.storage_options or None,
                writer=config.writer or None,
                delta_config=config.delta_config or None,
                commit=config.commit or None,
            )
            return PolarsExecutionRecordWriter(PolarsTargetWriter(locator))
        case UnityCatalogConfig():
            if spark is None:
                raise ValueError(
                    "A SparkSession is required to configure observability.record_store "
                    "with Unity Catalog storage."
                )
            from loom.etl.backends.spark import SparkTargetWriter

            if config.database:
                return SparkExecutionRecordWriter(spark, SparkTargetWriter(spark, None))

            locator = PrefixLocator(
                root=config.root,
                storage_options=config.storage_options or None,
                writer=config.writer or None,
                delta_config=config.delta_config or None,
                commit=config.commit or None,
            )
            return SparkExecutionRecordWriter(spark, SparkTargetWriter(spark, locator))
        case _:
            raise TypeError(f"Unsupported storage config type: {type(storage).__name__!r}")


__all__ = ["make_observers"]
