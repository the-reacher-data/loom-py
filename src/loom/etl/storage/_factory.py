"""Backend factory — instantiate reader, writer, catalog, observers and temp store
from a resolved :data:`~loom.etl.StorageConfig`.

This module is the single extension point for new storage backends.
Adding a third backend requires:
1. A new ``case NewConfig():`` branch in :func:`make_backends`.
2. A corresponding ``_make_<name>_backends()`` helper.

Internal module — not part of the public API.
"""

from __future__ import annotations

import dataclasses
import logging
from typing import Any, Protocol

from loom.etl.executor import ETLRunObserver
from loom.etl.executor.observer._events import (
    PipelineRunRecord,
    ProcessRunRecord,
    RunRecord,
    StepRunRecord,
)
from loom.etl.executor.observer.sinks._append_writer import RunSinkAppendWriter
from loom.etl.schema._table import TableRef
from loom.etl.storage._config import DeltaConfig, StorageConfig, UnityCatalogConfig
from loom.etl.storage._io import SourceReader, TableDiscovery, TargetWriter
from loom.etl.storage._observability import ObservabilityConfig, RunSinkConfig
from loom.etl.temp._cleaners import TempCleaner
from loom.etl.temp._store import IntermediateStore

_log = logging.getLogger(__name__)


class _TempStoreAware(Protocol):
    """Structural protocol satisfied by every StorageConfig variant.

    All current variants (:class:`~loom.etl.DeltaConfig`,
    :class:`~loom.etl.UnityCatalogConfig`) carry these two fields.  Any future
    backend added to :data:`~loom.etl.StorageConfig` must also declare them so
    that :func:`make_temp_store` works without modification.
    """

    @property
    def tmp_root(self) -> str: ...

    @property
    def tmp_storage_options(self) -> dict[str, str]: ...


def make_backends(
    config: StorageConfig,
    spark: Any = None,
) -> tuple[SourceReader, TargetWriter, TableDiscovery]:
    """Instantiate reader, writer, and catalog from *config*.

    Args:
        config: Resolved storage config.
        spark:  Active SparkSession.  Required when *config* is
                :class:`~loom.etl.UnityCatalogConfig`.

    Returns:
        Triple of (reader, writer, catalog).

    Raises:
        ValueError: When *config* is UnityCatalogConfig and *spark* is None.
    """
    match config:
        case DeltaConfig():
            return _make_polars_backends(config)
        case UnityCatalogConfig():
            if spark is None:
                raise ValueError(
                    "A SparkSession is required for UnityCatalogConfig. "
                    "Pass spark=<session> to ETLRunner.from_yaml() or ETLRunner.from_config()."
                )
            return _make_spark_backends(spark)
        case _:  # pragma: no cover
            raise TypeError(f"Unsupported storage config type: {type(config).__name__!r}")


def make_observers(
    config: ObservabilityConfig,
    storage: StorageConfig,
    spark: Any = None,
) -> list[ETLRunObserver]:
    """Build the observer list from *config*.

    Args:
        config:  Observability configuration.
        storage: Active storage backend configuration.
        spark:   Active SparkSession when using Unity Catalog.

    Returns:
        Ordered list of :class:`~loom.etl.executor.ETLRunObserver` instances.
    """
    from loom.etl.executor.observer._sink_observer import RunSinkObserver
    from loom.etl.executor.observer._structlog import StructlogRunObserver
    from loom.etl.executor.observer.sinks import DeltaRunSink

    observers: list[ETLRunObserver] = []
    if config.log:
        observers.append(StructlogRunObserver(slow_step_threshold_ms=config.slow_step_threshold_ms))
    if config.run_sink is not None:
        sink_cfg = config.run_sink
        sink_cfg.validate()
        sink_writer = _make_run_sink_append_writer(storage, sink_cfg, spark)
        observers.append(RunSinkObserver(DeltaRunSink(sink_writer, database=sink_cfg.database)))
    return observers


def make_temp_store(
    config: _TempStoreAware,
    spark: Any = None,
    cleaner: TempCleaner | None = None,
) -> IntermediateStore | None:
    """Build an IntermediateStore from config, or None when unconfigured.

    Args:
        config:  Any storage config that declares ``tmp_root`` and
                 ``tmp_storage_options`` (satisfies :class:`_TempStoreAware`).
        spark:   Active SparkSession.  When provided, the Spark backend is used;
                 otherwise the Polars Arrow IPC backend is used.
        cleaner: Cloud-aware temp cleaner.

    Returns:
        Configured :class:`~loom.etl._temp_store.IntermediateStore` or None.
    """
    if not config.tmp_root:
        return None
    backend = _make_temp_backend(spark, config.tmp_storage_options or {})
    return IntermediateStore(
        tmp_root=config.tmp_root,
        backend=backend,
        cleaner=cleaner,
    )


def _make_temp_backend(spark: Any, storage_options: dict[str, str]) -> Any:
    """Instantiate the appropriate temp backend for *spark* or Polars.

    Args:
        spark:           Active SparkSession, or ``None`` for Polars.
        storage_options: Cloud credentials for the Polars IPC backend.

    Returns:
        A :class:`~loom.etl.temp._store._TempBackend` instance.
    """
    if spark is not None:
        from loom.etl.backends.spark._temp import _SparkTempBackend

        return _SparkTempBackend(spark)
    from loom.etl.backends.polars._temp import _PolarsTempBackend

    return _PolarsTempBackend(storage_options)


def _make_spark_backends(
    spark: Any,
) -> tuple[SourceReader, TargetWriter, TableDiscovery]:
    from loom.etl.backends.spark import SparkCatalog, SparkSourceReader, SparkTargetWriter

    catalog = SparkCatalog(spark)
    return SparkSourceReader(spark), SparkTargetWriter(spark, None), catalog


def _make_polars_backends(
    config: DeltaConfig,
) -> tuple[SourceReader, TargetWriter, TableDiscovery]:
    from loom.etl.backends.polars import DeltaCatalog, PolarsSourceReader, PolarsTargetWriter

    locator = config.to_locator()
    catalog = DeltaCatalog(locator)
    return PolarsSourceReader(locator), PolarsTargetWriter(locator), catalog


class _PolarsRunSinkAppendWriter:
    def __init__(self, writer: Any) -> None:
        self._writer = writer

    def append(self, record: RunRecord, table_ref: TableRef, /) -> None:
        import polars as pl

        frame = pl.DataFrame([_record_row(record)]).lazy()
        self._writer.append(frame, table_ref, None)


class _SparkRunSinkAppendWriter:
    def __init__(self, spark: Any, writer: Any) -> None:
        self._spark = spark
        self._writer = writer

    def append(self, record: RunRecord, table_ref: TableRef, /) -> None:
        frame = self._spark.createDataFrame([_record_row(record)], schema=_spark_schema(record))
        self._writer.append(frame, table_ref, None)


def _make_run_sink_append_writer(
    storage: StorageConfig,
    config: RunSinkConfig,
    spark: Any,
) -> RunSinkAppendWriter:
    from loom.etl.storage._locator import PrefixLocator

    match storage:
        case DeltaConfig():
            if config.database:
                raise ValueError(
                    "observability.run_sink.database is only supported with Spark/Unity Catalog. "
                    "For Polars storage, configure observability.run_sink.root."
                )
            from loom.etl.backends.polars import PolarsTargetWriter

            locator = PrefixLocator(
                root=config.root,
                storage_options=config.storage_options or None,
                writer=config.writer or None,
                delta_config=config.delta_config or None,
                commit=config.commit or None,
            )
            return _PolarsRunSinkAppendWriter(PolarsTargetWriter(locator))
        case UnityCatalogConfig():
            if spark is None:
                raise ValueError(
                    "A SparkSession is required to configure observability.run_sink "
                    "with Unity Catalog storage."
                )
            from loom.etl.backends.spark import SparkTargetWriter

            if config.database:
                return _SparkRunSinkAppendWriter(spark, SparkTargetWriter(spark, None))

            locator = PrefixLocator(
                root=config.root,
                storage_options=config.storage_options or None,
                writer=config.writer or None,
                delta_config=config.delta_config or None,
                commit=config.commit or None,
            )
            return _SparkRunSinkAppendWriter(spark, SparkTargetWriter(spark, locator))
        case _:  # pragma: no cover
            raise TypeError(f"Unsupported storage config type: {type(storage).__name__!r}")


def _record_row(record: RunRecord) -> dict[str, Any]:
    row = dataclasses.asdict(record)
    row["event"] = str(row["event"])
    row["status"] = str(row["status"])
    return row


def _spark_schema(record: RunRecord) -> Any:
    from pyspark.sql import types as T

    if isinstance(record, PipelineRunRecord):
        return T.StructType(
            [
                T.StructField("event", T.StringType(), False),
                T.StructField("run_id", T.StringType(), False),
                T.StructField("correlation_id", T.StringType(), True),
                T.StructField("attempt", T.LongType(), False),
                T.StructField("pipeline", T.StringType(), False),
                T.StructField("started_at", T.TimestampType(), False),
                T.StructField("status", T.StringType(), False),
                T.StructField("duration_ms", T.LongType(), False),
                T.StructField("error", T.StringType(), True),
            ]
        )
    if isinstance(record, ProcessRunRecord):
        return T.StructType(
            [
                T.StructField("event", T.StringType(), False),
                T.StructField("run_id", T.StringType(), False),
                T.StructField("correlation_id", T.StringType(), True),
                T.StructField("attempt", T.LongType(), False),
                T.StructField("process_run_id", T.StringType(), False),
                T.StructField("process", T.StringType(), False),
                T.StructField("started_at", T.TimestampType(), False),
                T.StructField("status", T.StringType(), False),
                T.StructField("duration_ms", T.LongType(), False),
                T.StructField("error", T.StringType(), True),
            ]
        )
    if isinstance(record, StepRunRecord):
        return T.StructType(
            [
                T.StructField("event", T.StringType(), False),
                T.StructField("run_id", T.StringType(), False),
                T.StructField("correlation_id", T.StringType(), True),
                T.StructField("attempt", T.LongType(), False),
                T.StructField("step_run_id", T.StringType(), False),
                T.StructField("step", T.StringType(), False),
                T.StructField("started_at", T.TimestampType(), False),
                T.StructField("status", T.StringType(), False),
                T.StructField("duration_ms", T.LongType(), False),
                T.StructField("error", T.StringType(), True),
            ]
        )
    raise TypeError(f"Unsupported run record type: {type(record)!r}")
