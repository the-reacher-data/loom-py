"""Spark backend provider for ETL runner wiring."""

from __future__ import annotations

from typing import Any

from loom.etl.backends.spark._reader import SparkSourceReader
from loom.etl.backends.spark._writer import SparkTargetWriter
from loom.etl.observability.config import ExecutionRecordStoreConfig
from loom.etl.observability.sinks import ExecutionRecordWriter, TargetExecutionRecordWriter
from loom.etl.runner._providers import BackendProvider
from loom.etl.runtime.contracts import SourceReader, TargetWriter
from loom.etl.storage._config import StorageConfig
from loom.etl.storage._locator import PrefixLocator
from loom.etl.storage.routing import build_table_resolver


class SparkProvider(BackendProvider):
    """Create Spark reader/writer pair from storage config."""

    def create_backends(
        self,
        config: StorageConfig,
        spark: Any = None,
    ) -> tuple[SourceReader, TargetWriter]:
        if spark is None:
            raise ValueError(
                "A SparkSession is required for Spark backend provider. "
                "Pass spark=<session> to ETLRunner.from_yaml() or ETLRunner.from_config()."
            )
        route_resolver = build_table_resolver(config)
        file_locator = config.to_file_locator()
        return (
            SparkSourceReader(spark, route_resolver=route_resolver, file_locator=file_locator),
            SparkTargetWriter(
                spark,
                None,
                route_resolver=route_resolver,
                missing_table_policy=config.missing_table_policy,
                file_locator=file_locator,
            ),
        )

    def create_execution_record_writer(
        self,
        config: StorageConfig,
        record_store: ExecutionRecordStoreConfig,
        spark: Any = None,
    ) -> ExecutionRecordWriter:
        _ = config
        if spark is None:
            raise ValueError(
                "A SparkSession is required to configure observability.record_store "
                "with storage.engine='spark'."
            )
        if record_store.database:
            target_writer = SparkTargetWriter(spark, None)
            return TargetExecutionRecordWriter(target_writer)

        locator = PrefixLocator(
            root=record_store.root,
            storage_options=record_store.storage_options or None,
            writer=record_store.writer or None,
            delta_config=record_store.delta_config or None,
            commit=record_store.commit or None,
        )
        target_writer = SparkTargetWriter(spark, locator)
        return TargetExecutionRecordWriter(target_writer)


__all__ = ["SparkProvider"]
