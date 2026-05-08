"""Spark backend provider for ETL runner wiring."""

from __future__ import annotations

from typing import Any, cast

from loom.etl.backends.spark._reader import SparkSourceReader
from loom.etl.backends.spark._writer import SparkTargetWriter
from loom.etl.lineage._config import LineageConfig
from loom.etl.lineage.sinks import RecordFrameTargetWriter, TargetLineageWriter
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

    def create_lineage_writer(
        self,
        config: StorageConfig,
        lineage: LineageConfig,
        spark: Any = None,
    ) -> TargetLineageWriter:
        _ = config
        if spark is None:
            raise ValueError(
                "A SparkSession is required to configure observability.lineage "
                "with storage.engine='spark'."
            )
        if lineage.database:
            target_writer = SparkTargetWriter(
                spark,
                None,
                missing_table_policy=config.missing_table_policy,
            )
            return TargetLineageWriter(cast(RecordFrameTargetWriter, target_writer))

        locator = PrefixLocator(
            root=lineage.root,
            storage_options=lineage.storage_options or None,
            writer=lineage.writer or None,
            delta_config=lineage.delta_config or None,
            commit=lineage.commit or None,
        )
        target_writer = SparkTargetWriter(
            spark,
            locator,
            missing_table_policy=config.missing_table_policy,
        )
        return TargetLineageWriter(cast(RecordFrameTargetWriter, target_writer))


__all__ = ["SparkProvider"]
