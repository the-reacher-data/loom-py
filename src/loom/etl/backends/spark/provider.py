"""Spark backend provider for ETL runner wiring."""

from __future__ import annotations

from typing import Any

from loom.etl.backends.spark._reader import SparkSourceReader
from loom.etl.backends.spark._writer import SparkTargetWriter
from loom.etl.runner._providers import BackendProvider
from loom.etl.runtime.contracts import SourceReader, TargetWriter
from loom.etl.storage._config import StorageConfig
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
        return (
            SparkSourceReader(spark, route_resolver=route_resolver),
            SparkTargetWriter(
                spark,
                None,
                route_resolver=route_resolver,
                missing_table_policy=config.missing_table_policy,
            ),
        )


__all__ = ["SparkProvider"]
