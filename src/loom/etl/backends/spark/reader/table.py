"""Spark Delta/UC table source reader."""

from __future__ import annotations

import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from loom.etl.io.source import TableSourceSpec
from loom.etl.storage._locator import TableLocator, _as_locator

from ._shared import apply_json_decode_spark, apply_predicates_spark, apply_source_schema_spark


class SparkDeltaTableReader:
    """Read TABLE sources from Delta paths or Unity Catalog."""

    def __init__(
        self,
        spark: SparkSession,
        locator: str | os.PathLike[str] | TableLocator | None = None,
    ) -> None:
        self._spark = spark
        self._locator = _as_locator(locator) if locator is not None else None

    def read(self, spec: TableSourceSpec, params_instance: Any) -> DataFrame:
        """Read a TABLE source spec into a Spark DataFrame."""
        if self._locator is None:
            df = self._spark.table(spec.table_ref.ref)
        else:
            loc = self._locator.locate(spec.table_ref)
            df = self._spark.read.format("delta").load(loc.uri)

        if spec.columns:
            df = df.select(list(spec.columns))
        return apply_predicates_spark(
            apply_json_decode_spark(
                apply_source_schema_spark(df, spec.schema),
                spec.json_columns,
            ),
            spec.predicates,
            params_instance,
        )
