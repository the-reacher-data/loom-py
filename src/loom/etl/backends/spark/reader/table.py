"""Spark Delta/UC table source reader."""

from __future__ import annotations

import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from loom.etl.io.source import TableSourceSpec
from loom.etl.storage._locator import TableLocator, _as_locator
from loom.etl.storage.route import (
    CatalogRouteResolver,
    CatalogTarget,
    PathRouteResolver,
    TableRouteResolver,
)

from ._shared import apply_json_decode_spark, apply_predicates_spark, apply_source_schema_spark


class SparkDeltaTableReader:
    """Read TABLE sources from Delta paths or Unity Catalog."""

    def __init__(
        self,
        spark: SparkSession,
        locator: str | os.PathLike[str] | TableLocator | None = None,
        *,
        route_resolver: TableRouteResolver | None = None,
    ) -> None:
        self._spark = spark
        if route_resolver is None:
            if locator is None:
                route_resolver = CatalogRouteResolver()
            else:
                route_resolver = PathRouteResolver(_as_locator(locator))
        self._resolver = route_resolver

    def read(self, spec: TableSourceSpec, params_instance: Any) -> DataFrame:
        """Read a TABLE source spec into a Spark DataFrame."""
        target = self._resolver.resolve(spec.table_ref)
        if isinstance(target, CatalogTarget):
            df = self._spark.table(target.catalog_ref.ref)
        else:
            df = self._spark.read.format("delta").load(target.location.uri)

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
