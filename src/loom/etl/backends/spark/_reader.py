"""Spark source reader - direct implementation without layers."""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from loom.etl.backends._predicate import predicate_to_sql
from loom.etl.backends.spark._dtype import loom_type_to_spark
from loom.etl.io._format import Format
from loom.etl.io._read_options import CsvReadOptions, JsonReadOptions
from loom.etl.io.source import FileSourceSpec, SourceSpec, TableSourceSpec
from loom.etl.schema._schema import ColumnSchema
from loom.etl.storage.protocols import SourceReader
from loom.etl.storage.routing import (
    CatalogRouteResolver,
    CatalogTarget,
    PathRouteResolver,
    ResolvedTarget,
    TableRouteResolver,
)

_log = logging.getLogger(__name__)


class SparkSourceReader(SourceReader):
    """Spark source reader - reads Delta tables and files directly."""

    def __init__(
        self,
        spark: SparkSession,
        locator: str | None = None,
        *,
        route_resolver: TableRouteResolver | None = None,
    ) -> None:
        self._spark = spark

        if route_resolver is None:
            if locator is None:
                resolver: TableRouteResolver = CatalogRouteResolver()
            else:
                from loom.etl.storage._locator import _as_locator

                resolver = PathRouteResolver(_as_locator(locator))
        else:
            resolver = route_resolver

        self._resolver = resolver

    def read(self, spec: SourceSpec, params_instance: Any) -> DataFrame:
        """Read source spec and return DataFrame."""
        if isinstance(spec, TableSourceSpec):
            return self._read_table(spec, params_instance)

        if isinstance(spec, FileSourceSpec):
            return self._read_file(spec)

        raise TypeError(
            f"SparkSourceReader does not support source kind {spec.kind!r}. "
            "TEMP sources are handled by IntermediateStore."
        )

    def _read_table(self, spec: TableSourceSpec, params: Any) -> DataFrame:
        """Read Delta table."""
        target = self._resolver.resolve(spec.table_ref)

        if isinstance(target, CatalogTarget):
            df = self._spark.table(target.catalog_ref.ref)
        else:
            df = self._spark.read.format("delta").load(target.location.uri)

        # Apply predicates
        for pred in spec.predicates:
            df = df.filter(predicate_to_sql(pred, params))

        # Apply column projection
        if spec.columns:
            df = df.select(list(spec.columns))

        # Apply schema casting
        df = self._apply_source_schema(df, spec.schema)

        # Apply JSON decoding
        df = self._apply_json_decode(df, spec.json_columns)

        return df

    def _read_file(self, spec: FileSourceSpec) -> DataFrame:
        """Read file (CSV, JSON, Parquet)."""
        df = self._read_file_by_format(spec.path, spec.format, spec.read_options)

        if spec.columns:
            df = df.select(list(spec.columns))

        df = self._apply_source_schema(df, spec.schema)
        df = self._apply_json_decode(df, spec.json_columns)

        return df

    def _read_file_by_format(self, path: str, format: Any, options: Any) -> DataFrame:
        """Dispatch to format-specific reader."""
        from loom.etl.io._format import Format
        from loom.etl.io._read_options import CsvReadOptions, JsonReadOptions

        fmt = format.value if isinstance(format, Format) else format

        if fmt == "delta":
            return self._spark.read.format("delta").load(path)

        if fmt == "csv":
            opts = options if isinstance(options, CsvReadOptions) else CsvReadOptions()
            reader = (
                self._spark.read.option("sep", opts.separator)
                .option("header", str(opts.has_header).lower())
                .option("encoding", opts.encoding)
                .option("inferSchema", "true")
            )
            if opts.null_values:
                reader = reader.option("nullValue", opts.null_values[0])
            if opts.infer_schema_length is None:
                reader = reader.option("samplingRatio", "1.0")
            return reader.csv(path)

        if fmt == "json":
            opts = options if isinstance(options, JsonReadOptions) else JsonReadOptions()
            reader = self._spark.read.option("inferSchema", "true")
            if opts.infer_schema_length is None:
                reader = reader.option("samplingRatio", "1.0")
            return reader.json(path)

        if fmt == "parquet":
            return self._spark.read.parquet(path)

        raise ValueError(f"Unsupported format: {fmt}")

    def _apply_source_schema(self, df: DataFrame, schema: tuple[ColumnSchema, ...]) -> DataFrame:
        """Cast declared columns to their LoomDtype equivalents."""
        if not schema:
            return df

        from pyspark.sql import functions as F

        from loom.etl.backends.spark._dtype import loom_type_to_spark

        for col in schema:
            df = df.withColumn(col.name, F.col(col.name).cast(loom_type_to_spark(col.dtype)))
        return df

    def _apply_json_decode(self, df: DataFrame, json_columns: tuple[Any, ...]) -> DataFrame:
        """Decode JSON string columns."""
        if not json_columns:
            return df

        from pyspark.sql import functions as F

        from loom.etl.backends.spark._dtype import loom_type_to_spark

        for jc in json_columns:
            schema_ddl = loom_type_to_spark(jc.loom_type).simpleString()
            df = df.withColumn(jc.column, F.from_json(F.col(jc.column), schema_ddl))
        return df


__all__ = ["SparkSourceReader"]
