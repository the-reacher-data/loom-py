"""SparkBackend — Compute backend for Apache Spark."""

from __future__ import annotations

from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.readwriter import DataFrameWriter

from loom.etl.io._format import Format
from loom.etl.io._read_options import CsvReadOptions, JsonReadOptions
from loom.etl.io._write_options import CsvWriteOptions, JsonWriteOptions, ParquetWriteOptions
from loom.etl.io.target import SchemaMode
from loom.etl.schema._schema import ColumnSchema, LoomType
from loom.etl.schema._table import TableRef
from loom.etl.sql._predicate_sql import predicate_to_sql
from loom.etl.storage.route.model import CatalogTarget, ResolvedTarget
from loom.etl.storage.schema.model import SparkPhysicalSchema


class SparkReadOps:
    """Spark read operations for tables and files."""

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    # ==========================================================================
    # TABLE READ
    # ==========================================================================

    def table(
        self,
        target: ResolvedTarget,
        *,
        columns: tuple[str, ...] | None = None,
        predicates: tuple[Any, ...] = (),
        params: Any = None,
    ) -> DataFrame:
        """Read Delta table from catalog or path."""
        if isinstance(target, CatalogTarget):
            df = self._spark.table(target.catalog_ref.ref)
        else:
            df = self._spark.read.format("delta").load(target.location.uri)

        if columns:
            df = df.select(list(columns))

        for pred in predicates:
            df = df.filter(predicate_to_sql(pred, params))

        return df

    # ==========================================================================
    # FILE READ
    # ==========================================================================

    def file(
        self,
        path: str,
        format: str | Format,
        options: Any = None,
        *,
        columns: tuple[str, ...] | None = None,
        schema: tuple[ColumnSchema, ...] | None = None,
        json_columns: tuple[Any, ...] = (),
    ) -> DataFrame:
        """Read file (CSV, JSON, Parquet, Delta)."""
        fmt = format.value if isinstance(format, Format) else format
        df = self._read_file_by_format(path, fmt, options)

        if columns:
            df = df.select(list(columns))

        if schema:
            df = self._apply_source_schema(df, schema)

        if json_columns:
            df = self._apply_json_decode(df, json_columns)

        return df

    def _read_file_by_format(self, path: str, fmt: str, options: Any) -> DataFrame:
        """Dispatch to format-specific reader."""
        if fmt == "delta":
            return self._spark.read.format("delta").load(path)

        if fmt == "csv":
            csv_options = options if isinstance(options, CsvReadOptions) else CsvReadOptions()
            if csv_options.skip_rows:
                raise ValueError(
                    f"skip_rows={csv_options.skip_rows} not supported by Spark CSV reader. "
                    "Use Polars backend or pre-process the file."
                )
            reader = (
                self._spark.read.option("sep", csv_options.separator)
                .option("header", str(csv_options.has_header).lower())
                .option("encoding", csv_options.encoding)
                .option("inferSchema", "true")
            )
            if csv_options.null_values:
                reader = reader.option("nullValue", csv_options.null_values[0])
            if csv_options.infer_schema_length is None:
                reader = reader.option("samplingRatio", "1.0")
            return reader.csv(path)

        if fmt == "json":
            json_options = options if isinstance(options, JsonReadOptions) else JsonReadOptions()
            reader = self._spark.read.option("inferSchema", "true")
            if json_options.infer_schema_length is None:
                reader = reader.option("samplingRatio", "1.0")
            return reader.json(path)

        if fmt == "parquet":
            return self._spark.read.parquet(path)

        if fmt == "xlsx":
            raise TypeError(
                "Spark file reader does not support XLSX natively. "
                "Use CSV/JSON/PARQUET, or add spark-excel and a custom SourceReader."
            )

        raise ValueError(f"Unsupported format: {fmt}")

    def _apply_source_schema(self, df: DataFrame, schema: tuple[ColumnSchema, ...]) -> DataFrame:
        """Cast columns to schema types."""
        from loom.etl.backends.spark._dtype import loom_type_to_spark

        for col in schema:
            df = df.withColumn(col.name, F.col(col.name).cast(loom_type_to_spark(col.dtype)))
        return df

    def _apply_json_decode(self, df: DataFrame, json_columns: tuple[Any, ...]) -> DataFrame:
        """Decode JSON string columns."""
        from loom.etl.backends.spark._dtype import loom_type_to_spark

        for jc in json_columns:
            schema_ddl = loom_type_to_spark(jc.loom_type).simpleString()
            df = df.withColumn(jc.column, F.from_json(F.col(jc.column), schema_ddl))
        return df


class SparkSchemaOps:
    """Spark schema operations."""

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def exists(self, ref: TableRef) -> bool:
        """Check if table exists in catalog."""
        return bool(self._spark.catalog.tableExists(ref.ref))

    def columns(self, ref: TableRef) -> tuple[str, ...]:
        """List table columns."""
        if not self.exists(ref):
            return ()
        return tuple(self._spark.table(ref.ref).columns)

    def schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        """Read complete table schema."""
        if not self.exists(ref):
            return None
        fields = self._spark.table(ref.ref).schema.fields
        return tuple(
            ColumnSchema(
                name=f.name,
                dtype=self._to_loom(f.dataType),
                nullable=f.nullable,
            )
            for f in fields
        )

    def physical(self, target: ResolvedTarget) -> SparkPhysicalSchema | None:
        """Read physical schema for validation."""
        if isinstance(target, CatalogTarget):
            if not self.exists(target.catalog_ref):
                return None
            fields = self._spark.table(target.catalog_ref.ref).schema.fields
            return SparkPhysicalSchema(schema=T.StructType(list(fields)))
        else:
            try:
                dt = DeltaTable.forPath(self._spark, target.location.uri)
                return SparkPhysicalSchema(schema=dt.toDF().schema)
            except Exception:
                return None

    def align(
        self,
        frame: DataFrame,
        existing_schema: SparkPhysicalSchema | None,
        mode: SchemaMode,
    ) -> DataFrame:
        """Align frame schema with existing."""
        if existing_schema is None or mode is SchemaMode.OVERWRITE:
            return frame

        from loom.etl.backends.spark._schema import spark_apply_schema

        return spark_apply_schema(frame, existing_schema.schema, mode)

    def materialize(self, frame: DataFrame, streaming: bool) -> DataFrame:
        """Spark DataFrames are already eager."""
        return frame

    def _to_loom(self, dtype: T.DataType) -> LoomType:
        """Convert Spark type to Loom."""
        from loom.etl.backends.spark._dtype import spark_to_loom

        return spark_to_loom(dtype)


class SparkWriteOps:
    """Spark write operations for tables and files."""

    def __init__(self, spark: SparkSession, schema_ops: SparkSchemaOps) -> None:
        self._spark = spark
        self._schema = schema_ops

    # ==========================================================================
    # TABLE WRITE
    # ==========================================================================

    def create(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
        partition_cols: tuple[str, ...] = (),
    ) -> None:
        """Create new table (first run)."""
        writer = (
            frame.write.format("delta")
            .option("optimizeWrite", "true")
            .mode("overwrite")
            .option("overwriteSchema", "true")
        )
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        self._sink(writer, target)

    def append(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Append to table."""
        writer = frame.write.format("delta").option("optimizeWrite", "true").mode("append")
        if schema_mode is SchemaMode.EVOLVE:
            writer = writer.option("mergeSchema", "true")
        self._sink(writer, target)

    def replace(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Replace table."""
        writer = frame.write.format("delta").option("optimizeWrite", "true").mode("overwrite")
        if schema_mode is SchemaMode.OVERWRITE:
            writer = writer.option("overwriteSchema", "true")
        self._sink(writer, target)

    def replace_partitions(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
    ) -> None:
        """Replace partitions."""
        rows = frame.select(*partition_cols).distinct().collect()
        if not rows:
            return

        predicates = [
            f"({' AND '.join(f'{c} = {repr(row[c])}' for c in partition_cols)})" for row in rows
        ]
        predicate = " OR ".join(predicates)

        writer = (
            frame.sortWithinPartitions(*partition_cols)
            .write.format("delta")
            .option("optimizeWrite", "true")
            .mode("overwrite")
            .option("replaceWhere", predicate)
        )
        self._sink(writer, target)

    def replace_where(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        predicate: str,
        schema_mode: SchemaMode,
    ) -> None:
        """Replace where predicate matches."""
        writer = (
            frame.write.format("delta")
            .option("optimizeWrite", "true")
            .mode("overwrite")
            .option("replaceWhere", predicate)
        )
        self._sink(writer, target)

    def upsert(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        keys: tuple[str, ...],
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
        existing_schema: SparkPhysicalSchema | None,
    ) -> None:
        """Upsert/merge."""
        aligned = self._schema.align(frame, existing_schema, schema_mode)

        if isinstance(target, CatalogTarget):
            dt = DeltaTable.forName(self._spark, target.catalog_ref.ref)
        else:
            dt = DeltaTable.forPath(self._spark, target.location.uri)

        all_keys = list(keys) + list(partition_cols)
        merge_pred = " AND ".join(f"target.{k} = source.{k}" for k in all_keys)
        update_cols = [c for c in aligned.columns if c not in keys]

        (
            dt.alias("target")
            .merge(aligned.alias("source"), merge_pred)
            .whenMatchedUpdate(set={c: F.col(f"source.{c}") for c in update_cols})
            .whenNotMatchedInsert(values={c: F.col(f"source.{c}") for c in aligned.columns})
            .execute()
        )

    def _sink(self, writer: DataFrameWriter, target: ResolvedTarget) -> None:
        """Finalize write based on target type."""
        if isinstance(target, CatalogTarget):
            writer.saveAsTable(target.catalog_ref.ref)
        else:
            writer.save(target.location.uri)

    # ==========================================================================
    # FILE WRITE
    # ==========================================================================

    def file(
        self,
        frame: DataFrame,
        path: str,
        format: str | Format,
        options: Any = None,
    ) -> None:
        """Write file (CSV, JSON, Parquet, Delta)."""
        fmt = format.value if isinstance(format, Format) else format

        if fmt == "delta":
            frame.write.format("delta").mode("overwrite").save(path)
            return

        if fmt == "csv":
            csv_options = options if isinstance(options, CsvWriteOptions) else CsvWriteOptions()
            writer = (
                frame.write.mode("overwrite")
                .option("sep", csv_options.separator)
                .option("header", str(csv_options.has_header).lower())
            )
            self._apply_kwargs(writer, csv_options.kwargs).csv(path)
            return

        if fmt == "json":
            json_options = options if isinstance(options, JsonWriteOptions) else JsonWriteOptions()
            self._apply_kwargs(frame.write.mode("overwrite"), json_options.kwargs).json(path)
            return

        if fmt == "parquet":
            parquet_options = (
                options if isinstance(options, ParquetWriteOptions) else ParquetWriteOptions()
            )
            writer = frame.write.mode("overwrite").option(
                "compression",
                parquet_options.compression,
            )
            self._apply_kwargs(writer, parquet_options.kwargs).parquet(path)
            return

        if fmt == "xlsx":
            raise TypeError(
                "Spark file writer does not support XLSX natively. "
                "Use CSV/JSON/PARQUET, or add spark-excel."
            )

        raise ValueError(f"Unsupported format: {fmt}")

    def _apply_kwargs(
        self,
        writer: DataFrameWriter,
        kwargs: tuple[tuple[str, Any], ...],
    ) -> DataFrameWriter:
        for key, value in kwargs:
            writer = writer.option(key, str(value))
        return writer


class SparkBackend:
    """Compute backend for Apache Spark.

    Unified entry point for all Spark I/O operations.
    """

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark
        self.read = SparkReadOps(spark)
        self.schema = SparkSchemaOps(spark)
        self.write = SparkWriteOps(spark, self.schema)
