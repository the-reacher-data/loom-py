# Refactorización con Protocolos y División

## 🎯 Estructura con Protocolos

```
loom/etl/backends/
├── __init__.py
├── _protocols.py           # 3 Protocolos divididos
├── _mixins.py              # Helpers compartidos (opcional)
├── spark.py                # SparkBackend (implementa 3 protocolos)
└── polars.py               # PolarsBackend (implementa 3 protocolos)
```

## 📦 Protocolos Divididos

```python
# loom/etl/backends/_protocols.py
from typing import Protocol, TypeVar, Generic, Any, runtime_checkable

from loom.etl.io.target import SchemaMode
from loom.etl.schema._schema import ColumnSchema
from loom.etl.schema._table import TableRef
from loom.etl.storage.route.model import ResolvedTarget
from loom.etl.storage.schema.model import PhysicalSchema

FrameT = TypeVar("FrameT")
SchemaT = TypeVar("SchemaT", bound=PhysicalSchema)


# ============================================================================
# PROTOCOLO 1: Operaciones de Lectura
# ============================================================================

@runtime_checkable
class ReadOperations(Protocol, Generic[FrameT]):
    """Protocolo para lectura de datos."""

    def read_table(self, target: ResolvedTarget) -> FrameT:
        """Lee tabla Delta (path o catalog)."""
        ...

    def read_file(self, path: str, format: str, options: dict | None) -> FrameT:
        """Lee archivo (CSV, JSON, Parquet)."""
        ...

    def apply_filters(
        self,
        frame: FrameT,
        predicates: tuple,
        params: Any
    ) -> FrameT:
        """Aplica filtros/predicados al frame."""
        ...


# ============================================================================
# PROTOCOLO 2: Operaciones de Escritura
# ============================================================================

@runtime_checkable
class WriteOperations(Protocol, Generic[FrameT, SchemaT]):
    """Protocolo para escritura de datos."""

    def write_append(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode
    ) -> None:
        """Operación APPEND."""
        ...

    def write_replace(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode
    ) -> None:
        """Operación REPLACE (overwrite)."""
        ...

    def write_replace_partitions(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
    ) -> None:
        """Operación REPLACE PARTITIONS."""
        ...

    def write_replace_where(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        predicate: str,
        schema_mode: SchemaMode,
    ) -> None:
        """Operación REPLACE WHERE."""
        ...

    def write_upsert(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        keys: tuple[str, ...],
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
        existing_schema: SchemaT | None,
    ) -> None:
        """Operación UPSERT/MERGE."""
        ...


# ============================================================================
# PROTOCOLO 3: Operaciones de Schema
# ============================================================================

@runtime_checkable
class SchemaOperations(Protocol, Generic[FrameT, SchemaT]):
    """Protocolo para gestión de schema y metadatos."""

    # Descubrimiento
    def table_exists(self, ref: TableRef) -> bool:
        """Verifica si tabla existe."""
        ...

    def table_columns(self, ref: TableRef) -> tuple[str, ...]:
        """Lista columnas de tabla."""
        ...

    def table_schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        """Lee schema completo de tabla."""
        ...

    def read_physical_schema(
        self,
        target: ResolvedTarget
    ) -> SchemaT | None:
        """Lee schema físico para validación."""
        ...

    # Transformación
    def align_schema(
        self,
        frame: FrameT,
        existing_schema: SchemaT | None,
        mode: SchemaMode
    ) -> FrameT:
        """Alinea schema del frame con existente (STRICT/EVOLVE)."""
        ...

    def materialize_if_needed(
        self,
        frame: FrameT,
        streaming: bool
    ) -> FrameT:
        """Materializa frame si es necesario (Polars lazy -> eager)."""
        ...

    def predicate_to_sql(self, predicate: Any, params: Any) -> str:
        """Convierte predicado a SQL string."""
        ...

    # Helper común
    def _is_first_run(
        self,
        existing_schema: SchemaT | None,
        mode: SchemaMode
    ) -> bool:
        """Determina si es primera escritura."""
        return existing_schema is None and mode is SchemaMode.OVERWRITE


# ============================================================================
# PROTOCOLO UNIFICADO: ComputeBackend
# ============================================================================

@runtime_checkable
class ComputeBackend(
    ReadOperations[FrameT],
    WriteOperations[FrameT, SchemaT],
    SchemaOperations[FrameT, SchemaT],
    Protocol,
    Generic[FrameT, SchemaT],
):
    """
    Protocolo completo para backend de computación ETL.

    Implementaciones:
    - SparkBackend: Para Apache Spark
    - PolarsBackend: Para Polars + delta-rs
    """
    pass
```

## 🔥 Implementación: SparkBackend

```python
# loom/etl/backends/spark.py
from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from delta.tables import DeltaTable

from loom.etl.backends._protocols import ComputeBackend
from loom.etl.io.target import SchemaMode
from loom.etl.schema._schema import ColumnSchema
from loom.etl.schema._table import TableRef
from loom.etl.storage.route.model import CatalogTarget, PathTarget, ResolvedTarget
from loom.etl.storage.schema.model import SparkPhysicalSchema


class SparkBackend:
    """
    Backend de computación para Apache Spark.

    Implementa ComputeBackend[DataFrame, SparkPhysicalSchema].
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    # ========================================================================
    # READ OPERATIONS
    # ========================================================================

    def read_table(self, target: ResolvedTarget) -> DataFrame:
        if isinstance(target, CatalogTarget):
            return self._spark.table(target.catalog_ref.ref)
        return self._spark.read.format("delta").load(target.location.uri)

    def read_file(self, path: str, format: str, options: dict | None) -> DataFrame:
        reader = self._spark.read.format(format)
        if options:
            for k, v in options.items():
                reader = reader.option(k, v)
        return reader.load(path)

    def apply_filters(self, frame: DataFrame, predicates: tuple, params: Any) -> DataFrame:
        from loom.etl.sql._predicate_sql import predicate_to_sql
        for pred in predicates:
            sql = predicate_to_sql(pred, params)
            frame = frame.filter(sql)
        return frame

    # ========================================================================
    # SCHEMA OPERATIONS
    # ========================================================================

    def table_exists(self, ref: TableRef) -> bool:
        return bool(self._spark.catalog.tableExists(ref.ref))

    def table_columns(self, ref: TableRef) -> tuple[str, ...]:
        if not self.table_exists(ref):
            return ()
        return tuple(self._spark.table(ref.ref).columns)

    def table_schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        if not self.table_exists(ref):
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

    def read_physical_schema(self, target: ResolvedTarget) -> SparkPhysicalSchema | None:
        if isinstance(target, CatalogTarget):
            if not self.table_exists(target.catalog_ref):
                return None
            fields = self._spark.table(target.catalog_ref.ref).schema.fields
            return SparkPhysicalSchema(schema=T.StructType([f for f in fields]))
        else:
            try:
                dt = DeltaTable.forPath(self._spark, target.location.uri)
                return SparkPhysicalSchema(schema=dt.toDF().schema)
            except Exception:
                return None

    def align_schema(
        self,
        frame: DataFrame,
        existing_schema: SparkPhysicalSchema | None,
        mode: SchemaMode
    ) -> DataFrame:
        if existing_schema is None or mode is SchemaMode.OVERWRITE:
            return frame

        schema = existing_schema.schema
        frame_cols = set(frame.columns)

        for field in schema.fields:
            if field.name in frame_cols:
                frame = frame.withColumn(
                    field.name, self._cast(F.col(field.name), field.dataType)
                )
            else:
                frame = frame.withColumn(
                    field.name, F.lit(None).cast(field.dataType)
                )

        if mode is SchemaMode.STRICT:
            frame = frame.select([f.name for f in schema.fields])

        return frame

    def materialize_if_needed(self, frame: DataFrame, streaming: bool) -> DataFrame:
        return frame  # Spark ya es eager

    def predicate_to_sql(self, predicate: Any, params: Any) -> str:
        from loom.etl.sql._predicate_sql import predicate_to_sql
        return predicate_to_sql(predicate, params)

    # ========================================================================
    # WRITE OPERATIONS
    # ========================================================================

    def write_append(
        self, frame: DataFrame, target: ResolvedTarget, *, schema_mode: SchemaMode
    ) -> None:
        writer = frame.write.format("delta").option("optimizeWrite", "true").mode("append")
        if schema_mode is SchemaMode.EVOLVE:
            writer = writer.option("mergeSchema", "true")
        self._sink(writer, target)

    def write_replace(
        self, frame: DataFrame, target: ResolvedTarget, *, schema_mode: SchemaMode
    ) -> None:
        writer = frame.write.format("delta").option("optimizeWrite", "true").mode("overwrite")
        if schema_mode is SchemaMode.OVERWRITE:
            writer = writer.option("overwriteSchema", "true")
        self._sink(writer, target)

    def write_replace_partitions(
        self, frame: DataFrame, target: ResolvedTarget, *, partition_cols: tuple[str, ...], schema_mode: SchemaMode
    ) -> None:
        rows = frame.select(*partition_cols).distinct().collect()
        if not rows:
            return

        predicates = [f"({' AND '.join(f'{c} = {repr(row[c])}' for c in partition_cols)})" for row in rows]
        predicate = " OR ".join(predicates)

        writer = (
            frame.sortWithinPartitions(*partition_cols)
            .write.format("delta")
            .option("optimizeWrite", "true")
            .mode("overwrite")
            .option("replaceWhere", predicate)
        )
        self._sink(writer, target)

    def write_replace_where(
        self, frame: DataFrame, target: ResolvedTarget, *, predicate: str, schema_mode: SchemaMode
    ) -> None:
        writer = (
            frame.write.format("delta")
            .option("optimizeWrite", "true")
            .mode("overwrite")
            .option("replaceWhere", predicate)
        )
        self._sink(writer, target)

    def write_upsert(
        self, frame: DataFrame, target: ResolvedTarget, *, keys: tuple[str, ...],
        partition_cols: tuple[str, ...], schema_mode: SchemaMode,
        existing_schema: SparkPhysicalSchema | None
    ) -> None:
        # CREATE (first run)
        if self._is_first_run(existing_schema, schema_mode):
            writer = (
                frame.write.format("delta")
                .option("optimizeWrite", "true")
                .mode("overwrite")
                .option("overwriteSchema", "true")
            )
            self._sink(writer, target)
            return

        # MERGE
        aligned = self.align_schema(frame, existing_schema, schema_mode)

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

    # ========================================================================
    # HELPERS
    # ========================================================================

    def _cast(self, col: F.Column, dtype: T.DataType) -> F.Column:
        if isinstance(dtype, T.StructType):
            return F.struct([
                self._cast(F.col(f"{col}.{f.name}"), f.dataType).alias(f.name)
                for f in dtype.fields
            ])
        return col.cast(dtype)

    def _sink(self, writer, target: ResolvedTarget) -> None:
        if isinstance(target, CatalogTarget):
            writer.saveAsTable(target.catalog_ref.ref)
        else:
            writer.save(target.location.uri)

    def _to_loom(self, dtype: T.DataType):
        from loom.etl.backends.spark._dtype import spark_to_loom
        return spark_to_loom(dtype)

    def _is_first_run(self, existing_schema: SparkPhysicalSchema | None, mode: SchemaMode) -> bool:
        return existing_schema is None and mode is SchemaMode.OVERWRITE
```

## 🐻 Implementación: PolarsBackend

```python
# loom/etl/backends/polars.py
from __future__ import annotations

import logging
from typing import Any

import polars as pl
from deltalake import DeltaTable, DataCatalog, write_deltalake

from loom.etl.backends._protocols import ComputeBackend
from loom.etl.io.target import SchemaMode
from loom.etl.schema._schema import ColumnSchema
from loom.etl.schema._table import TableRef
from loom.etl.storage._locator import TableLocator
from loom.etl.storage.route.model import CatalogTarget, PathTarget, ResolvedTarget
from loom.etl.storage.schema.model import PolarsPhysicalSchema

_log = logging.getLogger(__name__)


class PolarsBackend:
    """
    Backend de computación para Polars + delta-rs.

    Implementa ComputeBackend[pl.LazyFrame, PolarsPhysicalSchema].
    """

    def __init__(self, locator: TableLocator):
        self._locator = locator

    # ========================================================================
    # READ OPERATIONS
    # ========================================================================

    def read_table(self, target: ResolvedTarget) -> pl.LazyFrame:
        loc = self._resolve(target)
        return pl.scan_delta(loc.uri, storage_options=loc.storage_options)

    def read_file(self, path: str, format: str, options: dict | None) -> pl.LazyFrame:
        readers = {
            "csv": pl.scan_csv,
            "json": pl.scan_ndjson,
            "parquet": pl.scan_parquet,
        }
        reader = readers.get(format)
        if not reader:
            raise ValueError(f"Unsupported format: {format}")
        return reader(path, **(options or {}))

    def apply_filters(self, frame: pl.LazyFrame, predicates: tuple, params: Any) -> pl.LazyFrame:
        from loom.etl.backends.polars._predicate import predicate_to_polars
        for pred in predicates:
            expr = predicate_to_polars(pred, params)
            frame = frame.filter(expr)
        return frame

    # ========================================================================
    # SCHEMA OPERATIONS
    # ========================================================================

    def table_exists(self, ref: TableRef) -> bool:
        try:
            loc = self._locator.locate(ref)
            return DeltaTable.is_deltatable(loc.uri, loc.storage_options)
        except Exception:
            return False

    def table_columns(self, ref: TableRef) -> tuple[str, ...]:
        schema = self.table_schema(ref)
        return tuple(c.name for c in schema) if schema else ()

    def table_schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        try:
            loc = self._locator.locate(ref)

            if self._is_uc_uri(loc.uri):
                return self._read_schema_uc(ref)

            dt = DeltaTable(loc.uri, storage_options=loc.storage_options)
            return self._to_column_schema(dt.schema())
        except Exception:
            return None

    def read_physical_schema(self, target: ResolvedTarget) -> PolarsPhysicalSchema | None:
        try:
            loc = self._resolve(target)
            dt = DeltaTable(loc.uri, storage_options=loc.storage_options)
            arrow = dt.schema().to_pyarrow()
            return PolarsPhysicalSchema(
                schema=pl.Schema(zip(arrow.names, arrow.types))
            )
        except Exception:
            return None

    def align_schema(
        self, frame: pl.LazyFrame, existing_schema: PolarsPhysicalSchema | None, mode: SchemaMode
    ) -> pl.LazyFrame:
        if existing_schema is None or mode is SchemaMode.OVERWRITE:
            return frame
        from loom.etl.backends.polars._schema import apply_schema
        return apply_schema(frame, existing_schema.schema, mode)

    def materialize_if_needed(self, frame: pl.LazyFrame, streaming: bool) -> pl.DataFrame:
        return frame.collect(engine="streaming" if streaming else "auto")

    def predicate_to_sql(self, predicate: Any, params: Any) -> str:
        from loom.etl.sql._predicate_sql import predicate_to_sql
        return predicate_to_sql(predicate, params)

    # ========================================================================
    # WRITE OPERATIONS
    # ========================================================================

    def _warn_uc_write(self, target: ResolvedTarget, existing_schema) -> None:
        if existing_schema is not None:
            return
        try:
            loc = self._locator.locate(target.logical_ref)
            if self._is_uc_uri(loc.uri):
                _log.warning("Writing to UC path %s. Data written but catalog registration may need Spark.", loc.uri)
        except Exception:
            pass

    def write_append(self, frame: pl.DataFrame, target: ResolvedTarget, *, schema_mode: SchemaMode) -> None:
        loc = self._get_write_location(target)
        write_deltalake(loc.uri, frame.to_arrow(), mode="append", storage_options=loc.storage_options)

    def write_replace(self, frame: pl.DataFrame, target: ResolvedTarget, *, schema_mode: SchemaMode) -> None:
        loc = self._get_write_location(target)
        write_deltalake(loc.uri, frame.to_arrow(), mode="overwrite", storage_options=loc.storage_options)

    def write_replace_partitions(self, frame: pl.DataFrame, target: ResolvedTarget, *,
                                  partition_cols: tuple[str, ...], schema_mode: SchemaMode) -> None:
        if frame.is_empty():
            return
        loc = self._get_write_location(target)
        from loom.etl.sql._upsert import _build_partition_predicate
        rows = frame.select(list(partition_cols)).unique().to_dicts()
        predicate = _build_partition_predicate(rows, partition_cols)
        write_deltalake(loc.uri, frame.to_arrow(), mode="overwrite", predicate=predicate, storage_options=loc.storage_options)

    def write_replace_where(self, frame: pl.DataFrame, target: ResolvedTarget, *,
                            predicate: str, schema_mode: SchemaMode) -> None:
        loc = self._get_write_location(target)
        write_deltalake(loc.uri, frame.to_arrow(), mode="overwrite", predicate=predicate, storage_options=loc.storage_options)

    def write_upsert(self, frame: pl.DataFrame, target: ResolvedTarget, *, keys: tuple[str, ...],
                     partition_cols: tuple[str, ...], schema_mode: SchemaMode,
                     existing_schema: PolarsPhysicalSchema | None) -> None:
        self._warn_uc_write(target, existing_schema)
        loc = self._get_write_location(target)

        if self._is_first_run(existing_schema, schema_mode):
            write_deltalake(loc.uri, frame.to_arrow(), mode="overwrite", storage_options=loc.storage_options)
            return

        aligned = self.align_schema(pl.LazyFrame(frame), existing_schema, schema_mode).collect()
        dt = DeltaTable(loc.uri, storage_options=loc.storage_options)

        all_keys = list(keys) + list(partition_cols)
        predicate = " AND ".join(f"target.{k} = source.{k}" for k in all_keys)
        update_cols = [c for c in aligned.columns if c not in keys]

        dt.merge(source=aligned.to_pandas(), predicate=predicate,
                 source_alias="source", target_alias="target")\
          .when_matched_update(updates={c: f"source.{c}" for c in update_cols})\
          .when_not_matched_insert(updates={c: f"source.{c}" for c in aligned.columns})\
          .execute()

    # ========================================================================
    # HELPERS
    # ========================================================================

    def _resolve(self, target: ResolvedTarget):
        if isinstance(target, CatalogTarget):
            return self._resolve_uc(target.catalog_ref)
        return self._locator.locate(target.logical_ref)

    def _resolve_uc(self, ref: TableRef):
        parts = ref.ref.split(".")
        catalog, schema, table = parts
        dt = DeltaTable.from_data_catalog(
            data_catalog=DataCatalog.UNITY,
            data_catalog_id=catalog,
            database_name=schema,
            table_name=table,
        )
        from loom.etl.storage._locator import TableLocation
        return TableLocation(uri=dt.table_uri, storage_options=dt.storage_options)

    def _is_uc_uri(self, uri: str) -> bool:
        return uri.startswith("uc://") or "databricks" in uri

    def _get_write_location(self, target: ResolvedTarget):
        if isinstance(target, CatalogTarget):
            raise ValueError("Polars requires path-based targets for writes")
        return self._locator.locate(target.logical_ref)

    def _read_schema_uc(self, ref: TableRef):
        parts = ref.ref.split(".")
        catalog, schema, table = parts
        dt = DeltaTable.from_data_catalog(
            data_catalog=DataCatalog.UNITY,
            data_catalog_id=catalog,
            database_name=schema,
            table_name=table,
        )
        return self._to_column_schema(dt.schema())

    def _to_column_schema(self, delta_schema) -> tuple[ColumnSchema, ...]:
        from loom.etl.backends.polars._dtype import polars_to_loom_type
        arrow = delta_schema.to_pyarrow()
        return tuple(
            ColumnSchema(name=n, dtype=polars_to_loom_type(pl.from_arrow_type(t)), nullable=True)
            for n, t in zip(arrow.names, arrow.types)
        )

    def _is_first_run(self, existing_schema: PolarsPhysicalSchema | None, mode: SchemaMode) -> bool:
        return existing_schema is None and mode is SchemaMode.OVERWRITE
```

## 📊 Ventajas de Protocolos

| Aspecto | Beneficio |
|---------|-----------|
| **División** | 3 protocolos pequeños (~4-6 métodos cada uno) |
| **Composición** | ComputeBackend = Read + Write + Schema |
| **Type Safety** | Generic[FrameT, SchemaT] funciona mejor con Protocol |
| **Sin Herencia Forzada** | Las clases "implementan", no "heredan" |
| **Runtime Check** | `@runtime_checkable` permite isinstance() |
| **Flexibilidad** | Se puede implementar parcialmente si es necesario |

## 🗑️ Cleanup Final

**Eliminados:** 14 ficheros (~1,200 líneas)
**Nuevos:** 4 ficheros (~900 líneas)
**Neto:** -300 líneas, arquitectura más limpia
