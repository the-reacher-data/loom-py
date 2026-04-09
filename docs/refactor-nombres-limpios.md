# Refactorización: Nombres Limpios + Protocolos Divididos

## 🎯 Principio

**Sin prefijos redundantes:** `write_append()` → `append()`, `read_table()` → `table()`

Los protocolos definen la interfaz dividida, pero la implementación es una clase unificada con nombres limpios.

## 📦 Protocolos (Interfaces)

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


@runtime_checkable
class ReadOperations(Protocol, Generic[FrameT]):
    """Operaciones de lectura."""

    def table(self, target: ResolvedTarget) -> FrameT:
        """Lee tabla Delta."""
        ...

    def file(self, path: str, format: str, options: dict | None) -> FrameT:
        """Lee archivo."""
        ...

    def filter(self, frame: FrameT, predicates: tuple, params: Any) -> FrameT:
        """Aplica filtros."""
        ...


@runtime_checkable
class WriteOperations(Protocol, Generic[FrameT, SchemaT]):
    """Operaciones de escritura."""

    def append(self, frame: FrameT, target: ResolvedTarget, *, schema_mode: SchemaMode) -> None:
        ...

    def replace(self, frame: FrameT, target: ResolvedTarget, *, schema_mode: SchemaMode) -> None:
        ...

    def replace_partitions(self, frame: FrameT, target: ResolvedTarget, *,
                           partition_cols: tuple[str, ...], schema_mode: SchemaMode) -> None:
        ...

    def replace_where(self, frame: FrameT, target: ResolvedTarget, *,
                      predicate: str, schema_mode: SchemaMode) -> None:
        ...

    def upsert(self, frame: FrameT, target: ResolvedTarget, *,
               keys: tuple[str, ...], partition_cols: tuple[str, ...],
               schema_mode: SchemaMode, existing_schema: SchemaT | None) -> None:
        ...


@runtime_checkable
class SchemaOperations(Protocol, Generic[FrameT, SchemaT]):
    """Operaciones de schema y metadata."""

    def exists(self, ref: TableRef) -> bool:
        ...

    def columns(self, ref: TableRef) -> tuple[str, ...]:
        ...

    def schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        """Lee schema completo."""
        ...

    def physical_schema(self, target: ResolvedTarget) -> SchemaT | None:
        """Lee schema físico para validación."""
        ...

    def align(self, frame: FrameT, existing_schema: SchemaT | None, mode: SchemaMode) -> FrameT:
        """Alinea schema."""
        ...

    def materialize(self, frame: FrameT, streaming: bool) -> FrameT:
        """Materializa si es necesario."""
        ...

    def to_sql(self, predicate: Any, params: Any) -> str:
        """Convierte predicado a SQL."""
        ...


@runtime_checkable
class ComputeBackend(ReadOperations[FrameT], WriteOperations[FrameT, SchemaT],
                     SchemaOperations[FrameT, SchemaT], Protocol, Generic[FrameT, SchemaT]):
    """Protocolo completo."""
    pass
```

## 🔥 SparkBackend (Implementación Unificada, Nombres Limpios)

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
from loom.etl.storage.route.model import CatalogTarget, ResolvedTarget
from loom.etl.storage.schema.model import SparkPhysicalSchema


class SparkBackend:
    """
    Backend de computación para Apache Spark.

    Implementa ComputeBackend[DataFrame, SparkPhysicalSchema]
    con nombres limpios (sin prefijos write_/read_).
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    # ========================================================================
    # READ OPERATIONS (sin prefijo read_)
    # ========================================================================

    def table(self, target: ResolvedTarget) -> DataFrame:
        """Lee tabla Delta."""
        if isinstance(target, CatalogTarget):
            return self._spark.table(target.catalog_ref.ref)
        return self._spark.read.format("delta").load(target.location.uri)

    def file(self, path: str, format: str, options: dict | None) -> DataFrame:
        """Lee archivo."""
        reader = self._spark.read.format(format)
        if options:
            for k, v in options.items():
                reader = reader.option(k, v)
        return reader.load(path)

    def filter(self, frame: DataFrame, predicates: tuple, params: Any) -> DataFrame:
        """Aplica filtros."""
        from loom.etl.sql._predicate_sql import predicate_to_sql
        for pred in predicates:
            sql = predicate_to_sql(pred, params)
            frame = frame.filter(sql)
        return frame

    # ========================================================================
    # SCHEMA OPERATIONS (nombres cortos)
    # ========================================================================

    def exists(self, ref: TableRef) -> bool:
        """Verifica si tabla existe."""
        return bool(self._spark.catalog.tableExists(ref.ref))

    def columns(self, ref: TableRef) -> tuple[str, ...]:
        """Lista columnas."""
        if not self.exists(ref):
            return ()
        return tuple(self._spark.table(ref.ref).columns)

    def schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        """Lee schema completo."""
        if not self.exists(ref):
            return None
        fields = self._spark.table(ref.ref).schema.fields
        return tuple(
            ColumnSchema(name=f.name, dtype=self._to_loom(f.dataType), nullable=f.nullable)
            for f in fields
        )

    def physical_schema(self, target: ResolvedTarget) -> SparkPhysicalSchema | None:
        """Lee schema físico."""
        if isinstance(target, CatalogTarget):
            if not self.exists(target.catalog_ref):
                return None
            fields = self._spark.table(target.catalog_ref.ref).schema.fields
            return SparkPhysicalSchema(schema=T.StructType([f for f in fields]))
        else:
            try:
                dt = DeltaTable.forPath(self._spark, target.location.uri)
                return SparkPhysicalSchema(schema=dt.toDF().schema)
            except Exception:
                return None

    def align(self, frame: DataFrame, existing_schema: SparkPhysicalSchema | None,
              mode: SchemaMode) -> DataFrame:
        """Alinea schema."""
        if existing_schema is None or mode is SchemaMode.OVERWRITE:
            return frame

        schema = existing_schema.schema
        frame_cols = set(frame.columns)

        for field in schema.fields:
            if field.name in frame_cols:
                frame = frame.withColumn(field.name, self._cast(F.col(field.name), field.dataType))
            else:
                frame = frame.withColumn(field.name, F.lit(None).cast(field.dataType))

        if mode is SchemaMode.STRICT:
            frame = frame.select([f.name for f in schema.fields])

        return frame

    def materialize(self, frame: DataFrame, streaming: bool) -> DataFrame:
        """Spark ya es eager."""
        return frame

    def to_sql(self, predicate: Any, params: Any) -> str:
        """Convierte a SQL."""
        from loom.etl.sql._predicate_sql import predicate_to_sql
        return predicate_to_sql(predicate, params)

    # ========================================================================
    # WRITE OPERATIONS (sin prefijo write_)
    # ========================================================================

    def append(self, frame: DataFrame, target: ResolvedTarget, *, schema_mode: SchemaMode) -> None:
        """Operación APPEND."""
        writer = frame.write.format("delta").option("optimizeWrite", "true").mode("append")
        if schema_mode is SchemaMode.EVOLVE:
            writer = writer.option("mergeSchema", "true")
        self._sink(writer, target)

    def replace(self, frame: DataFrame, target: ResolvedTarget, *, schema_mode: SchemaMode) -> None:
        """Operación REPLACE."""
        writer = frame.write.format("delta").option("optimizeWrite", "true").mode("overwrite")
        if schema_mode is SchemaMode.OVERWRITE:
            writer = writer.option("overwriteSchema", "true")
        self._sink(writer, target)

    def replace_partitions(self, frame: DataFrame, target: ResolvedTarget, *,
                           partition_cols: tuple[str, ...], schema_mode: SchemaMode) -> None:
        """REPLACE PARTITIONS."""
        rows = frame.select(*partition_cols).distinct().collect()
        if not rows:
            return

        predicates = [f"({' AND '.join(f'{c} = {repr(row[c])}' for c in partition_cols)})"
                     for row in rows]
        predicate = " OR ".join(predicates)

        writer = (frame.sortWithinPartitions(*partition_cols)
                 .write.format("delta")
                 .option("optimizeWrite", "true")
                 .mode("overwrite")
                 .option("replaceWhere", predicate))
        self._sink(writer, target)

    def replace_where(self, frame: DataFrame, target: ResolvedTarget, *,
                      predicate: str, schema_mode: SchemaMode) -> None:
        """REPLACE WHERE."""
        writer = (frame.write.format("delta")
                 .option("optimizeWrite", "true")
                 .mode("overwrite")
                 .option("replaceWhere", predicate))
        self._sink(writer, target)

    def upsert(self, frame: DataFrame, target: ResolvedTarget, *,
               keys: tuple[str, ...], partition_cols: tuple[str, ...],
               schema_mode: SchemaMode, existing_schema: SparkPhysicalSchema | None) -> None:
        """UPSERT/MERGE."""
        # CREATE (first run)
        if existing_schema is None and schema_mode is SchemaMode.OVERWRITE:
            writer = (frame.write.format("delta")
                     .option("optimizeWrite", "true")
                     .mode("overwrite")
                     .option("overwriteSchema", "true"))
            self._sink(writer, target)
            return

        # MERGE
        aligned = self.align(frame, existing_schema, schema_mode)

        if isinstance(target, CatalogTarget):
            dt = DeltaTable.forName(self._spark, target.catalog_ref.ref)
        else:
            dt = DeltaTable.forPath(self._spark, target.location.uri)

        all_keys = list(keys) + list(partition_cols)
        merge_pred = " AND ".join(f"target.{k} = source.{k}" for k in all_keys)
        update_cols = [c for c in aligned.columns if c not in keys]

        (dt.alias("target")
            .merge(aligned.alias("source"), merge_pred)
            .whenMatchedUpdate(set={c: F.col(f"source.{c}") for c in update_cols})
            .whenNotMatchedInsert(values={c: F.col(f"source.{c}") for c in aligned.columns})
            .execute())

    # ========================================================================
    # HELPERS
    # ========================================================================

    def _cast(self, col: F.Column, dtype: T.DataType) -> F.Column:
        if isinstance(dtype, T.StructType):
            return F.struct([self._cast(F.col(f"{col}.{f.name}"), f.dataType).alias(f.name)
                           for f in dtype.fields])
        return col.cast(dtype)

    def _sink(self, writer, target: ResolvedTarget) -> None:
        if isinstance(target, CatalogTarget):
            writer.saveAsTable(target.catalog_ref.ref)
        else:
            writer.save(target.location.uri)

    def _to_loom(self, dtype: T.DataType):
        from loom.etl.backends.spark._dtype import spark_to_loom
        return spark_to_loom(dtype)
```

## 📊 Comparación de Nombres

| Antes (con prefijo) | Después (limpio) | Operación |
|---------------------|------------------|-----------|
| `read_table()` | `table()` | Lectura |
| `read_file()` | `file()` | Lectura |
| `apply_filters()` | `filter()` | Lectura |
| `write_append()` | `append()` | Escritura |
| `write_replace()` | `replace()` | Escritura |
| `write_upsert()` | `upsert()` | Escritura |
| `table_exists()` | `exists()` | Schema |
| `table_columns()` | `columns()` | Schema |
| `table_schema()` | `schema()` | Schema |
| `align_schema()` | `align()` | Schema |
| `materialize_if_needed()` | `materialize()` | Schema |
| `predicate_to_sql()` | `to_sql()` | Schema |

## ✅ Ventajas

1. **Nombres limpios**: Sin redundancia (contexto está claro)
2. **Protocolos divididos**: Organización lógica
3. **Implementación unificada**: Una clase, métodos cortos
4. **Type safety**: Generic[FrameT, SchemaT] funciona
5. **~15 métodos por clase**: Tamaño manejable
