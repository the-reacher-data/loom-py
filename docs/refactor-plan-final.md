# Plan Final de Refactorización ETL (Con Feedback)

## ✅ Hallazgos Aceptados

### 1. Mantener WritePlanner, SchemaReader, TableDiscovery
**Decisión:** NO eliminar estos componentes. Están bien diseñados y separan responsabilidades correctamente.

```python
# SE MANTIENEN:
- WritePlanner: Separa planning (route + schema) de ejecución
- SchemaReader: Abstrae lectura de schema (Spark vs Polars son diferentes)
- TableDiscovery: Compile-time vs runtime distinction es válida
```

### 2. CommonWriteExecutor en storage/write/
**Corrección:** NO crear `execution/` nueva. Mantener en `storage/write/`.

```
loom/etl/storage/write/
├── __init__.py
├── exec.py              # WriteExecutor (único, genérico) ← ANTES: protocolo vacío
├── plan.py              # SE MANTIENE: WritePlanner
├── ops.py               # SE MANTIENE: WriteOperation dataclasses
└── generic.py           # ELIMINAR: GenericTargetWriter (adapter sin valor)
```

### 3. apply_schema es método nativo
**Corrección:** NO poner `validate_schema` en executor. Cada backend tiene sus tipos.

```python
class NativeOperations(Protocol, Generic[FrameT, SchemaT]):
    """
    SchemaT = SparkPhysicalSchema para Spark,
    SchemaT = PolarsPhysicalSchema para Polars
    """

    # Cada backend implementa con sus tipos concretos
    def align_schema(
        self,
        frame: FrameT,
        existing_schema: SchemaT | None,
        mode: SchemaMode
    ) -> FrameT: ...

    # Spark usa: spark_apply_schema() con T.StructType
    # Polars usa: apply_schema() con pl.Schema
```

### 4. Eliminar adapters pasarela
**Confirmado:** Eliminar `SparkDeltaTableWriter` y `PolarsDeltaTableWriter`.

```python
# ELIMINAR: src/loom/etl/backends/spark/writer/table.py (94 líneas)
# ELIMINAR: src/loom/etl/backends/polars/writer/table.py (90 líneas)

# REEMPLAZAR por: SparkTargetWriter que use WriteExecutor directamente
```

### 5. Resolver duplicación en factory
**Pendiente:** Analizar `src/loom/etl/storage/_factory.py`.

---

## 🔍 Análisis de storage/_factory.py

```python
# src/loom/etl/storage/_factory.py
class StorageConfig:
    """Factory con demasiada lógica de routing duplicada."""

    def create_reader(self) -> SourceReader:
        if self.backend == "spark":
            return SparkSourceReader(self.spark, self.locator)
        elif self.backend == "polars":
            return PolarsSourceReader(self.locator)

    def create_writer(self) -> TargetWriter:
        if self.backend == "spark":
            return SparkTargetWriter(self.spark, self.locator)  # ← Duplicación
        elif self.backend == "polars":
            return PolarsTargetWriter(self.locator)             # ← Duplicación
```

**Problema:** Factory decide backend específico, pero luego el backend decide otra vez.

**Solución:** Factory crea el "bridge" correcto, o eliminar factory y dejar que usuario cree directamente.

---

## 🏗️ Arquitectura Final

### Estructura de Ficheros

```
loom/etl/
├── backends/
│   ├── spark/
│   │   ├── __init__.py          # Exports compatibles
│   │   ├── _catalog.py          # SE MANTIENE
│   │   ├── _dtype.py            # SE MANTIENE
│   │   ├── _reader.py           # REFACTOR (usa align_schema nativo)
│   │   ├── _schema.py           # SE MANTIENE
│   │   ├── _native.py           # NUEVO: SparkNativeOperations
│   │   └── _writer.py           # REFACTOR: SparkTargetWriter simplificado
│   └── polars/
│       ├── __init__.py          # Exports compatibles
│       ├── _catalog.py          # SE MANTIENE
│       ├── _dtype.py            # SE MANTIENE
│       ├── _reader.py           # REFACTOR (usa align_schema nativo)
│       ├── _schema.py           # SE MANTIENE
│       ├── _native.py           # NUEVO: PolarsNativeOperations
│       └── _writer.py           # REFACTOR: PolarsTargetWriter simplificado
├── storage/
│   ├── _factory.py              # REFACTOR: Eliminar duplicación o simplificar
│   ├── _locator.py              # SE MANTIENE
│   ├── route/                   # SE MANTIENE
│   ├── schema/                  # SE MANTIENE
│   └── write/
│       ├── __init__.py
│       ├── exec.py              # WriteExecutor (único genérico)
│       ├── plan.py              # SE MANTIENE: WritePlanner
│       └── ops.py               # SE MANTIENE: WriteOperation dataclasses
└── ...
```

### Ficheros ELIMINADOS (6 ficheros, -800 líneas)

| Fichero | Líneas | Razón |
|---------|--------|-------|
| `spark/writer/table.py` | 94 | Adapter passthrough |
| `polars/writer/table.py` | 90 | Adapter passthrough |
| `spark/writer/exec.py` | 313 | Duplicado, unificar en storage/write/exec.py |
| `polars/writer/exec.py` | 279 | Duplicado, unificar en storage/write/exec.py |
| `storage/write/generic.py` | 58 | Adapter sin valor |
| `spark/writer/core.py` | 115 | Lógica mover a _writer.py simplificado |
| `polars/writer/core.py` | 107 | Lógica mover a _writer.py simplificado |

---

## 📝 Implementación Detallada

### 1. NativeOperations Protocol (Tipado Fuerte)

```python
# loom/etl/backends/_native.py
from typing import Protocol, TypeVar, Generic

FrameT = TypeVar("FrameT")
SchemaT = TypeVar("SchemaT")

class NativeOperations(Protocol, Generic[FrameT, SchemaT]):
    """
    Operaciones nativas específicas de cada backend.

    Type parameters:
        FrameT: DataFrame (Spark) o LazyFrame/DataFrame (Polars)
        SchemaT: SparkPhysicalSchema o PolarsPhysicalSchema
    """

    # ---------- Schema Alignment (específico por backend) ----------
    def align_schema(
        self,
        frame: FrameT,
        existing_schema: SchemaT | None,
        mode: SchemaMode,
    ) -> FrameT:
        """
        Alinea schema del frame con schema existente.

        STRICT: Reordena columnas, añade nulls para missing
        EVOLVE: Añade nuevas columnas, mantiene existentes
        OVERWRITE: Devuelve frame sin cambios
        """
        ...

    # ---------- Materialization (solo Polars necesita esto) ----------
    def materialize_if_needed(
        self,
        frame: FrameT,
        streaming: bool
    ) -> FrameT:
        """
        Polars: LazyFrame -> DataFrame (collect)
        Spark: DataFrame -> DataFrame (noop, Spark maneja su streaming)
        """
        ...

    # ---------- Predicates ----------
    def predicate_to_sql(self, predicate: PredicateNode, params: Any) -> str:
        """Convierte predicado a SQL para ReplaceWhere."""
        ...

    # ---------- Write Operations ----------
    def append(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Ejecuta operación de append nativa."""
        ...

    def replace(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Ejecuta operación de replace nativa."""
        ...

    def replace_partitions(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
    ) -> None:
        """Ejecuta operación de replace partitions nativa."""
        ...

    def replace_where(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        predicate: str,  # SQL string
        schema_mode: SchemaMode,
    ) -> None:
        """Ejecuta operación de replace where nativa."""
        ...

    def upsert(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        keys: tuple[str, ...],
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
        existing_schema: SchemaT | None,  # Backend decide CREATE vs MERGE
    ) -> None:
        """
        Ejecuta upsert/merge nativo.

        El backend decide:
        - Si existing_schema is None: CREATE (first run)
        - Si existe: MERGE con DeltaTable API
        """
        ...
```

### 2. WriteExecutor Unificado

```python
# loom/etl/storage/write/exec.py
from typing import Any, Generic, TypeVar

from loom.etl.io.target import SchemaMode
from loom.etl.storage.write.ops import (
    AppendOp, ReplaceOp, ReplacePartitionsOp, ReplaceWhereOp, UpsertOp,
)

FrameT = TypeVar("FrameT")
SchemaT = TypeVar("SchemaT")


class WriteExecutor(Generic[FrameT, SchemaT]):
    """
    Executor unificado de operaciones de escritura.

    Orquesta el flujo común:
    1. Alinea schema (delega a native.align_schema)
    2. Materializa si es necesario (solo Polars)
    3. Ejecuta operación nativa específica

    El backend decide detalles finos (ej: first-run en upsert).
    """

    def __init__(self, native: NativeOperations[FrameT, SchemaT]):
        self._native = native

    def execute(
        self,
        frame: FrameT,
        op: WriteOperation,
        params: Any,
    ) -> None:
        """Ejecuta operación de escritura."""
        match op:
            case AppendOp():
                self._exec_append(frame, op)
            case ReplaceOp():
                self._exec_replace(frame, op)
            case ReplacePartitionsOp():
                self._exec_replace_partitions(frame, op)
            case ReplaceWhereOp():
                self._exec_replace_where(frame, op, params)
            case UpsertOp():
                self._exec_upsert(frame, op)
            case _:
                raise TypeError(f"Unsupported operation: {type(op)}")

    def _exec_append(self, frame: FrameT, op: AppendOp) -> None:
        """Append: Alinea schema y delega."""
        aligned = self._native.align_schema(
            frame, op.existing_schema, op.schema_mode
        )
        self._native.append(aligned, op.target, schema_mode=op.schema_mode)

    def _exec_replace(self, frame: FrameT, op: ReplaceOp) -> None:
        """Replace: Alinea schema y delega."""
        aligned = self._native.align_schema(
            frame, op.existing_schema, op.schema_mode
        )
        self._native.replace(aligned, op.target, schema_mode=op.schema_mode)

    def _exec_replace_partitions(self, frame: FrameT, op: ReplacePartitionsOp) -> None:
        """Replace partitions: Alinea schema y delega."""
        aligned = self._native.align_schema(
            frame, op.existing_schema, op.schema_mode
        )
        self._native.replace_partitions(
            aligned,
            op.target,
            partition_cols=op.partition_cols,
            schema_mode=op.schema_mode,
        )

    def _exec_replace_where(
        self,
        frame: FrameT,
        op: ReplaceWhereOp,
        params: Any
    ) -> None:
        """Replace where: Alinea schema, convierte predicado y delega."""
        aligned = self._native.align_schema(
            frame, op.existing_schema, op.schema_mode
        )
        predicate_sql = self._native.predicate_to_sql(
            op.replace_predicate, params
        )
        self._native.replace_where(
            aligned,
            op.target,
            predicate=predicate_sql,
            schema_mode=op.schema_mode,
        )

    def _exec_upsert(self, frame: FrameT, op: UpsertOp) -> None:
        """
        Upsert: Materializa y delega TODO al backend.

        El backend decide si CREATE (first run) o MERGE.
        """
        materialized = self._native.materialize_if_needed(
            frame, op.streaming
        )

        # Backend tiene toda la info para decidir CREATE vs MERGE
        self._native.upsert(
            materialized,
            op.target,
            keys=op.upsert_keys,
            partition_cols=op.partition_cols,
            schema_mode=op.schema_mode,
            existing_schema=op.existing_schema,
        )
```

### 3. SparkNativeOperations

```python
# loom/etl/backends/spark/_native.py
from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from delta.tables import DeltaTable  # Import al toplevel

from loom.etl.io.target import SchemaMode
from loom.etl.storage.route.model import CatalogTarget, ResolvedTarget
from loom.etl.storage.schema.model import SparkPhysicalSchema


class SparkNativeOperations:
    """
    Operaciones nativas de Spark.

    Depende de:
    - pyspark: Lectura, escritura, schema
    - deltalake (Python lib): Solo MERGE vía DeltaTable API
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    # ---------- Schema Alignment ----------
    def align_schema(
        self,
        frame: DataFrame,
        existing_schema: SparkPhysicalSchema | None,
        mode: SchemaMode,
    ) -> DataFrame:
        """Alinea schema usando PySpark nativo."""
        if existing_schema is None or mode is SchemaMode.OVERWRITE:
            return frame

        schema = existing_schema.schema
        frame_cols = set(frame.columns)

        # Aplicar casting
        for field in schema.fields:
            if field.name in frame_cols:
                frame = frame.withColumn(
                    field.name,
                    self._cast(F.col(field.name), field.dataType)
                )

        # Añadir columnas missing como null
        for field in schema.fields:
            if field.name not in frame_cols:
                frame = frame.withColumn(
                    field.name,
                    F.lit(None).cast(field.dataType)
                )

        # STRICT: Reordenar columnas
        if mode is SchemaMode.STRICT:
            frame = frame.select([f.name for f in schema.fields])

        return frame

    def _cast(self, col: F.Column, dtype: T.DataType) -> F.Column:
        """Cast recursivo para tipos complejos."""
        if isinstance(dtype, T.StructType):
            return F.struct([
                self._cast(F.col(f"{col}.{f.name}"), f.dataType).alias(f.name)
                for f in dtype.fields
            ])
        return col.cast(dtype)

    # ---------- Materialization ----------
    def materialize_if_needed(
        self,
        frame: DataFrame,
        streaming: bool
    ) -> DataFrame:
        """Spark DataFrames ya son eager."""
        return frame

    # ---------- Predicates ----------
    def predicate_to_sql(self, predicate: Any, params: Any) -> str:
        from loom.etl.sql._predicate_sql import predicate_to_sql
        return predicate_to_sql(predicate, params)

    # ---------- Write Operations ----------
    def append(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        writer = frame.write \
            .format("delta") \
            .option("optimizeWrite", "true") \
            .mode("append")

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
        writer = frame.write \
            .format("delta") \
            .option("optimizeWrite", "true") \
            .mode("overwrite")

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
        # Calcular replaceWhere predicate
        rows = frame.select(*partition_cols).distinct().collect()
        if not rows:
            return

        predicates = []
        for row in rows:
            row_pred = " AND ".join(
                f"{col} = '{row[col]}'"
                for col in partition_cols
            )
            predicates.append(f"({row_pred})")

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
        """
        Upsert: Decide internamente CREATE vs MERGE.
        """
        if existing_schema is None:
            # FIRST RUN: Crear tabla
            writer = (
                frame.write.format("delta")
                .option("optimizeWrite", "true")
                .mode("overwrite")
                .option("overwriteSchema", "true")
            )
            self._sink(writer, target)
            return

        # EXISTING TABLE: Hacer MERGE
        aligned = self.align_schema(frame, existing_schema, schema_mode)

        # Obtener DeltaTable handle
        if isinstance(target, CatalogTarget):
            dt = DeltaTable.forName(self._spark, target.catalog_ref.ref)
        else:
            dt = DeltaTable.forPath(self._spark, target.location.uri)

        # Construir predicado de merge
        all_keys = list(keys) + list(partition_cols)
        merge_pred = " AND ".join(f"target.{k} = source.{k}" for k in all_keys)

        # Columnas a actualizar (excluir keys)
        update_cols = [c for c in aligned.columns if c not in keys]

        # Ejecutar merge
        (
            dt.alias("target")
            .merge(aligned.alias("source"), merge_pred)
            .whenMatchedUpdate(set={
                c: F.col(f"source.{c}") for c in update_cols
            })
            .whenNotMatchedInsert(values={
                c: F.col(f"source.{c}") for c in aligned.columns
            })
            .execute()
        )

    def _sink(self, writer, target: ResolvedTarget) -> None:
        """Finaliza escritura según tipo de target."""
        if isinstance(target, CatalogTarget):
            writer.saveAsTable(target.catalog_ref.ref)
        else:
            writer.save(target.location.uri)
```

### 4. PolarsNativeOperations (con Warning UC)

```python
# loom/etl/backends/polars/_native.py
from __future__ import annotations

import logging
from typing import Any

import polars as pl
from deltalake import DeltaTable, write_deltalake  # Imports al toplevel

from loom.etl.io.target import SchemaMode
from loom.etl.storage._locator import TableLocator
from loom.etl.storage.route.model import CatalogTarget, PathTarget, ResolvedTarget
from loom.etl.storage.schema.model import PolarsPhysicalSchema

_log = logging.getLogger(__name__)


class PolarsNativeOperations:
    """
    Operaciones nativas de Polars + delta-rs.

    Depende de:
    - polars: DataFrame operations
    - deltalake (delta-rs): Todo (read, write, merge)
    """

    def __init__(self, locator: TableLocator):
        self._locator = locator

    # ---------- Schema Alignment ----------
    def align_schema(
        self,
        frame: pl.LazyFrame,
        existing_schema: PolarsPhysicalSchema | None,
        mode: SchemaMode,
    ) -> pl.LazyFrame:
        """Alinea schema usando Polars."""
        if existing_schema is None or mode is SchemaMode.OVERWRITE:
            return frame

        # Lógica de apply_schema de polars/_schema.py
        # ... casting de columnas ...
        return frame

    # ---------- Materialization ----------
    def materialize_if_needed(
        self,
        frame: pl.LazyFrame,
        streaming: bool
    ) -> pl.DataFrame:
        """Polars requiere materialización antes de MERGE."""
        return frame.collect(engine="streaming" if streaming else "auto")

    # ---------- Predicates ----------
    def predicate_to_sql(self, predicate: Any, params: Any) -> str:
        from loom.etl.sql._predicate_sql import predicate_to_sql
        return predicate_to_sql(predicate, params)

    # ---------- Write Operations ----------
    def _get_location(self, target: ResolvedTarget) -> TableLocation:
        """Obtiene ubicación física."""
        if isinstance(target, CatalogTarget):
            raise ValueError(
                "Polars backend does not support Unity Catalog writes directly. "
                "Use Spark backend for UC writes, or configure path-based storage."
            )
        return self._locator.locate(target.logical_ref)

    def _warn_if_uc_first_create(
        self,
        target: ResolvedTarget,
        existing_schema: PolarsPhysicalSchema | None
    ) -> None:
        """
        Warning de producto: Polars + UC first-create puede no registrar en catálogo.
        """
        if existing_schema is not None:
            return

        if not isinstance(target, PathTarget):
            return

        if not target.location.uri.lower().startswith("uc://"):
            return

        _log.warning(
            "Polars first-create for UC target '%s' at '%s'. "
            "Table data will be written but catalog registration is not guaranteed. "
            "Pre-create the table in UC or use Spark backend for guaranteed registration.",
            target.logical_ref.ref,
            target.location.uri,
        )

    def append(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        loc = self._get_location(target)
        write_deltalake(
            loc.uri,
            frame.to_arrow(),
            mode="append",
            storage_options=loc.storage_options,
        )

    def replace(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        loc = self._get_location(target)
        write_deltalake(
            loc.uri,
            frame.to_arrow(),
            mode="overwrite",
            storage_options=loc.storage_options,
        )

    def replace_partitions(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
    ) -> None:
        if frame.is_empty():
            _log.warning("replace_partitions: empty frame, nothing written")
            return

        loc = self._get_location(target)

        # Calcular predicate para replace partitions
        partition_df = frame.select(list(partition_cols)).unique()
        rows = partition_df.to_dicts()

        from loom.etl.sql._upsert import _build_partition_predicate
        predicate = _build_partition_predicate(rows, partition_cols)

        write_deltalake(
            loc.uri,
            frame.to_arrow(),
            mode="overwrite",
            predicate=predicate,
            storage_options=loc.storage_options,
        )

    def replace_where(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        predicate: str,
        schema_mode: SchemaMode,
    ) -> None:
        loc = self._get_location(target)
        write_deltalake(
            loc.uri,
            frame.to_arrow(),
            mode="overwrite",
            predicate=predicate,
            storage_options=loc.storage_options,
        )

    def upsert(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        keys: tuple[str, ...],
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
        existing_schema: PolarsPhysicalSchema | None,
    ) -> None:
        """
        Upsert: Decide internamente CREATE vs MERGE.

        Mantiene warning de producto para UC first-create.
        """
        # Warning de producto para UC first-create
        self._warn_if_uc_first_create(target, existing_schema)

        loc = self._get_location(target)

        if existing_schema is None:
            # FIRST RUN: Crear tabla
            write_deltalake(
                loc.uri,
                frame.to_arrow(),
                mode="overwrite",
                storage_options=loc.storage_options,
            )
            return

        # EXISTING TABLE: Hacer MERGE vía delta-rs
        aligned = self.align_schema(
            pl.LazyFrame(frame), existing_schema, schema_mode
        )
        materialized = aligned.collect()

        dt = DeltaTable(loc.uri, storage_options=loc.storage_options or {})

        # Construir predicado
        all_keys = list(keys) + list(partition_cols)
        predicate = " AND ".join(f"target.{k} = source.{k}" for k in all_keys)

        update_cols = [c for c in materialized.columns if c not in keys]

        dt.merge(
            source=materialized.to_pandas(),
            predicate=predicate,
            source_alias="source",
            target_alias="target",
        ).when_matched_update(
            updates={c: f"source.{c}" for c in update_cols}
        ).when_not_matched_insert(
            updates={c: f"source.{c}" for c in materialized.columns}
        ).execute()
```

### 5. SparkTargetWriter Simplificado

```python
# loom/etl/backends/spark/_writer.py
from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession

from loom.etl.io.target import TargetSpec
from loom.etl.io.target._table import TableWriteSpec
from loom.etl.storage.route import CatalogRouteResolver, PathRouteResolver
from loom.etl.storage.schema.spark import SparkSchemaReader
from loom.etl.storage.write import WriteExecutor, WritePlanner

from ._native import SparkNativeOperations


class SparkTargetWriter:
    """
    Writer unificado para Spark.

    Simplificado: Usa WritePlanner + WriteExecutor + SparkNativeOperations.
    """

    def __init__(
        self,
        spark: SparkSession,
        locator: TableLocator | None = None,
        *,
        route_resolver: TableRouteResolver | None = None,
        schema_reader: SchemaReader | None = None,
    ):
        # Resolver
        if route_resolver is None:
            if locator is None:
                route_resolver = CatalogRouteResolver()
            else:
                from loom.etl.storage._locator import _as_locator
                route_resolver = PathRouteResolver(_as_locator(locator))

        # Schema reader
        if schema_reader is None:
            schema_reader = SparkSchemaReader(spark)

        # Planner (se mantiene)
        self._planner = WritePlanner(route_resolver, schema_reader)

        # Executor con operaciones nativas
        native_ops = SparkNativeOperations(spark)
        self._executor = WriteExecutor(native_ops)

    def write(
        self,
        frame: DataFrame,
        spec: TargetSpec,
        params: Any,
        *,
        streaming: bool = False,
    ) -> None:
        """Escribe frame según spec."""
        if not isinstance(spec, TableWriteSpec):
            raise TypeError(f"SparkTargetWriter only supports table specs, got {type(spec)}")

        # Planificar operación (route + schema)
        op = self._planner.plan(spec, streaming=streaming)

        # Ejecutar
        self._executor.execute(frame, op, params)
```

---

## 📊 Resumen de Cambios

| Componente | Estado | Notas |
|------------|--------|-------|
| WritePlanner | ✅ MANTENER | Bien diseñado, separa planning de ejecución |
| SchemaReader | ✅ MANTENER | Spark y Polars son diferentes |
| TableDiscovery | ✅ MANTENER | Compile-time vs runtime válido |
| WriteExecutor | 🔄 REFACTOR | Unificar en storage/write/exec.py |
| SparkDeltaTableWriter | ❌ ELIMINAR | Adapter passthrough |
| PolarsDeltaTableWriter | ❌ ELIMINAR | Adapter passthrough |
| SparkWriteExecutor | ❌ ELIMINAR | Duplicado, unificar |
| PolarsWriteExecutor | ❌ ELIMINAR | Duplicado, unificar |
| SparkNativeOperations | ✅ NUEVO | Operaciones reales Spark |
| PolarsNativeOperations | ✅ NUEVO | Operaciones reales Polars |
| align_schema | 🔄 MOVER | A cada NativeOperations (tipos diferentes) |
| Warning UC Polars | ✅ MANTENER | Comportamiento de producto |

**Total:** -6 ficheros, ~800 líneas, arquitectura más limpia.
