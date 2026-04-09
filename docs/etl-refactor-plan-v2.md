# Plan de Refactorización ETL - Versión 2 (Sin First-Run Separation)

## 1. Clarificación de Dependencias

### Spark Backend
```python
# Dependencias:
# - pyspark: Lectura/Escritura nativa
# - deltalake (Python lib): SOLO para MERGE/UPSERT vía DeltaTable API

from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable  # Import al toplevel

class SparkNativeOps:
    def __init__(self, spark: SparkSession):
        self._spark = spark

    def merge(self, frame: DataFrame, op: UpsertOp) -> None:
        # DeltaTable es de la librería deltalake, no delta-rs
        dt = DeltaTable.forName(self._spark, op.table_ref.ref)
        dt.merge(...).execute()
```

### Polars Backend
```python
# Dependencias:
# - polars: DataFrame operations
# - deltalake (delta-rs Python bindings): TODO (read, write, merge)

import polars as pl
from deltalake import DeltaTable, write_deltalake  # Import al toplevel

class PolarsNativeOps:
    def __init__(self, locator: TableLocator):
        self._locator = locator

    def merge(self, frame: pl.DataFrame, op: UpsertOp) -> None:
        loc = self._locator.locate(op.table_ref)
        dt = DeltaTable(loc.uri, storage_options=loc.storage_options)
        dt.merge(...).execute()
```

## 2. Executor Unificado (Sin First-Run Separation)

```python
# loom/etl/execution/_executor.py
from __future__ import annotations

from typing import Any, Generic, TypeVar

from loom.etl.io.target import SchemaMode
from loom.etl.storage.write import AppendOp, ReplaceOp, ReplacePartitionsOp, ReplaceWhereOp, UpsertOp, WriteOperation

FrameT = TypeVar("FrameT")


class WriteExecutor(Generic[FrameT]):
    """
    Executor unificado de operaciones de escritura.

    NO separa "first run" vs "normal run" como lógica externa.
    La optimización de first-run está integrada en cada operación.
    """

    def __init__(self, native: NativeOperations[FrameT]):
        self._native = native

    def execute(self, frame: FrameT, op: WriteOperation, params: Any) -> None:
        """Ejecuta cualquier operación de escritura."""
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
        """Append: Siempre valida schema existente."""
        validated = self._native.validate_schema(frame, op.existing_schema, op.schema_mode)
        self._native.append(validated, op.target, schema_mode=op.schema_mode)

    def _exec_replace(self, frame: FrameT, op: ReplaceOp) -> None:
        """Replace: Siempre valida (first-run es optimización interna del backend)."""
        validated = self._native.validate_schema(frame, op.existing_schema, op.schema_mode)
        self._native.replace(validated, op.target, schema_mode=op.schema_mode)

    def _exec_replace_partitions(self, frame: FrameT, op: ReplacePartitionsOp) -> None:
        """Replace partitions."""
        validated = self._native.validate_schema(frame, op.existing_schema, op.schema_mode)
        self._native.replace_partitions(
            validated,
            op.target,
            partition_cols=op.partition_cols,
            schema_mode=op.schema_mode
        )

    def _exec_replace_where(self, frame: FrameT, op: ReplaceWhereOp, params: Any) -> None:
        """Replace where con predicado."""
        validated = self._native.validate_schema(frame, op.existing_schema, op.schema_mode)
        predicate_sql = self._native.predicate_to_sql(op.replace_predicate, params)
        self._native.replace_where(validated, op.target, predicate=predicate_sql, schema_mode=op.schema_mode)

    def _exec_upsert(self, frame: FrameT, op: UpsertOp) -> None:
        """
        Upsert/Merge.

        Optimización de first-run: El backend decide si hacer CREATE o MERGE.
        Para Polars: Siempre requiere collect() antes del merge.
        """
        # Materializar si es necesario (Polars lazy -> eager)
        materialized = self._native.materialize_if_needed(frame, op.streaming)

        # El backend decide: si no existe tabla, hace CREATE; si existe, hace MERGE
        self._native.upsert(
            materialized,
            op.target,
            keys=op.upsert_keys,
            partition_cols=op.partition_cols,
            schema_mode=op.schema_mode,
            existing_schema=op.existing_schema,  # Backend usa esto para decidir CREATE vs MERGE
        )


class NativeOperations(Protocol, Generic[FrameT]):
    """
    Operaciones nativas del backend.

    El backend implementa la lógica concreta. El executor solo orquesta.
    """

    # Schema validation (común para todos)
    def validate_schema(
        self,
        frame: FrameT,
        existing_schema: Any | None,
        mode: SchemaMode
    ) -> FrameT: ...

    # Materialización (para Polars lazy frames)
    def materialize_if_needed(self, frame: FrameT, streaming: bool) -> FrameT: ...

    # Conversion de predicados (para ReplaceWhere)
    def predicate_to_sql(self, predicate: Any, params: Any) -> str: ...

    # Operaciones de escritura
    def append(self, frame: FrameT, target: ResolvedTarget, *, schema_mode: SchemaMode) -> None: ...
    def replace(self, frame: FrameT, target: ResolvedTarget, *, schema_mode: SchemaMode) -> None: ...
    def replace_partitions(self, frame: FrameT, target: ResolvedTarget, *, partition_cols: tuple[str, ...], schema_mode: SchemaMode) -> None: ...
    def replace_where(self, frame: FrameT, target: ResolvedTarget, *, predicate: str, schema_mode: SchemaMode) -> None: ...

    # Upsert con decisión interna de CREATE vs MERGE
    def upsert(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        keys: tuple[str, ...],
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
        existing_schema: Any | None,
    ) -> None: ...
```

## 3. Implementaciones Nativas (Imports al Toplevel)

### SparkNativeOperations

```python
# loom/etl/backends/spark/_native.py
from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable  # Import al toplevel

from loom.etl.io.target import SchemaMode
from loom.etl.storage.route.model import CatalogTarget, PathTarget, ResolvedTarget
from loom.etl.storage.schema.model import SparkPhysicalSchema


class SparkNativeOperations:
    """
    Operaciones nativas de Spark.

    Depende de:
    - pyspark: Todo excepto MERGE
    - deltalake (Python lib): Solo MERGE vía DeltaTable API
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def validate_schema(
        self,
        frame: DataFrame,
        existing_schema: SparkPhysicalSchema | None,
        mode: SchemaMode
    ) -> DataFrame:
        """Validación/ajuste de schema usando PySpark nativo."""
        if existing_schema is None or mode is SchemaMode.OVERWRITE:
            return frame

        # Lógica de spark_apply_schema
        schema = existing_schema.schema
        # ... casting de columnas ...
        return frame

    def materialize_if_needed(self, frame: DataFrame, streaming: bool) -> DataFrame:
        """Spark DataFrames ya son eager (o streaming manejado por Spark)."""
        return frame

    def predicate_to_sql(self, predicate: Any, params: Any) -> str:
        from loom.etl.sql._predicate_sql import predicate_to_sql
        return predicate_to_sql(predicate, params)

    def append(self, frame: DataFrame, target: ResolvedTarget, *, schema_mode: SchemaMode) -> None:
        writer = frame.write.format("delta").option("optimizeWrite", "true").mode("append")
        if schema_mode is SchemaMode.EVOLVE:
            writer = writer.option("mergeSchema", "true")
        self._sink(writer, target)

    def replace(self, frame: DataFrame, target: ResolvedTarget, *, schema_mode: SchemaMode) -> None:
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
        schema_mode: SchemaMode
    ) -> None:
        # Lógica de replace partitions
        rows = frame.select(*partition_cols).distinct().collect()
        if not rows:
            return  # Nothing to write

        predicate = self._build_partition_predicate(rows, partition_cols)
        writer = (
            frame.sortWithinPartitions(*partition_cols)
            .write.format("delta")
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
        existing_schema: Any | None,
    ) -> None:
        """
        Upsert con decisión interna de CREATE vs MERGE.

        Si existing_schema is None: CREATE (overwrite)
        Si existe: MERGE vía DeltaTable API
        """
        if existing_schema is None:
            # First run: Create table
            writer = (
                frame.write.format("delta")
                .option("optimizeWrite", "true")
                .mode("overwrite")
                .option("overwriteSchema", "true")
            )
            self._sink(writer, target)
            return

        # Existing table: Do MERGE
        validated = self.validate_schema(frame, existing_schema, schema_mode)

        # Obtener DeltaTable handle
        if isinstance(target, CatalogTarget):
            dt = DeltaTable.forName(self._spark, target.catalog_ref.ref)
        else:
            dt = DeltaTable.forPath(self._spark, target.location.uri)

        # Build merge predicate
        all_keys = list(keys) + list(partition_cols)
        predicate = " AND ".join(f"target.{k} = source.{k}" for k in all_keys)

        # Build update set (excluyendo keys)
        update_cols = [c for c in validated.columns if c not in keys]
        update_set = {c: F.col(f"source.{c}") for c in update_cols}
        insert_values = {c: F.col(f"source.{c}") for c in validated.columns}

        # Execute merge
        (
            dt.alias("target")
            .merge(validated.alias("source"), predicate)
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_values)
            .execute()
        )

    def _sink(self, writer, target: ResolvedTarget) -> None:
        """Finaliza la escritura según el tipo de target."""
        if isinstance(target, CatalogTarget):
            writer.saveAsTable(target.catalog_ref.ref)
        else:
            writer.save(target.location.uri)
```

### PolarsNativeOperations

```python
# loom/etl/backends/polars/_native.py
from __future__ import annotations

from typing import Any

import polars as pl
from deltalake import DeltaTable, write_deltalake  # Import al toplevel

from loom.etl.io.target import SchemaMode
from loom.etl.storage.route.model import PathTarget, ResolvedTarget
from loom.etl.storage.schema.model import PolarsPhysicalSchema


class PolarsNativeOperations:
    """
    Operaciones nativas de Polars.

    Depende de:
    - polars: DataFrame operations
    - deltalake (delta-rs): Todo (read, write, merge)
    """

    def __init__(self, locator: TableLocator):
        self._locator = locator

    def validate_schema(
        self,
        frame: pl.LazyFrame,
        existing_schema: PolarsPhysicalSchema | None,
        mode: SchemaMode
    ) -> pl.LazyFrame:
        """Validación/ajuste de schema usando Polars."""
        if existing_schema is None or mode is SchemaMode.OVERWRITE:
            return frame

        # Lógica de apply_schema
        return frame  # Placeholder

    def materialize_if_needed(self, frame: pl.LazyFrame, streaming: bool) -> pl.DataFrame:
        """Polars requiere materialización antes de MERGE."""
        return frame.collect(engine="streaming" if streaming else "auto")

    def predicate_to_sql(self, predicate: Any, params: Any) -> str:
        from loom.etl.sql._predicate_sql import predicate_to_sql
        return predicate_to_sql(predicate, params)

    def append(self, frame: pl.DataFrame, target: ResolvedTarget, *, schema_mode: SchemaMode) -> None:
        loc = self._locator.locate(target.table_ref)
        write_deltalake(
            loc.uri,
            frame.to_arrow(),
            mode="append",
            storage_options=loc.storage_options,
        )

    def replace(self, frame: pl.DataFrame, target: ResolvedTarget, *, schema_mode: SchemaMode) -> None:
        loc = self._locator.locate(target.table_ref)
        write_deltalake(
            loc.uri,
            frame.to_arrow(),
            mode="overwrite",
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
        existing_schema: Any | None,
    ) -> None:
        """
        Upsert con decisión interna de CREATE vs MERGE.

        NOTA: Polars NO puede escribir directamente a Unity Catalog.
        Solo soporta path-based targets.
        """
        if not isinstance(target, PathTarget):
            raise ValueError(
                "Polars backend only supports path-based targets for writes. "
                "Use Spark backend for Unity Catalog writes."
            )

        loc = self._locator.locate(target.table_ref)

        if existing_schema is None:
            # First run: Create table
            write_deltalake(
                loc.uri,
                frame.to_arrow(),
                mode="overwrite",
                storage_options=loc.storage_options,
            )
            return

        # Existing table: Do MERGE vía delta-rs
        validated = self.validate_schema(frame, existing_schema, schema_mode)

        dt = DeltaTable(loc.uri, storage_options=loc.storage_options or {})

        # Build merge predicate
        all_keys = list(keys) + list(partition_cols)
        predicate = " AND ".join(f"target.{k} = source.{k}" for k in all_keys)

        update_cols = [c for c in validated.columns if c not in keys]
        update_set = {c: f"source.{c}" for c in update_cols}
        insert_values = {c: f"source.{c}" for c in validated.columns}

        (
            dt.merge(
                source=validated.to_pandas(),  # delta-rs acepta pandas/arrow
                predicate=predicate,
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update(updates=update_set)
            .when_not_matched_insert(updates=insert_values)
            .execute()
        )
```

## 4. Cambios en Estructura de Archivos

```
loom/etl/
├── backends/
│   ├── spark/
│   │   ├── __init__.py          # Exports: SparkTargetWriter, SparkSourceReader
│   │   ├── _native.py           # SparkNativeOperations (NUEVO)
│   │   ├── _reader.py           # SparkSourceReader (REFACTOR: usa _native)
│   │   ├── _catalog.py          # SparkCatalog (SE MANTIENE)
│   │   └── _writer.py           # SparkTargetWriter (REFACTOR: usa WriteExecutor + _native)
│   └── polars/
│       ├── __init__.py          # Exports: PolarsTargetWriter, PolarsSourceReader
│       ├── _native.py           # PolarsNativeOperations (NUEVO)
│       ├── _reader.py           # PolarsSourceReader (REFACTOR: usa _native)
│       ├── _catalog.py          # DeltaCatalog (SE MANTIENE)
│       └── _writer.py           # PolarsTargetWriter (REFACTOR: usa WriteExecutor + _native)
├── execution/                   # NUEVA CARPETA
│   ├── __init__.py
│   └── _executor.py             # WriteExecutor (único, genérico)
└── ...
```

## 5. API Pública (Sin Cambios)

```python
# Los usuarios no ven diferencia
from loom.etl.backends.spark import SparkTargetWriter, SparkSourceReader
from loom.etl.backends.polars import PolarsTargetWriter, PolarsSourceReader

# Uso idéntico al actual
writer = SparkTargetWriter(spark, locator)
writer.write(frame, spec, params)
```

## 6. Ventajas de este Diseño

1. **Imports al toplevel**: No más imports dentro de funciones
2. **No hay "first run" separado**: La decisión está dentro de `upsert()` donde pertenece
3. **Claro qué usa qué**:
   - Spark: PySpark nativo + deltalake (solo merge)
   - Polars: delta-rs para todo
4. **Executor único**: La orquestación es genérica, las operaciones son específicas
5. **Extensible**: Nuevo backend = implementar NativeOperations
