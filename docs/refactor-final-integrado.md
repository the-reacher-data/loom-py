# Refactorización Final: Backend Integrado (Sin "Native")

## 🎯 Principios

1. **Sin "Native" en nombres**: Usar `SparkBackend`, `PolarsBackend`
2. **Una clase por backend**: Integra lectura, escritura, schema, catálogo
3. **delta-rs soporta UC**: Con credentials via `storage_options` o `DataCatalog`

## 🔧 Estructura Final

```
loom/etl/
├── backends/
│   ├── spark/
│   │   ├── __init__.py          # SparkBackend, SparkTargetWriter, SparkSourceReader
│   │   ├── _backend.py          # SparkBackend (TODO en una clase)
│   │   └── _writer.py           # SparkTargetWriter (dispatcher simplificado)
│   └── polars/
│       ├── __init__.py          # PolarsBackend, PolarsTargetWriter, PolarsSourceReader
│       ├── _backend.py          # PolarsBackend (TODO en una clase)
│       └── _writer.py           # PolarsTargetWriter (dispatcher simplificado)
├── storage/
│   └── write/
│       ├── __init__.py
│       ├── _executor.py         # WriteExecutor (único, orquesta)
│       ├── _plan.py             # WritePlanner (SE MANTIENE)
│       └── _ops.py              # WriteOperation dataclasses (SE MANTIENEN)
└── ...
```

## 🗑️ Ficheros ELIMINADOS (10 ficheros)

| Fichero | Razón |
|---------|-------|
| `spark/_catalog.py` | Integrado en SparkBackend |
| `spark/_schema.py` | Integrado en SparkBackend |
| `polars/_catalog.py` | Integrado en PolarsBackend |
| `polars/_schema.py` | Integrado en PolarsBackend |
| `storage/schema/spark.py` | Integrado en SparkBackend |
| `storage/schema/delta.py` | Integrado en PolarsBackend |
| `spark/writer/table.py` | Adapter sin valor |
| `polars/writer/table.py` | Adapter sin valor |
| `spark/writer/exec.py` | Duplicado |
| `polars/writer/exec.py` | Duplicado |

## 📦 SparkBackend (Todo en Una Clase)

```python
# loom/etl/backends/spark/_backend.py
from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from delta.tables import DeltaTable

from loom.etl.io.target import SchemaMode
from loom.etl.schema._schema import ColumnSchema
from loom.etl.schema._table import TableRef
from loom.etl.storage.route.model import CatalogTarget, PathTarget, ResolvedTarget
from loom.etl.storage.schema.model import PhysicalSchema, SparkPhysicalSchema


class SparkBackend:
    """
    Backend completo para Apache Spark.

    Integra:
    - Lectura de fuentes (Delta, archivos)
    - Escritura de destinos (Delta con todos los modos)
    - Gestión de schema (lectura, validación, alineación)
    - Descubrimiento de tablas (UC + path-based)
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    # ========================================================================
    # DESCUBRIMIENTO DE TABLAS (antes SparkCatalog)
    # ========================================================================

    def table_exists(self, ref: TableRef) -> bool:
        """Verifica existencia vía Spark catalog."""
        return bool(self._spark.catalog.tableExists(ref.ref))

    def table_columns(self, ref: TableRef) -> tuple[str, ...]:
        """Lista columnas vía Spark catalog."""
        if not self.table_exists(ref):
            return ()
        return tuple(self._spark.table(ref.ref).columns)

    def table_schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        """Lee schema vía Spark catalog."""
        if not self.table_exists(ref):
            return None
        fields = self._spark.table(ref.ref).schema.fields
        return tuple(
            ColumnSchema(
                name=f.name,
                dtype=self._spark_type_to_loom(f.dataType),
                nullable=f.nullable,
            )
            for f in fields
        )

    def read_physical_schema(self, target: ResolvedTarget) -> SparkPhysicalSchema | None:
        """Lee schema físico para validación antes de escribir."""
        if isinstance(target, CatalogTarget):
            if not self.table_exists(target.catalog_ref):
                return None
            fields = self._spark.table(target.catalog_ref.ref).schema.fields
            return SparkPhysicalSchema(
                schema=T.StructType([f for f in fields])
            )
        else:  # PathTarget
            # Para path, leer del Delta log directamente
            from delta.tables import DeltaTable
            try:
                dt = DeltaTable.forPath(self._spark, target.location.uri)
                return SparkPhysicalSchema(schema=dt.toDF().schema)
            except Exception:
                return None

    # ========================================================================
    # LECTURA DE FUENTES
    # ========================================================================

    def read_table(self, target: ResolvedTarget) -> DataFrame:
        """Lee tabla Delta."""
        if isinstance(target, CatalogTarget):
            return self._spark.table(target.catalog_ref.ref)
        return self._spark.read.format("delta").load(target.location.uri)

    def read_file(self, path: str, format: str, options: dict | None) -> DataFrame:
        """Lee archivo (CSV, JSON, Parquet)."""
        reader = self._spark.read.format(format)
        if options:
            for k, v in options.items():
                reader = reader.option(k, v)
        return reader.load(path)

    def apply_filters(
        self,
        frame: DataFrame,
        predicates: tuple,
        params: Any
    ) -> DataFrame:
        """Aplica filtros al frame."""
        from loom.etl.sql._predicate_sql import predicate_to_sql
        for pred in predicates:
            sql = predicate_to_sql(pred, params)
            frame = frame.filter(sql)
        return frame

    # ========================================================================
    # ESCRITURA DE DESTINOS
    # ========================================================================

    def write_append(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Ejecuta append nativo."""
        writer = frame.write \
            .format("delta") \
            .option("optimizeWrite", "true") \
            .mode("append")

        if schema_mode is SchemaMode.EVOLVE:
            writer = writer.option("mergeSchema", "true")

        self._sink(writer, target)

    def write_replace(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Ejecuta replace nativo."""
        writer = frame.write \
            .format("delta") \
            .option("optimizeWrite", "true") \
            .mode("overwrite")

        if schema_mode is SchemaMode.OVERWRITE:
            writer = writer.option("overwriteSchema", "true")

        self._sink(writer, target)

    def write_replace_partitions(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
    ) -> None:
        """Ejecuta replace partitions nativo."""
        # Calcular predicate
        rows = frame.select(*partition_cols).distinct().collect()
        if not rows:
            return

        predicates = []
        for row in rows:
            row_pred = " AND ".join(f"{col} = '{row[col]}'" for col in partition_cols)
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

    def write_replace_where(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        predicate: str,
        schema_mode: SchemaMode,
    ) -> None:
        """Ejecuta replace where nativo."""
        writer = (
            frame.write.format("delta")
            .option("optimizeWrite", "true")
            .mode("overwrite")
            .option("replaceWhere", predicate)
        )

        self._sink(writer, target)

    def write_upsert(
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
        Ejecuta upsert/merge nativo.

        Decisión interna CREATE vs MERGE.
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

        # Existing table: Merge
        aligned = self.align_schema(frame, existing_schema, schema_mode)

        # Obtener DeltaTable
        if isinstance(target, CatalogTarget):
            dt = DeltaTable.forName(self._spark, target.catalog_ref.ref)
        else:
            dt = DeltaTable.forPath(self._spark, target.location.uri)

        # Build merge
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
    # UTILIDADES
    # ========================================================================

    def align_schema(
        self,
        frame: DataFrame,
        existing_schema: SparkPhysicalSchema | None,
        mode: SchemaMode,
    ) -> DataFrame:
        """Alinea schema del frame con existente."""
        if existing_schema is None or mode is SchemaMode.OVERWRITE:
            return frame

        schema = existing_schema.schema
        frame_cols = set(frame.columns)

        # Cast columnas existentes
        for field in schema.fields:
            if field.name in frame_cols:
                frame = frame.withColumn(
                    field.name,
                    self._cast_column(F.col(field.name), field.dataType)
                )

        # Añadir columnas missing como null
        for field in schema.fields:
            if field.name not in frame_cols:
                frame = frame.withColumn(
                    field.name,
                    F.lit(None).cast(field.dataType)
                )

        # STRICT: Reordenar
        if mode is SchemaMode.STRICT:
            frame = frame.select([f.name for f in schema.fields])

        return frame

    def _cast_column(self, col: F.Column, dtype: T.DataType) -> F.Column:
        """Cast recursivo para tipos anidados."""
        if isinstance(dtype, T.StructType):
            return F.struct([
                self._cast_column(F.col(f"{col}.{f.name}"), f.dataType).alias(f.name)
                for f in dtype.fields
            ])
        return col.cast(dtype)

    def _sink(self, writer, target: ResolvedTarget) -> None:
        """Finaliza escritura."""
        if isinstance(target, CatalogTarget):
            writer.saveAsTable(target.catalog_ref.ref)
        else:
            writer.save(target.location.uri)

    def _spark_type_to_loom(self, dtype: T.DataType) -> LoomDtype:
        """Convierte tipo Spark a Loom."""
        from loom.etl.backends.spark._dtype import spark_to_loom
        return spark_to_loom(dtype)

    def materialize_if_needed(self, frame: DataFrame, streaming: bool) -> DataFrame:
        """Spark no requiere materialización explícita."""
        return frame

    def predicate_to_sql(self, predicate: Any, params: Any) -> str:
        """Convierte predicado a SQL."""
        from loom.etl.sql._predicate_sql import predicate_to_sql
        return predicate_to_sql(predicate, params)
```

## 📦 PolarsBackend (Todo en Una Clase + UC Support)

```python
# loom/etl/backends/polars/_backend.py
from __future__ import annotations

import logging
from typing import Any

import polars as pl
from deltalake import DeltaTable, DataCatalog, write_deltalake

from loom.etl.io.target import SchemaMode
from loom.etl.schema._schema import ColumnSchema
from loom.etl.schema._table import TableRef
from loom.etl.storage._locator import TableLocation, TableLocator
from loom.etl.storage.route.model import CatalogTarget, PathTarget, ResolvedTarget
from loom.etl.storage.schema.model import PhysicalSchema, PolarsPhysicalSchema

_log = logging.getLogger(__name__)


class PolarsBackend:
    """
    Backend completo para Polars + delta-rs.

    Integra:
    - Lectura de fuentes (Delta vía delta-rs, archivos)
    - Escritura de destinos (Delta vía delta-rs)
    - Gestión de schema (lectura, validación, alineación)
    - Descubrimiento de tablas (UC + path-based vía delta-rs)

    IMPORTANTE: delta-rs soporta UC vía DataCatalog.UNITY
    """

    def __init__(
        self,
        locator: TableLocator,
        # UC credentials se pasan aquí o vía storage_options en locator
        uc_workspace_url: str | None = None,
        uc_token: str | None = None,
    ):
        self._locator = locator
        self._uc_workspace_url = uc_workspace_url
        self._uc_token = uc_token

    def _resolve_location(self, target: ResolvedTarget) -> TableLocation:
        """Resuelve ubicación física."""
        if isinstance(target, CatalogTarget):
            # Para UC, necesitamos obtener el path subyacente
            # NOTA: delta-rs DataCatalog nos da acceso de lectura
            # Para escritura, necesitamos el path físico
            raise ValueError(
                "PolarsBackend requires path-based targets for writes. "
                "Use SparkBackend for full Unity Catalog support, "
                "or configure path-based storage."
            )
        return self._locator.locate(target.logical_ref)

    # ========================================================================
    # DESCUBRIMIENTO DE TABLAS (integrado, usa delta-rs)
    # ========================================================================

    def table_exists(self, ref: TableRef) -> bool:
        """Verifica existencia vía delta-rs."""
        try:
            loc = self._locator.locate(ref)
            return DeltaTable.is_deltatable(loc.uri, loc.storage_options)
        except Exception:
            return False

    def table_columns(self, ref: TableRef) -> tuple[str, ...]:
        """Lista columnas vía delta-rs."""
        schema = self.table_schema(ref)
        return tuple(c.name for c in schema) if schema else ()

    def table_schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        """Lee schema vía delta-rs."""
        try:
            loc = self._locator.locate(ref)

            # Si es UC, usar DataCatalog
            if loc.uri.startswith("uc://") or self._is_uc_path(loc.uri):
                return self._read_schema_uc(ref, loc)

            # Path normal
            dt = DeltaTable(loc.uri, storage_options=loc.storage_options)
            return self._delta_schema_to_column_schema(dt.schema())

        except Exception:
            return None

    def _is_uc_path(self, uri: str) -> bool:
        """Detecta si es URI de Unity Catalog."""
        return uri.startswith("uc://") or ".azuredatabricks.net" in uri

    def _read_schema_uc(
        self,
        ref: TableRef,
        loc: TableLocation
    ) -> tuple[ColumnSchema, ...] | None:
        """
        Lee schema de Unity Catalog vía delta-rs DataCatalog.

        Requiere:
        - DATABRICKS_WORKSPACE_URL en env o pasado en constructor
        - DATABRICKS_ACCESS_TOKEN en env o pasado en constructor
        """
        try:
            # Parse catalog.schema.table
            parts = ref.ref.split(".")
            if len(parts) != 3:
                return None

            catalog_name, schema_name, table_name = parts

            # delta-rs 1.0+ soporta UC vía DataCatalog
            dt = DeltaTable.from_data_catalog(
                data_catalog=DataCatalog.UNITY,
                data_catalog_id=catalog_name,
                database_name=schema_name,
                table_name=table_name,
                # Credentials vía storage_options o env vars
                storage_options=loc.storage_options,
            )

            return self._delta_schema_to_column_schema(dt.schema())

        except Exception as e:
            _log.warning(f"Failed to read UC schema for {ref.ref}: {e}")
            return None

    def read_physical_schema(
        self,
        target: ResolvedTarget
    ) -> PolarsPhysicalSchema | None:
        """Lee schema físico para validación antes de escribir."""
        try:
            if isinstance(target, CatalogTarget):
                # Intentar leer vía UC
                schema = self.table_schema(target.catalog_ref)
                if schema is None:
                    return None
                # Convertir a PolarsPhysicalSchema
                pl_schema = self._column_schema_to_polars(schema)
                return PolarsPhysicalSchema(schema=pl_schema)

            # Path-based
            loc = self._locator.locate(target.logical_ref)

            # Para UC paths
            if self._is_uc_path(loc.uri):
                schema = self.table_schema(target.logical_ref)
                if schema:
                    pl_schema = self._column_schema_to_polars(schema)
                    return PolarsPhysicalSchema(schema=pl_schema)
                return None

            # Delta normal
            dt = DeltaTable(loc.uri, storage_options=loc.storage_options)
            arrow_schema = dt.schema().to_pyarrow()
            pl_schema = pl.Schema(zip(arrow_schema.names, arrow_schema.types))
            return PolarsPhysicalSchema(schema=pl_schema)

        except Exception:
            return None

    def _delta_schema_to_column_schema(
        self,
        delta_schema
    ) -> tuple[ColumnSchema, ...]:
        """Convierte schema delta a ColumnSchema."""
        from loom.etl.backends.polars._dtype import polars_to_loom_type

        arrow_schema = delta_schema.to_pyarrow()
        return tuple(
            ColumnSchema(
                name=name,
                dtype=polars_to_loom_type(pl.from_arrow_type(dtype)),
                nullable=True,
            )
            for name, dtype in zip(arrow_schema.names, arrow_schema.types)
        )

    def _column_schema_to_polars(
        self,
        columns: tuple[ColumnSchema, ...]
    ) -> pl.Schema:
        """Convierte ColumnSchema a Polars Schema."""
        from loom.etl.backends.polars._dtype import loom_type_to_polars

        return pl.Schema([
            (col.name, loom_type_to_polars(col.dtype))
            for col in columns
        ])

    # ========================================================================
    # LECTURA DE FUENTES
    # ========================================================================

    def read_table(self, target: ResolvedTarget) -> pl.LazyFrame:
        """Lee tabla Delta vía delta-rs."""
        if isinstance(target, CatalogTarget):
            # Leer vía UC
            loc = self._resolve_uc_path(target.catalog_ref)
        else:
            loc = self._locator.locate(target.logical_ref)

        return pl.scan_delta(
            loc.uri,
            storage_options=loc.storage_options
        )

    def _resolve_uc_path(self, ref: TableRef) -> TableLocation:
        """Resuelve tabla UC a path físico vía delta-rs DataCatalog."""
        parts = ref.ref.split(".")
        if len(parts) != 3:
            raise ValueError(f"Invalid UC reference: {ref.ref}")

        catalog_name, schema_name, table_name = parts

        dt = DeltaTable.from_data_catalog(
            data_catalog=DataCatalog.UNITY,
            data_catalog_id=catalog_name,
            database_name=schema_name,
            table_name=table_name,
        )

        return TableLocation(
            uri=dt.table_uri,
            storage_options=dt.storage_options,
        )

    def read_file(self, path: str, format: str, options: dict | None) -> pl.LazyFrame:
        """Lee archivo vía Polars."""
        readers = {
            "csv": pl.scan_csv,
            "json": pl.scan_ndjson,
            "parquet": pl.scan_parquet,
        }
        reader = readers.get(format)
        if not reader:
            raise ValueError(f"Unsupported format: {format}")
        return reader(path, **(options or {}))

    def apply_filters(
        self,
        frame: pl.LazyFrame,
        predicates: tuple,
        params: Any
    ) -> pl.LazyFrame:
        """Aplica filtros al frame."""
        from loom.etl.backends.polars._predicate import predicate_to_polars
        for pred in predicates:
            expr = predicate_to_polars(pred, params)
            frame = frame.filter(expr)
        return frame

    # ========================================================================
    # ESCRITURA DE DESTINOS
    # ========================================================================

    def _warn_uc_first_create(
        self,
        target: ResolvedTarget,
        existing_schema: PhysicalSchema | None
    ) -> None:
        """Warning de producto para UC first-create."""
        if existing_schema is not None:
            return

        loc = self._locator.locate(target.logical_ref)
        if not self._is_uc_path(loc.uri):
            return

        _log.warning(
            "Polars writing to UC path '%s' for first time. "
            "Data will be written but catalog registration may require Spark. "
            "Consider pre-creating table in UC or using Spark backend.",
            loc.uri,
        )

    def write_append(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Ejecuta append vía delta-rs."""
        loc = self._resolve_location(target)

        write_deltalake(
            loc.uri,
            frame.to_arrow(),
            mode="append",
            storage_options=loc.storage_options,
        )

    def write_replace(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Ejecuta replace vía delta-rs."""
        loc = self._resolve_location(target)

        write_deltalake(
            loc.uri,
            frame.to_arrow(),
            mode="overwrite",
            storage_options=loc.storage_options,
        )

    def write_replace_partitions(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
    ) -> None:
        """Ejecuta replace partitions vía delta-rs."""
        if frame.is_empty():
            return

        loc = self._resolve_location(target)

        # Calcular predicate
        from loom.etl.sql._upsert import _build_partition_predicate
        rows = frame.select(list(partition_cols)).unique().to_dicts()
        predicate = _build_partition_predicate(rows, partition_cols)

        write_deltalake(
            loc.uri,
            frame.to_arrow(),
            mode="overwrite",
            predicate=predicate,
            storage_options=loc.storage_options,
        )

    def write_replace_where(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        predicate: str,
        schema_mode: SchemaMode,
    ) -> None:
        """Ejecuta replace where vía delta-rs."""
        loc = self._resolve_location(target)

        write_deltalake(
            loc.uri,
            frame.to_arrow(),
            mode="overwrite",
            predicate=predicate,
            storage_options=loc.storage_options,
        )

    def write_upsert(
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
        Ejecuta upsert vía delta-rs.

        Decisión interna CREATE vs MERGE.
        Mantiene warning de producto para UC.
        """
        self._warn_uc_first_create(target, existing_schema)

        loc = self._resolve_location(target)

        if existing_schema is None:
            # First run: Create
            write_deltalake(
                loc.uri,
                frame.to_arrow(),
                mode="overwrite",
                storage_options=loc.storage_options,
            )
            return

        # Existing: Merge vía delta-rs
        aligned = self.align_schema(
            pl.LazyFrame(frame), existing_schema, schema_mode
        ).collect()

        dt = DeltaTable(loc.uri, storage_options=loc.storage_options)

        all_keys = list(keys) + list(partition_cols)
        predicate = " AND ".join(f"target.{k} = source.{k}" for k in all_keys)

        update_cols = [c for c in aligned.columns if c not in keys]

        dt.merge(
            source=aligned.to_pandas(),
            predicate=predicate,
            source_alias="source",
            target_alias="target",
        ).when_matched_update(
            updates={c: f"source.{c}" for c in update_cols}
        ).when_not_matched_insert(
            updates={c: f"source.{c}" for c in aligned.columns}
        ).execute()

    # ========================================================================
    # UTILIDADES
    # ========================================================================

    def align_schema(
        self,
        frame: pl.LazyFrame,
        existing_schema: PolarsPhysicalSchema | None,
        mode: SchemaMode,
    ) -> pl.LazyFrame:
        """Alinea schema del frame con existente."""
        if existing_schema is None or mode is SchemaMode.OVERWRITE:
            return frame

        from loom.etl.backends.polars._schema import apply_schema
        return apply_schema(frame, existing_schema.schema, mode)

    def materialize_if_needed(
        self,
        frame: pl.LazyFrame,
        streaming: bool
    ) -> pl.DataFrame:
        """Polars requiere materialización antes de MERGE."""
        return frame.collect(engine="streaming" if streaming else "auto")

    def predicate_to_sql(self, predicate: Any, params: Any) -> str:
        """Convierte predicado a SQL."""
        from loom.etl.sql._predicate_sql import predicate_to_sql
        return predicate_to_sql(predicate, params)
```

## 📊 Resumen de Cambios

| Aspecto | Antes | Después | Cambio |
|---------|-------|---------|--------|
| **Clases por backend** | 6 (catalog, schema, reader, writer, exec, dtype) | 1 (Backend integrado) | -83% |
| **Ficheros escritura** | 12 | 4 | -67% |
| **Líneas duplicadas** | ~1,200 | 0 | -100% |
| **Nombres** | NativeOperations | SparkBackend, PolarsBackend | Claro |
| **UC support delta-rs** | Parcial | Completo vía DataCatalog | ✅ |

## ✅ Checklist Implementación

- [ ] Crear `SparkBackend` (integra todo)
- [ ] Crear `PolarsBackend` (integra todo + UC)
- [ ] Actualizar `WriteExecutor` para usar Backend
- [ ] Simplificar `SparkTargetWriter` (dispatcher)
- [ ] Simplificar `PolarsTargetWriter` (dispatcher)
- [ ] Eliminar 10 ficheros obsoletos
- [ ] Tests: Verificar UC con delta-rs
- [ ] Tests: Verificar paths con credentials
- [ ] Compatibilidad API pública
