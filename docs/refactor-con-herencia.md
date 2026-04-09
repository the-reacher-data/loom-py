# Refactorización con Herencia (Sin Composición)

## 🎯 Estructura con Herencia

```
loom/etl/backends/
├── _base.py          # DeltaBackend (clase base abstracta)
├── spark.py          # SparkBackend (hereda de DeltaBackend)
└── polars.py         # PolarsBackend (hereda de DeltaBackend)
```

## 📦 Clase Base Abstracta

```python
# loom/etl/backends/_base.py
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from loom.etl.io.target import SchemaMode
from loom.etl.schema._schema import ColumnSchema
from loom.etl.schema._table import TableRef
from loom.etl.storage.route.model import ResolvedTarget
from loom.etl.storage.schema.model import PhysicalSchema

FrameT = TypeVar("FrameT")
SchemaT = TypeVar("SchemaT", bound=PhysicalSchema)


class DeltaBackend(ABC, Generic[FrameT, SchemaT]):
    """
    Clase base para backends Delta (Spark y Polars).

    Define la interfaz común y lógica compartida.
    Las implementaciones concretas sobreescriben lo específico.
    """

    # ========================================================================
    # INTERFACE PÚBLICA (todos los backends deben implementar)
    # ========================================================================

    @abstractmethod
    def table_exists(self, ref: TableRef) -> bool:
        """Verifica si tabla existe."""
        raise NotImplementedError

    @abstractmethod
    def table_columns(self, ref: TableRef) -> tuple[str, ...]:
        """Lista columnas de tabla."""
        raise NotImplementedError

    @abstractmethod
    def table_schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        """Lee schema de tabla."""
        raise NotImplementedError

    @abstractmethod
    def read_physical_schema(self, target: ResolvedTarget) -> SchemaT | None:
        """Lee schema físico para validación."""
        raise NotImplementedError

    @abstractmethod
    def read_table(self, target: ResolvedTarget) -> FrameT:
        """Lee tabla Delta."""
        raise NotImplementedError

    @abstractmethod
    def read_file(self, path: str, format: str, options: dict | None) -> FrameT:
        """Lee archivo."""
        raise NotImplementedError

    @abstractmethod
    def apply_filters(self, frame: FrameT, predicates: tuple, params: Any) -> FrameT:
        """Aplica filtros."""
        raise NotImplementedError

    # ---- Escritura (5 operaciones) ----

    @abstractmethod
    def write_append(
        self, frame: FrameT, target: ResolvedTarget, *, schema_mode: SchemaMode
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def write_replace(
        self, frame: FrameT, target: ResolvedTarget, *, schema_mode: SchemaMode
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def write_replace_partitions(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def write_replace_where(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        predicate: str,
        schema_mode: SchemaMode,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    # ---- Utilidades ----

    @abstractmethod
    def align_schema(
        self, frame: FrameT, existing_schema: SchemaT | None, mode: SchemaMode
    ) -> FrameT:
        """Alinea schema del frame con existente."""
        raise NotImplementedError

    @abstractmethod
    def materialize_if_needed(self, frame: FrameT, streaming: bool) -> FrameT:
        """Materializa frame si es necesario (Polars lazy -> eager)."""
        raise NotImplementedError

    @abstractmethod
    def predicate_to_sql(self, predicate: Any, params: Any) -> str:
        """Convierte predicado a SQL."""
        raise NotImplementedError

    # ========================================================================
    # MÉTODOS AYUDANTES COMPARTIDOS (opcional, pueden usar los hijos)
    # ========================================================================

    def _is_first_run(self, existing_schema: SchemaT | None, mode: SchemaMode) -> bool:
        """Determina si es primera escritura (crear tabla)."""
        return existing_schema is None and mode is SchemaMode.OVERWRITE
```

## 🔥 SparkBackend (Implementación)

```python
# loom/etl/backends/spark.py
from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from delta.tables import DeltaTable

from loom.etl.backends._base import DeltaBackend
from loom.etl.io.target import SchemaMode
from loom.etl.schema._schema import ColumnSchema
from loom.etl.schema._table import TableRef
from loom.etl.storage.route.model import CatalogTarget, PathTarget, ResolvedTarget
from loom.etl.storage.schema.model import SparkPhysicalSchema


class SparkBackend(DeltaBackend[DataFrame, SparkPhysicalSchema]):
    """
    Backend para Apache Spark con Delta Lake.
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    # ---- Descubrimiento ----

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
                dtype=self._to_loom_type(f.dataType),
                nullable=f.nullable,
            )
            for f in fields
        )

    def read_physical_schema(self, target: ResolvedTarget) -> SparkPhysicalSchema | None:
        """Lee schema para validación antes de escribir."""
        if isinstance(target, CatalogTarget):
            if not self.table_exists(target.catalog_ref):
                return None
            fields = self._spark.table(target.catalog_ref.ref).schema.fields
            return SparkPhysicalSchema(schema=T.StructType([f for f in fields]))
        else:
            # Path - leer del Delta log
            try:
                dt = DeltaTable.forPath(self._spark, target.location.uri)
                return SparkPhysicalSchema(schema=dt.toDF().schema)
            except Exception:
                return None

    # ---- Lectura ----

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

    # ---- Escritura ----

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
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
    ) -> None:
        # Calcular replaceWhere
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
        # Decisión CREATE vs MERGE
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

    # ---- Utilidades ----

    def align_schema(
        self, frame: DataFrame, existing_schema: SparkPhysicalSchema | None, mode: SchemaMode
    ) -> DataFrame:
        if existing_schema is None or mode is SchemaMode.OVERWRITE:
            return frame

        schema = existing_schema.schema
        frame_cols = set(frame.columns)

        # Cast y añadir columnas
        for field in schema.fields:
            if field.name in frame_cols:
                frame = frame.withColumn(
                    field.name, self._cast_col(F.col(field.name), field.dataType)
                )
            else:
                frame = frame.withColumn(
                    field.name, F.lit(None).cast(field.dataType)
                )

        if mode is SchemaMode.STRICT:
            frame = frame.select([f.name for f in schema.fields])

        return frame

    def _cast_col(self, col: F.Column, dtype: T.DataType) -> F.Column:
        if isinstance(dtype, T.StructType):
            return F.struct([
                self._cast_col(F.col(f"{col}.{f.name}"), f.dataType).alias(f.name)
                for f in dtype.fields
            ])
        return col.cast(dtype)

    def materialize_if_needed(self, frame: DataFrame, streaming: bool) -> DataFrame:
        return frame  # Spark ya es eager

    def predicate_to_sql(self, predicate: Any, params: Any) -> str:
        from loom.etl.sql._predicate_sql import predicate_to_sql
        return predicate_to_sql(predicate, params)

    def _sink(self, writer, target: ResolvedTarget) -> None:
        if isinstance(target, CatalogTarget):
            writer.saveAsTable(target.catalog_ref.ref)
        else:
            writer.save(target.location.uri)

    def _to_loom_type(self, dtype: T.DataType):
        from loom.etl.backends.spark._dtype import spark_to_loom
        return spark_to_loom(dtype)
```

## 🐻 PolarsBackend (Implementación)

```python
# loom/etl/backends/polars.py
from __future__ import annotations

import logging
from typing import Any

import polars as pl
from deltalake import DeltaTable, DataCatalog, write_deltalake

from loom.etl.backends._base import DeltaBackend
from loom.etl.io.target import SchemaMode
from loom.etl.schema._schema import ColumnSchema
from loom.etl.schema._table import TableRef
from loom.etl.storage._locator import TableLocator
from loom.etl.storage.route.model import CatalogTarget, PathTarget, ResolvedTarget
from loom.etl.storage.schema.model import PolarsPhysicalSchema

_log = logging.getLogger(__name__)


class PolarsBackend(DeltaBackend[pl.LazyFrame, PolarsPhysicalSchema]):
    """
    Backend para Polars + delta-rs.

    Soporta Unity Catalog para lectura vía DataCatalog.
    Para escritura, requiere path-based storage.
    """

    def __init__(self, locator: TableLocator):
        self._locator = locator

    def _resolve(self, target: ResolvedTarget) -> TableLocation:
        """Resuelve target a ubicación física."""
        if isinstance(target, CatalogTarget):
            # Para lectura UC, resolvemos vía DataCatalog
            return self._resolve_uc(target.catalog_ref)
        return self._locator.locate(target.logical_ref)

    def _resolve_uc(self, ref: TableRef) -> TableLocation:
        """Resuelve referencia UC a path físico vía delta-rs."""
        parts = ref.ref.split(".")
        if len(parts) != 3:
            raise ValueError(f"Invalid UC reference: {ref.ref}")

        catalog, schema, table = parts
        dt = DeltaTable.from_data_catalog(
            data_catalog=DataCatalog.UNITY,
            data_catalog_id=catalog,
            database_name=schema,
            table_name=table,
        )
        return TableLocation(uri=dt.table_uri, storage_options=dt.storage_options)

    # ---- Descubrimiento ----

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

            # Si parece UC, usar DataCatalog
            if self._is_uc_uri(loc.uri):
                return self._read_schema_uc(ref)

            # Delta normal
            dt = DeltaTable(loc.uri, storage_options=loc.storage_options)
            return self._to_column_schema(dt.schema())

        except Exception:
            return None

    def _is_uc_uri(self, uri: str) -> bool:
        return uri.startswith("uc://") or "databricks" in uri

    def _read_schema_uc(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        """Lee schema de UC vía delta-rs DataCatalog."""
        try:
            parts = ref.ref.split(".")
            if len(parts) != 3:
                return None

            catalog, schema, table = parts
            dt = DeltaTable.from_data_catalog(
                data_catalog=DataCatalog.UNITY,
                data_catalog_id=catalog,
                database_name=schema,
                table_name=table,
            )
            return self._to_column_schema(dt.schema())
        except Exception as e:
            _log.warning(f"Failed to read UC schema {ref.ref}: {e}")
            return None

    def read_physical_schema(self, target: ResolvedTarget) -> PolarsPhysicalSchema | None:
        try:
            loc = self._resolve(target)
            dt = DeltaTable(loc.uri, storage_options=loc.storage_options)
            arrow_schema = dt.schema().to_pyarrow()
            pl_schema = pl.Schema(zip(arrow_schema.names, arrow_schema.types))
            return PolarsPhysicalSchema(schema=pl_schema)
        except Exception:
            return None

    # ---- Lectura ----

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

    # ---- Escritura ----

    def _warn_uc_write(self, target: ResolvedTarget, existing_schema) -> None:
        """Warning para escritura en UC."""
        if existing_schema is not None:
            return
        try:
            loc = self._locator.locate(target.logical_ref)
            if self._is_uc_uri(loc.uri):
                _log.warning(
                    "Writing to UC path %s. Data will be written but "
                    "catalog registration may require Spark.",
                    loc.uri
                )
        except Exception:
            pass

    def _get_write_location(self, target: ResolvedTarget) -> TableLocation:
        """Obtiene ubicación para escritura."""
        if isinstance(target, CatalogTarget):
            raise ValueError(
                "PolarsBackend requires path-based targets for writes. "
                "Use SparkBackend for Unity Catalog writes."
            )
        return self._locator.locate(target.logical_ref)

    def write_append(
        self, frame: pl.DataFrame, target: ResolvedTarget, *, schema_mode: SchemaMode
    ) -> None:
        loc = self._get_write_location(target)
        write_deltalake(
            loc.uri,
            frame.to_arrow(),
            mode="append",
            storage_options=loc.storage_options,
        )

    def write_replace(
        self, frame: pl.DataFrame, target: ResolvedTarget, *, schema_mode: SchemaMode
    ) -> None:
        loc = self._get_write_location(target)
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
        if frame.is_empty():
            return

        loc = self._get_write_location(target)

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
        loc = self._get_write_location(target)
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
        self._warn_uc_write(target, existing_schema)

        loc = self._get_write_location(target)

        if self._is_first_run(existing_schema, schema_mode):
            write_deltalake(
                loc.uri,
                frame.to_arrow(),
                mode="overwrite",
                storage_options=loc.storage_options,
            )
            return

        # MERGE
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

    # ---- Utilidades ----

    def align_schema(
        self,
        frame: pl.LazyFrame,
        existing_schema: PolarsPhysicalSchema | None,
        mode: SchemaMode
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

    def _to_column_schema(self, delta_schema) -> tuple[ColumnSchema, ...]:
        from loom.etl.backends.polars._dtype import polars_to_loom_type
        arrow = delta_schema.to_pyarrow()
        return tuple(
            ColumnSchema(
                name=name,
                dtype=polars_to_loom_type(pl.from_arrow_type(dtype)),
                nullable=True,
            )
            for name, dtype in zip(arrow.names, arrow.types)
        )
```

## 📊 Ventajas de esta Estructura

| Aspecto | Beneficio |
|---------|-----------|
| **Herencia** | Interfaz clara, type safety con generics |
| **Sin "Native"** | Nombres claros: SparkBackend, PolarsBackend |
| **Separación moderada** | ~12 métodos por clase, no 20 |
| **Métodos compartidos** | `_is_first_run` en base, etc. |
| **Extensible** | Nuevo backend = heredar de DeltaBackend |

## 🗑️ Ficheros Eliminados

| Fichero | Razón |
|---------|-------|
| `spark/_catalog.py` | Integrado en SparkBackend |
| `spark/_schema.py` | Integrado en SparkBackend |
| `polars/_catalog.py` | Integrado en PolarsBackend |
| `polars/_schema.py` | Integrado en PolarsBackend |
| `spark/writer/*.py` | Reemplazados por SparkBackend + WriteExecutor |
| `polars/writer/*.py` | Reemplazados por PolarsBackend + WriteExecutor |
| `storage/schema/spark.py` | Integrado |
| `storage/schema/delta.py` | Integrado |

**Total: -10 ficheros, ~1,200 líneas**
