# Plan de Refactorización ETL - Arquitectura Unificada

## 1. Análisis de Requisitos

### 1.1 Unity Catalog Support

| Feature | Spark | Polars (delta-rs 1.0.5) |
|---------|-------|------------------------|
| **Lectura UC** | `spark.table("catalog.schema.table")` | `DeltaTable.from_data_catalog(DataCatalog.UNITY, ...)` |
| **Escritura UC** | `df.write.saveAsTable("catalog.schema.table")` | ⚠️ No soportado directamente |
| **Autenticación** | Vía Spark conf | Env vars: `DATABRICKS_WORKSPACE_URL`, `DATABRICKS_ACCESS_TOKEN` |
| **AWS Glue** | Vía Spark conf | `DeltaTable.from_data_catalog(DataCatalog.AWS, ...)` |

**Observación crítica**: delta-rs soporta UC para lectura pero NO para escritura directa. Para escribir a UC con Polars, se debe:
1. Escribir al path subyacente (obtenido vía UC API)
2. O usar el Databricks REST API para registrar la tabla

### 1.2 Configuración Requerida

```yaml
# Configuración de usuario (simplificada)
etl:
  storage:
    # Modo 1: Path-based (S3/GCS/ADLS)
    root: "s3://my-bucket/delta/"
    storage_options:
      AWS_REGION: "us-east-1"

    # Modo 2: Unity Catalog
    catalog:
      type: "unity"  # o "glue"
      workspace_url: "${DATABRICKS_WORKSPACE_URL}"
      token: "${DATABRICKS_ACCESS_TOKEN}"

    # Modo 3: Mappings explícitos (multi-cuenta/multi-región)
    mappings:
      "raw.orders":
        uri: "s3://raw-bucket/orders/"
        storage_options:
          AWS_ACCESS_KEY_ID: "${RAW_AWS_KEY}"
          AWS_SECRET_ACCESS_KEY: "${RAW_AWS_SECRET}"
      "curated.customers":
        uri: "gs://curated-bucket/customers/"
        storage_options:
          GOOGLE_SERVICE_ACCOUNT_KEY: "${GCP_SA_KEY}"
```

## 2. Propuesta de Arquitectura

### 2.1 Core Principle: "Storage Backend" vs "Compute Backend"

Separar claramente:
- **Storage Backend**: Dónde y cómo se almacenan los datos (path, UC, Glue)
- **Compute Backend**: Cómo se procesan los datos (Spark, Polars)

```python
# Nueva estructura
loom/etl/
├── storage/                    # Storage backends (agnostic a compute)
│   ├── base.py                 # StorageBackend protocol
│   ├── path.py                 # Path-based storage (S3/GCS/ADLS/Local)
│   ├── unity_catalog.py        # Databricks Unity Catalog
│   └── glue_catalog.py         # AWS Glue Data Catalog
├── compute/                    # Compute engines
│   ├── base.py                 # ComputeBackend protocol
│   ├── spark.py                # SparkBackend
│   └── polars.py               # PolarsBackend
└── bridge.py                   # Une storage + compute
```

### 2.2 Storage Backend

```python
# loom/etl/storage/base.py
from typing import Protocol, runtime_checkable

@runtime_checkable
class StorageBackend(Protocol):
    """Abstracción de almacenamiento - independiente de compute engine."""

    # -- Discovery --
    def exists(self, ref: TableRef) -> bool: ...
    def schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None: ...
    def resolve(self, ref: TableRef) -> ResolvedLocation: ...

    # -- Schema Management --
    def update_schema(self, ref: TableRef, schema: tuple[ColumnSchema, ...]) -> None: ...


# loom/etl/storage/models.py
@dataclass(frozen=True)
class ResolvedLocation:
    """Ubicación resuelta de una tabla - uniforme para todos los casos."""
    type: Literal["path", "unity", "glue"]
    table_ref: TableRef
    # Para path-based
    uri: str | None = None
    storage_options: dict[str, str] | None = None
    # Para catalog-based
    catalog_ref: TableRef | None = None  # e.g., "main.raw.orders" in UC
```

### 2.3 Path Storage (S3/GCS/ADLS/Local)

```python
# loom/etl/storage/path.py
class PathStorage:
    """Storage backend for path-based access (no catalog)."""

    def __init__(
        self,
        root: str | None = None,  # e.g., "s3://bucket/prefix/"
        locator: TableLocator | None = None,  # Para mappings complejos
    ):
        self._locator = locator or PrefixLocator(root)

    def exists(self, ref: TableRef) -> bool:
        loc = self._locator.locate(ref)
        # Usar fsspec para check existence
        return _path_exists(loc.uri)

    def schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        loc = self._locator.locate(ref)
        if not _path_exists(loc.uri):
            return None
        # Leer schema vía delta-rs (funciona para Spark y Polars)
        return _read_delta_schema(loc)

    def resolve(self, ref: TableRef) -> ResolvedLocation:
        loc = self._locator.locate(ref)
        return ResolvedLocation(
            type="path",
            table_ref=ref,
            uri=loc.uri,
            storage_options=loc.storage_options,
        )
```

### 2.4 Unity Catalog Storage

```python
# loom/etl/storage/unity_catalog.py
class UnityCatalogStorage:
    """Storage backend para Databricks Unity Catalog."""

    def __init__(
        self,
        workspace_url: str | None = None,  # De env var si no proporcionado
        token: str | None = None,
        # Para Spark: se usa la sesión Spark configurada con UC
        spark_session: SparkSession | None = None,
    ):
        self._spark = spark_session
        self._workspace_url = workspace_url or os.environ.get("DATABRICKS_WORKSPACE_URL")
        self._token = token or os.environ.get("DATABRICKS_ACCESS_TOKEN")

    def exists(self, ref: TableRef) -> bool:
        if self._spark:
            return bool(self._spark.catalog.tableExists(ref.ref))
        # Para Polars: usar delta-rs con DataCatalog.UNITY
        from deltalake import DeltaTable, DataCatalog
        try:
            catalog_name, schema_name, table_name = ref.ref.split(".")
            DeltaTable.from_data_catalog(
                data_catalog=DataCatalog.UNITY,
                data_catalog_id=catalog_name,
                database_name=schema_name,
                table_name=table_name,
            )
            return True
        except Exception:
            return False

    def resolve(self, ref: TableRef) -> ResolvedLocation:
        """Resuelve tabla UC a su ubicación física."""
        # Para Spark: usar spark.table() para obtener path si es externa
        # Para Polars: la tabla UC se lee vía DataCatalog, pero escritura requiere path
        return ResolvedLocation(
            type="unity",
            table_ref=ref,
            catalog_ref=ref,
            # El URI real se obtiene según el compute backend
        )
```

### 2.5 Compute Backend

```python
# loom/etl/compute/base.py
class ComputeBackend(Protocol[FrameT]):
    """Abstracción de motor de computación."""

    # -- Propiedades --
    @property
    def name(self) -> str: ...
    @property
    def supports_streaming(self) -> bool: ...
    @property
    def is_distributed(self) -> bool: ...

    # -- Reading --
    def read_path(self, loc: ResolvedLocation) -> FrameT: ...
    def read_catalog(self, loc: ResolvedLocation) -> FrameT: ...
    def apply_filters(self, frame: FrameT, predicates: tuple[PredicateNode, ...], params: Any) -> FrameT: ...
    def apply_schema(self, frame: FrameT, schema: tuple[ColumnSchema, ...]) -> FrameT: ...

    # -- Writing --
    def write_append(self, frame: FrameT, loc: ResolvedLocation, schema_mode: SchemaMode) -> None: ...
    def write_replace(self, frame: FrameT, loc: ResolvedLocation, schema_mode: SchemaMode) -> None: ...
    def write_replace_partitions(self, frame: FrameT, loc: ResolvedLocation, cols: tuple[str, ...], schema_mode: SchemaMode) -> None: ...
    def write_upsert(self, frame: FrameT, loc: ResolvedLocation, keys: tuple[str, ...], partition_cols: tuple[str, ...], schema_mode: SchemaMode) -> None: ...

    # -- Execution --
    def collect(self, frame: FrameT) -> FrameT: ...
    def to_native(self, frame: FrameT) -> Any: ...  # Para interoperabilidad
```

### 2.6 Spark Compute Backend

```python
# loom/etl/compute/spark.py
class SparkComputeBackend:
    """Implementación de ComputeBackend para Apache Spark."""

    def __init__(self, spark: SparkSession):
        self._spark = spark

    @property
    def name(self) -> str:
        return "spark"

    @property
    def supports_streaming(self) -> bool:
        return True

    def read_path(self, loc: ResolvedLocation) -> DataFrame:
        """Lee desde path (S3/GCS/ADLS/Local)."""
        return self._spark.read.format("delta").load(loc.uri)

    def read_catalog(self, loc: ResolvedLocation) -> DataFrame:
        """Lee desde Unity Catalog."""
        # Spark tiene soporte nativo UC
        return self._spark.table(loc.catalog_ref.ref if loc.catalog_ref else loc.table_ref.ref)

    def write_replace(self, frame: DataFrame, loc: ResolvedLocation, schema_mode: SchemaMode) -> None:
        writer = frame.write.format("delta").option("optimizeWrite", "true").mode("overwrite")
        if schema_mode is SchemaMode.OVERWRITE:
            writer = writer.option("overwriteSchema", "true")

        if loc.type == "unity":
            writer.saveAsTable(loc.catalog_ref.ref if loc.catalog_ref else loc.table_ref.ref)
        else:
            writer.save(loc.uri)

    def write_upsert(self, frame: DataFrame, loc: ResolvedLocation, keys: tuple[str, ...], partition_cols: tuple[str, ...], schema_mode: SchemaMode) -> None:
        from delta.tables import DeltaTable

        # Spark tiene MERGE nativo via DeltaTable API
        if loc.type == "unity":
            dt = DeltaTable.forName(self._spark, loc.catalog_ref.ref if loc.catalog_ref else loc.table_ref.ref)
        else:
            dt = DeltaTable.forPath(self._spark, loc.uri)

        # ... lógica de merge
```

### 2.7 Polars Compute Backend

```python
# loom/etl/compute/polars.py
class PolarsComputeBackend:
    """Implementación de ComputeBackend para Polars + delta-rs."""

    def __init__(self):
        pass  # Polars no requiere sesión explícita

    @property
    def name(self) -> str:
        return "polars"

    @property
    def supports_streaming(self) -> bool:
        return True  # Polars tiene streaming engine

    def read_path(self, loc: ResolvedLocation) -> pl.LazyFrame:
        """Lee desde path usando delta-rs."""
        return pl.scan_delta(loc.uri, storage_options=loc.storage_options or {})

    def read_catalog(self, loc: ResolvedLocation) -> pl.LazyFrame:
        """Lee desde Unity Catalog vía delta-rs DataCatalog."""
        from deltalake import DeltaTable, DataCatalog

        catalog_name, schema_name, table_name = (loc.catalog_ref.ref if loc.catalog_ref else loc.table_ref.ref).split(".")
        dt = DeltaTable.from_data_catalog(
            data_catalog=DataCatalog.UNITY,
            data_catalog_id=catalog_name,
            database_name=schema_name,
            table_name=table_name,
        )
        # Convertir a Polars LazyFrame
        return pl.scan_delta(dt.table_uri, storage_options=dt.storage_options)

    def write_replace(self, frame: pl.DataFrame, loc: ResolvedLocation, schema_mode: SchemaMode) -> None:
        """Escribe a path - UC escritura requiere path subyacente."""
        from deltalake import write_deltalake

        if loc.type == "unity":
            # ⚠️ Polars no puede escribir directamente a UC
            # Solución: Escribir al path subyacente obtenido de UC
            raise NotImplementedError(
                "Polars cannot write directly to Unity Catalog. "
                "Use path-based storage or Spark backend for UC writes."
            )

        write_deltalake(
            loc.uri,
            frame.to_arrow(),
            mode="overwrite",
            storage_options=loc.storage_options,
        )
```

### 2.8 Bridge: Uniendo Storage + Compute

```python
# loom/etl/bridge.py
class ETLBridge:
    """Une storage backend con compute backend."""

    def __init__(
        self,
        storage: StorageBackend,
        compute: ComputeBackend[FrameT],
    ):
        self._storage = storage
        self._compute = compute

    def read(self, ref: TableRef, predicates: tuple[PredicateNode, ...] = (), params: Any = None) -> FrameT:
        """Lee tabla aplicando filtros."""
        loc = self._storage.resolve(ref)

        # Elegir método de lectura según tipo de storage
        if loc.type in ("unity", "glue"):
            frame = self._compute.read_catalog(loc)
        else:
            frame = self._compute.read_path(loc)

        # Aplicar filtros
        if predicates:
            frame = self._compute.apply_filters(frame, predicates, params)

        return frame

    def write(
        self,
        frame: FrameT,
        ref: TableRef,
        mode: WriteMode,
        schema_mode: SchemaMode = SchemaMode.STRICT,
        **options,
    ) -> None:
        """Escribe tabla con modo especificado."""
        loc = self._storage.resolve(ref)
        existing_schema = self._storage.schema(ref)

        # First-run optimization
        if existing_schema is None and schema_mode is SchemaMode.OVERWRITE:
            validated = frame
        else:
            validated = self._compute.apply_schema(frame, existing_schema)

        # Dispatch a método de escritura
        write_methods = {
            WriteMode.APPEND: self._compute.write_append,
            WriteMode.REPLACE: self._compute.write_replace,
            WriteMode.REPLACE_PARTITIONS: self._compute.write_replace_partitions,
            WriteMode.UPSERT: self._compute.write_upsert,
        }

        method = write_methods[mode]
        if mode is WriteMode.REPLACE_PARTITIONS:
            method(validated, loc, options["partition_cols"], schema_mode)
        elif mode is WriteMode.UPSERT:
            method(validated, loc, options["keys"], options.get("partition_cols", ()), schema_mode)
        else:
            method(validated, loc, schema_mode)
```

## 3. API de Usuario (Simplificada)

```python
# Configuración declarativa
from loom.etl import ETLConfig, StorageConfig, ComputeConfig

config = ETLConfig(
    storage=StorageConfig.unity_catalog(
        workspace_url="https://...",
        token="${DATABRICKS_TOKEN}",
    ),
    compute=ComputeConfig.spark(
        session=spark,
    ),
)

# O path-based
config = ETLConfig(
    storage=StorageConfig.path(
        root="s3://my-bucket/delta/",
        storage_options={"AWS_REGION": "us-east-1"},
    ),
    compute=ComputeConfig.polars(),
)

# Uso
from loom.etl import ETLStep, FromTable, IntoTable

class MyStep(ETLStep):
    source = FromTable("raw.orders")
    target = IntoTable("curated.orders").append()

    def execute(self, params, *, source):
        return source.filter(pl.col("amount") > 100)

# Ejecución
from loom.etl import ETLExecutor

executor = ETLExecutor(config)
executor.run_step(MyStep, params)
```

## 4. Migración y Compatibilidad

### 4.1 Backward Compatibility

```python
# loom/etl/backends/spark/__init__.py
# Mantener aliases por compatibilidad

from ...compute.spark import SparkComputeBackend as _SparkBackend
from ...storage.unity_catalog import UnityCatalogStorage
from ...bridge import ETLBridge

def SparkCatalog(spark):
    """Legacy alias - mantiene compatibilidad."""
    storage = UnityCatalogStorage(spark_session=spark)
    return storage

def SparkSourceReader(spark, locator=None):
    """Legacy alias."""
    from ...storage.path import PathStorage

    if locator is None:
        storage = UnityCatalogStorage(spark_session=spark)
    else:
        storage = PathStorage(locator=locator)

    compute = _SparkBackend(spark)
    bridge = ETLBridge(storage, compute)
    return bridge
```

## 5. Timeline de Implementación

### Fase 1: Storage Abstraction (2-3 días)
- Crear `storage/base.py` con `StorageBackend` protocol
- Crear `storage/path.py` con `PathStorage`
- Crear `storage/unity_catalog.py` con `UnityCatalogStorage`
- Tests unitarios

### Fase 2: Compute Abstraction (3-4 días)
- Crear `compute/base.py` con `ComputeBackend` protocol
- Crear `compute/spark.py` con `SparkComputeBackend`
- Crear `compute/polars.py` con `PolarsComputeBackend`
- Tests unitarios

### Fase 3: Bridge (2 días)
- Crear `bridge.py` con `ETLBridge`
- Integrar con `ETLExecutor` existente
- Tests de integración

### Fase 4: Legacy Compatibility (1-2 días)
- Crear aliases en `backends/spark/` y `backends/polars/`
- Asegurar todos los tests existentes pasan
- Deprecated warnings en APIs legacy

### Fase 5: Cleanup (opcional, futuro)
- Eliminar código legacy cuando sea seguro
- Documentar nueva arquitectura

## 6. Beneficios Esperados

| Métrica | Antes | Después | Mejora |
|---------|-------|---------|--------|
| Líneas de código | ~3,500 | ~1,500 | -57% |
| Clases | 25+ | 7 | -72% |
| Duplicación Spark/Polars | 70% | 10% | -86% |
| Nuevos backends | Complejo | Fácil | +500% |
| Test coverage por backend | Fragmentado | Unificado | +40% |
