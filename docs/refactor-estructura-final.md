# Estructura Final de Ficheros

## 🎯 Nombre de Clase Base: `ExecutionBackend`

Alternativas consideradas:
- ✅ `ExecutionBackend` - Enfocado en ejecución de ETL
- ✅ `MotorBackend` - En español, claro
- ✅ `ComputeBackend` - Enfocado en computación
- ❌ `DeltaBackend` - Confusión con Delta Lake format

**Elegimos:** `ExecutionBackend`

## 📁 Estructura de Ficheros Limpia

```
loom/etl/
├── backends/
│   ├── __init__.py
│   ├── _base.py              # ExecutionBackend (clase base abstracta)
│   ├── spark.py              # SparkBackend
│   └── polars.py             # PolarsBackend
│
├── io/
│   ├── __init__.py
│   ├── source.py             # FromTable, FromFile (DSL)
│   ├── target.py             # IntoTable (DSL)
│   └── _specs.py             # Specs: AppendSpec, ReplaceSpec, etc.
│
├── storage/
│   ├── __init__.py
│   ├── _locator.py           # TableLocator, PrefixLocator, MappingLocator
│   ├── _factory.py           # StorageConfig (simplificado)
│   │
│   ├── route/
│   │   ├── __init__.py
│   │   ├── resolver.py       # TableRouteResolver, PathRouteResolver, CatalogRouteResolver
│   │   ├── model.py          # ResolvedTarget, PathTarget, CatalogTarget
│   │   └── catalog.py        # RoutedCatalog (si se mantiene)
│   │
│   ├── schema/
│   │   ├── __init__.py
│   │   ├── model.py          # PhysicalSchema, SparkPhysicalSchema, PolarsPhysicalSchema
│   │   └── reader.py         # SchemaReader (protocolo, opcional)
│   │
│   └── write/
│       ├── __init__.py
│       ├── _executor.py      # WriteExecutor (único, usa ExecutionBackend)
│       ├── _plan.py          # WritePlanner (SE MANTIENE)
│       └── _ops.py           # WriteOperation dataclasses (SE MANTIENEN)
│
└── ...
```

## 🗑️ Ficheros ELIMINADOS (14 ficheros)

### Backends - Eliminados (8)
```
loom/etl/backends/spark/
├── _catalog.py              # 101 líneas → Integrado en SparkBackend
├── _reader.py               # 148 líneas → Integrado en SparkBackend
├── _schema.py               # 131 líneas → Integrado en SparkBackend
└── writer/
    ├── __init__.py          # 5 líneas
    ├── core.py              # 115 líneas → Integrado
    ├── table.py             # 94 líneas → ELIMINADO (adapter)
    ├── file.py              # 85 líneas → Integrado
    └── exec.py              # 313 líneas → Reemplazado por WriteExecutor

loom/etl/backends/polars/
├── _catalog.py              # 101 líneas → Integrado en PolarsBackend
├── _reader.py               # 258 líneas → Integrado en PolarsBackend
├── _schema.py               # 143 líneas → Integrado en PolarsBackend
└── writer/
    ├── __init__.py          # 5 líneas
    ├── core.py              # 107 líneas → Integrado
    ├── table.py             # 90 líneas → ELIMINADO (adapter)
    ├── file.py              # 136 líneas → Integrado
    └── exec.py              # 279 líneas → Reemplazado por WriteExecutor
```

### Storage - Eliminados (4)
```
loom/etl/storage/schema/
├── reader.py                # 40 líneas → Integrado en backends
├── spark.py                 # 50 líneas → Integrado en SparkBackend
└── delta.py                 # 45 líneas → Integrado en PolarsBackend

loom/etl/storage/write/
└── generic.py               # 58 líneas → ELIMINADO (adapter sin valor)
```

### Otros - Eliminados (2)
```
loom/etl/backends/spark/_dtype.py   # 126 líneas → Mover a loom/etl/types/
loom/etl/backends/polars/_dtype.py  # 257 líneas → Mover a loom/etl/types/
```

## ✅ Ficheros NUEVOS/MODIFICADOS (5 ficheros)

```
loom/etl/
├── backends/
│   ├── _base.py              # NUEVO: ExecutionBackend (interfaz base)
│   ├── spark.py              # NUEVO: SparkBackend (~300 líneas, integrado)
│   └── polars.py             # NUEVO: PolarsBackend (~350 líneas, integrado)
│
├── storage/write/
│   └── _executor.py          # MODIFICADO: Usa ExecutionBackend
│
└── types/
    └── _converters.py        # NUEVO: Conversiones de tipos Spark/Polars/Loom
        # - spark_type_to_loom()
        # - polars_type_to_loom()
        # - loom_type_to_spark()
        # - loom_type_to_polars()
```

## 📊 Código por Fichero (Después)

| Fichero | Líneas | Responsabilidad |
|---------|--------|-----------------|
| `backends/_base.py` | ~80 | Clase base abstracta ExecutionBackend |
| `backends/spark.py` | ~280 | Todo Spark: lectura, escritura, schema, catálogo |
| `backends/polars.py` | ~320 | Todo Polars: lectura, escritura, schema, catálogo, UC |
| `storage/write/_executor.py` | ~100 | Orquestación común (match/case) |
| `storage/write/_plan.py` | ~192 | SE MANTIENE: WritePlanner |
| `types/_converters.py` | ~150 | Conversiones de tipos |

**Total: ~1,120 líneas** (vs ~3,200 antes) = **-65%**

## 🔧 ExecutionBackend (Interfaz)

```python
# loom/etl/backends/_base.py
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from loom.etl.io.target import SchemaMode
from loom.etl.schema._schema import ColumnSchema
from loom.etl.schema._table import TableRef
from loom.etl.storage.route.model import ResolvedTarget
from loom.etl.storage.schema.model import PhysicalSchema

FrameT = TypeVar("FrameT")
SchemaT = TypeVar("SchemaT", bound=PhysicalSchema)


class ExecutionBackend(ABC, Generic[FrameT, SchemaT]):
    """
    Backend de ejecución ETL.

    Implementaciones concretas:
    - SparkBackend: Para Apache Spark
    - PolarsBackend: Para Polars + delta-rs
    """

    # ------------------------------------------------------------------------
    # DESCUBRIMIENTO Y METADATA
    # ------------------------------------------------------------------------

    @abstractmethod
    def table_exists(self, ref: TableRef) -> bool: ...

    @abstractmethod
    def table_columns(self, ref: TableRef) -> tuple[str, ...]: ...

    @abstractmethod
    def table_schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None: ...

    @abstractmethod
    def read_physical_schema(self, target: ResolvedTarget) -> SchemaT | None: ...

    # ------------------------------------------------------------------------
    # LECTURA
    # ------------------------------------------------------------------------

    @abstractmethod
    def read_table(self, target: ResolvedTarget) -> FrameT: ...

    @abstractmethod
    def read_file(self, path: str, format: str, options: dict | None) -> FrameT: ...

    @abstractmethod
    def apply_filters(self, frame: FrameT, predicates: tuple, params: Any) -> FrameT: ...

    # ------------------------------------------------------------------------
    # ESCRITURA (5 operaciones)
    # ------------------------------------------------------------------------

    @abstractmethod
    def write_append(self, frame: FrameT, target: ResolvedTarget, *, schema_mode: SchemaMode) -> None: ...

    @abstractmethod
    def write_replace(self, frame: FrameT, target: ResolvedTarget, *, schema_mode: SchemaMode) -> None: ...

    @abstractmethod
    def write_replace_partitions(self, frame: FrameT, target: ResolvedTarget, *, partition_cols: tuple[str, ...], schema_mode: SchemaMode) -> None: ...

    @abstractmethod
    def write_replace_where(self, frame: FrameT, target: ResolvedTarget, *, predicate: str, schema_mode: SchemaMode) -> None: ...

    @abstractmethod
    def write_upsert(self, frame: FrameT, target: ResolvedTarget, *, keys: tuple[str, ...], partition_cols: tuple[str, ...], schema_mode: SchemaMode, existing_schema: SchemaT | None) -> None: ...

    # ------------------------------------------------------------------------
    # UTILIDADES
    # ------------------------------------------------------------------------

    @abstractmethod
    def align_schema(self, frame: FrameT, existing_schema: SchemaT | None, mode: SchemaMode) -> FrameT: ...

    @abstractmethod
    def materialize_if_needed(self, frame: FrameT, streaming: bool) -> FrameT: ...

    @abstractmethod
    def predicate_to_sql(self, predicate: Any, params: Any) -> str: ...

    # ------------------------------------------------------------------------
    # HELPERS COMPARTIDOS
    # ------------------------------------------------------------------------

    def _is_first_run(self, existing_schema: SchemaT | None, mode: SchemaMode) -> bool:
        """Determina si es primera escritura (crear tabla nueva)."""
        return existing_schema is None and mode is SchemaMode.OVERWRITE
```

## 📦 API Pública (Stable)

```python
# loom/etl/backends/__init__.py

# Spark
from loom.etl.backends.spark import SparkBackend

# Polars
from loom.etl.backends.polars import PolarsBackend

# Writers (dispatchers simplificados)
from loom.etl.backends.spark import SparkTargetWriter
from loom.etl.backends.polars import PolarsTargetWriter

# Readers
from loom.etl.backends.spark import SparkSourceReader
from loom.etl.backends.polars import PolarsSourceReader
```

## 🔥 Uso del Usuario (Sin Cambios)

```python
from loom.etl.backends.spark import SparkTargetWriter, SparkSourceReader
from loom.etl import ETLStep, FromTable, IntoTable

# Configuración
writer = SparkTargetWriter(spark, locator="s3://bucket/delta/")
reader = SparkSourceReader(spark, locator="s3://bucket/delta/")

# Step
class MyStep(ETLStep):
    orders = FromTable("raw.orders")
    target = IntoTable("curated.orders").append()

    def execute(self, params, *, orders):
        return orders.filter(orders.amount > 100)

# Ejecución
from loom.etl import ETLExecutor
executor = ETLExecutor(reader, writer)
executor.run_step(MyStep, params)
```

## ✅ Checklist Implementación

### Fase 1: Base y Tipos
- [ ] Crear `backends/_base.py` con `ExecutionBackend`
- [ ] Crear `types/_converters.py` con conversiones de tipos
- [ ] Mover `_dtype.py` de spark/polars a `types/`

### Fase 2: Implementaciones
- [ ] Crear `backends/spark.py` con `SparkBackend`
- [ ] Crear `backends/polars.py` con `PolarsBackend`

### Fase 3: Executor
- [ ] Modificar `storage/write/_executor.py` para usar `ExecutionBackend`
- [ ] Simplificar `SparkTargetWriter` (dispatcher solo)
- [ ] Simplificar `PolarsTargetWriter` (dispatcher solo)

### Fase 4: Cleanup
- [ ] Eliminar 14 ficheros obsoletos
- [ ] Actualizar imports en `__init__.py`
- [ ] Tests pasan

### Fase 5: Compatibilidad
- [ ] Verificar API pública sin cambios
- [ ] Documentar cambios internos
