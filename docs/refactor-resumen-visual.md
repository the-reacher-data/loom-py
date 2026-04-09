# Resumen Visual: Refactorización ETL

## 🌳 Árbol de Ficheros

### ANTES (26 ficheros, ~3,200 líneas)

```
loom/etl/
├── backends/
│   ├── spark/
│   │   ├── __init__.py
│   │   ├── _catalog.py          # 101 líneas
│   │   ├── _dtype.py            # 126 líneas
│   │   ├── _reader.py           # 148 líneas
│   │   ├── _schema.py           # 131 líneas
│   │   └── writer/
│   │       ├── __init__.py
│   │       ├── core.py          # 115 líneas  ← DUPLICADO
│   │       ├── table.py         # 94 líneas   ← ADAPTER sin valor
│   │       ├── file.py          # 85 líneas
│   │       └── exec.py          # 313 líneas  ← DUPLICADO
│   └── polars/
│       ├── __init__.py
│       ├── _catalog.py          # 101 líneas
│       ├── _dtype.py            # 257 líneas
│       ├── _reader.py           # 258 líneas
│       ├── _schema.py           # 143 líneas
│       └── writer/
│           ├── __init__.py
│           ├── core.py          # 107 líneas  ← DUPLICADO
│           ├── table.py         # 90 líneas   ← ADAPTER sin valor
│           ├── file.py          # 136 líneas
│           └── exec.py          # 279 líneas  ← DUPLICADO
├── storage/
│   └── write/
│       ├── __init__.py
│       ├── exec.py              # 32 líneas   ← Protocolo
│       ├── plan.py              # 192 líneas  ← INDIRECCIÓN innecesaria
│       ├── ops.py               # 89 líneas   ← Dataclasses intermedias
│       └── generic.py           # 58 líneas   ← ADAPTER sin valor
└── ...
```

### DESPUÉS (14 ficheros, ~1,400 líneas)

```
loom/etl/
├── backends/
│   ├── spark/
│   │   ├── __init__.py          # Exports compatibles
│   │   ├── _catalog.py          # SE MANTIENE
│   │   ├── _dtype.py            # SE MANTIENE
│   │   ├── _reader.py           # REFACTOR (simplificado)
│   │   ├── _schema.py           # SE MANTIENE
│   │   ├── _native.py           # NUEVO: Operaciones nativas
│   │   └── _writer.py           # REFACTOR: Usa executor común
│   └── polars/
│       ├── __init__.py          # Exports compatibles
│       ├── _catalog.py          # SE MANTIENE
│       ├── _dtype.py            # SE MANTIENE
│       ├── _reader.py           # REFACTOR (simplificado)
│       ├── _schema.py           # SE MANTIENE
│       ├── _native.py           # NUEVO: Operaciones nativas
│       └── _writer.py           # REFACTOR: Usa executor común
├── execution/                   # NUEVA CARPETA
│   ├── __init__.py
│   └── _executor.py             # ÚNICO executor genérico
└── ...
```

## 🗑️ Ficheros que DESAPARECEN (12 ficheros, -1,800 líneas)

| Fichero | Líneas | Razón |
|---------|--------|-------|
| `spark/writer/core.py` | 115 | Lógica duplicada con polars |
| `spark/writer/table.py` | 94 | Adapter passthrough 100% |
| `spark/writer/exec.py` | 313 | Unificado en execution/_executor.py |
| `polars/writer/core.py` | 107 | Lógica duplicada con spark |
| `polars/writer/table.py` | 90 | Adapter passthrough 100% |
| `polars/writer/exec.py` | 279 | Unificado en execution/_executor.py |
| `storage/write/plan.py` | 192 | Indirección: Spec→Op innecesaria |
| `storage/write/ops.py` | 89 | Dataclasses intermedias eliminadas |
| `storage/write/generic.py` | 58 | Adapter sin valor real |
| `storage/write/exec.py` | 32 | Protocolo simplificado |
| `spark/writer/file.py` | 85 | Movido a _native.py |
| `polars/writer/file.py` | 136 | Movido a _native.py |

**Total: -1,800 líneas (-56%)**

## 🎯 Beneficios

### 1. Duplicación Eliminada

```python
# ANTES: Mismo código en 2 sitios
def _exec_append(self, frame, op, params):  # spark/exec.py:62-65
    locator = _locator_for_target(op.target)
    validated = _validated_frame(frame, op.existing_schema)
    _write_frame(validated, op, params, locator)

def _exec_append(self, frame, op, params):  # polars/exec.py:62-67
    locator = _locator_for_target(op.target)
    validated = _validated_frame(frame, op.existing_schema)
    _write_frame(_collect_frame(validated), op, params, locator)

# DESPUÉS: Una sola implementación
class WriteExecutor:
    def _exec_append(self, native, frame, spec, target, existing_schema):
        validated = native.validate_schema(frame, existing_schema, spec.schema_mode)
        native.append(validated, target, schema_mode=spec.schema_mode)
```

### 2. Flujo Simplificado

```
ANTES (7 pasos):
IntoTable().append()
  → AppendSpec
    → WritePlanner.plan()        ← Paso extra
      → AppendOp
        → GenericTargetWriter.write()
          → SparkWriteExecutor.execute()
            → SparkWriteExecutor._exec_append()
              → _write_frame()

DESPUÉS (4 pasos):
IntoTable().append()
  → AppendSpec
    → SparkTargetWriter.write()   # Dispatcher simple
      → WriteExecutor.execute()   # Orquestación común
        → SparkNativeOps.append() # Operación nativa
```

### 3. Responsabilidades Claras

| Componente | Antes | Después |
|------------|-------|---------|
| `WritePlanner` | Transformar Spec→Op | ❌ Eliminado |
| `WriteExecutor` | 2 clases duplicadas | 1 clase genérica |
| `*DeltaTableWriter` | 2 adapters passthrough | ❌ Eliminados |
| `*NativeOps` | No existía | 2 clases con operaciones reales |

### 4. API Pública Estable

```python
# El usuario NO nota diferencia
from loom.etl.backends.spark import SparkTargetWriter

writer = SparkTargetWriter(spark, locator)
writer.write(frame, spec, params)  # Igual que antes
```

## 📊 Métricas

| Métrica | Antes | Después | Cambio |
|---------|-------|---------|--------|
| **Ficheros escritura** | 12 | 4 | -67% |
| **Líneas escritura** | ~1,800 | ~600 | -67% |
| **Clases escritura** | 10 | 3 | -70% |
| **Niveles indirección** | 7 | 4 | -43% |
| **Duplicación Spark/Polars** | 70% | 0% | -100% |

## 🔧 Implementación Propuesta

### Paso 1: Crear execution/_executor.py
```python
class WriteExecutor(Generic[FrameT]):
    """ÚNICO executor para todos los backends."""

    def __init__(self, native: NativeOperations[FrameT]):
        self._native = native

    def execute(self, frame, spec, target, existing_schema, params):
        match spec:
            case AppendSpec():
                return self._exec_append(frame, spec, target, existing_schema)
            case ReplaceSpec():
                return self._exec_replace(frame, spec, target, existing_schema)
            # ... etc
```

### Paso 2: Crear backends/spark/_native.py
```python
class SparkNativeOperations:
    """Operaciones Spark con imports al toplevel."""

    from delta.tables import DeltaTable  # Import aquí, NO en métodos

    def append(self, frame, target, *, schema_mode): ...
    def replace(self, frame, target, *, schema_mode): ...
    def upsert(self, frame, target, *, keys, existing_schema):
        # Decisión CREATE vs MERGE aquí
        if existing_schema is None:
            # CREATE
        else:
            # MERGE vía DeltaTable
```

### Paso 3: Simplificar backends/spark/_writer.py
```python
class SparkTargetWriter:
    """Dispatcher simplificado."""

    def __init__(self, spark, locator):
        self._resolver = PathRouteResolver(locator)
        self._schema_reader = SparkSchemaReader(spark)
        self._executor = WriteExecutor(SparkNativeOperations(spark))

    def write(self, frame, spec, params):
        target = self._resolver.resolve(spec.table_ref)
        existing_schema = self._schema_reader.read_schema(target)
        self._executor.execute(frame, spec, target, existing_schema, params)
```

### Paso 4: Eliminar
- `writer/core.py` → Lógica en executor
- `writer/table.py` → Adapter innecesario
- `writer/exec.py` → Reemplazado por executor único
- `storage/write/plan.py` → Indirección eliminada
- `storage/write/ops.py` → Dataclasses innecesarias
- `storage/write/generic.py` → Adapter innecesario

## ✅ Checklist

- [ ] Crear `execution/_executor.py` (único executor)
- [ ] Crear `spark/_native.py` (operaciones nativas)
- [ ] Crear `polars/_native.py` (operaciones nativas)
- [ ] Refactorizar `spark/_writer.py` (simplificado)
- [ ] Refactorizar `polars/_writer.py` (simplificado)
- [ ] Eliminar 12 ficheros obsoletos
- [ ] Actualizar tests
- [ ] Verificar compatibilidad API pública
