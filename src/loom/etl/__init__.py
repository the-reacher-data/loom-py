"""Loom ETL — declarative, compile-time-validated ETL framework.

Public API
----------

Authoring::

    from loom.etl import (
        ETLParams,
        ETLStep,
        ETLProcess,
        ETLPipeline,
        Sources,
        SourceSet,
        FromTable,
        FromFile,
        IntoTable,
        IntoFile,
        Format,
        TableRef,
        col,
        params,
    )

I/O protocols::

    from loom.etl import TableDiscovery, SourceReader, TargetWriter

Compilation::

    from loom.etl.compiler import ETLCompiler, ETLCompilationError

Testing stubs::

    from loom.etl.testing import StubCatalog, StubSourceReader, StubTargetWriter

Namespaced API (discoverable by bounded context)::

    from loom.etl import declarative, pipeline, runner, schema, storage, runtime

Internal modules (``_*.py``) and ``loom.etl.compiler._*`` are not part of
the public API and may change without notice.
"""

from loom.etl.checkpoint import (
    CheckpointCleaner,
    CheckpointScope,
    CheckpointStore,
)
from loom.etl.declarative import (
    CsvReadOptions,
    CsvWriteOptions,
    ExcelReadOptions,
    Format,
    FromFile,
    FromTable,
    FromTemp,
    IntoFile,
    IntoTable,
    IntoTemp,
    JsonReadOptions,
    JsonWriteOptions,
    ParquetReadOptions,
    ParquetWriteOptions,
    ReadOptions,
    SchemaMode,
    Sources,
    SourceSet,
    WriteOptions,
)
from loom.etl.observability import ExecutionRecordStoreConfig, ObservabilityConfig, OtelConfig
from loom.etl.pipeline import (
    ETLParams,
    ETLPipeline,
    ETLProcess,
    ETLStep,
    ParamExpr,
    StepSQL,
    params,
)
from loom.etl.runner import ETLRunner, InvalidStageError
from loom.etl.runtime.contracts import SourceReader, TableDiscovery, TargetWriter
from loom.etl.schema import (
    ArrayType,
    CategoricalType,
    ColumnSchema,
    DatetimeType,
    DecimalType,
    DurationType,
    EnumType,
    ListType,
    LoomDtype,
    LoomType,
    SchemaError,
    SchemaNotFoundError,
    StructField,
    StructType,
    TableRef,
    col,
)
from loom.etl.storage import (
    CatalogConnection,
    FilePathConfig,
    FileRoute,
    MappingLocator,
    PrefixLocator,
    StorageConfig,
    StorageDefaults,
    StorageEngine,
    TableLocation,
    TableLocator,
    TablePathConfig,
    TableRoute,
)

__all__ = [
    # params
    "ETLParams",
    # step / process / pipeline
    "ETLStep",
    "StepSQL",
    "ETLProcess",
    "ETLPipeline",
    # runner
    "ETLRunner",
    "InvalidStageError",
    # sources
    "Sources",
    "SourceSet",
    "FromTable",
    "FromFile",
    "FromTemp",
    # targets
    "IntoTable",
    "IntoFile",
    "IntoTemp",
    # intermediates
    "CheckpointCleaner",
    "CheckpointScope",
    "CheckpointStore",
    # format
    "Format",
    # read options
    "ReadOptions",
    "CsvReadOptions",
    "JsonReadOptions",
    "ExcelReadOptions",
    "ParquetReadOptions",
    # write options
    "WriteOptions",
    "CsvWriteOptions",
    "JsonWriteOptions",
    "ParquetWriteOptions",
    # table / column refs
    "TableRef",
    "col",
    # params proxy
    "params",
    "ParamExpr",
    # I/O protocols
    "TableDiscovery",
    "SourceReader",
    "TargetWriter",
    # storage locator
    "TableLocation",
    "TableLocator",
    "PrefixLocator",
    "MappingLocator",
    # storage config (YAML-loadable)
    "StorageEngine",
    "StorageConfig",
    "StorageDefaults",
    "CatalogConnection",
    "TablePathConfig",
    "TableRoute",
    "FilePathConfig",
    "FileRoute",
    # observability config (YAML-loadable)
    "ObservabilityConfig",
    "OtelConfig",
    "ExecutionRecordStoreConfig",
    # schema — primitive
    "SchemaMode",
    "ColumnSchema",
    "LoomDtype",
    "LoomType",
    "SchemaNotFoundError",
    "SchemaError",
    # schema — complex / structural types
    "ListType",
    "ArrayType",
    "StructType",
    "StructField",
    "DecimalType",
    "DatetimeType",
    "DurationType",
    "CategoricalType",
    "EnumType",
]
