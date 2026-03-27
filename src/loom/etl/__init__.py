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

    from loom.etl import io, pipeline, runner, schema, sql, storage, temp

Internal modules (``_*.py``) and ``loom.etl.compiler._*`` are not part of
the public API and may change without notice.
"""

from loom.etl.io import (
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
    ParquetReadOptions,
    ParquetWriteOptions,
    ReadOptions,
    SchemaMode,
    Sources,
    SourceSet,
    WriteOptions,
)
from loom.etl.pipeline import ETLParams, ETLPipeline, ETLProcess, ETLStep, ParamExpr, params
from loom.etl.runner import ETLRunner, InvalidStageError
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
from loom.etl.sql import StepSQL
from loom.etl.storage import (
    DeltaConfig,
    MappingLocator,
    ObservabilityConfig,
    PrefixLocator,
    RunSinkConfig,
    SourceReader,
    StorageBackend,
    StorageConfig,
    TableDiscovery,
    TableLocation,
    TableLocator,
    TargetWriter,
    UnityCatalogConfig,
)
from loom.etl.temp import (
    AutoTempCleaner,
    DbutilsTempCleaner,
    FsspecTempCleaner,
    IntermediateStore,
    LocalTempCleaner,
    TempCleaner,
    TempScope,
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
    "TempScope",
    "IntermediateStore",
    # temp cleaners
    "TempCleaner",
    "LocalTempCleaner",
    "FsspecTempCleaner",
    "DbutilsTempCleaner",
    "AutoTempCleaner",
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
    "StorageBackend",
    "StorageConfig",
    "DeltaConfig",
    "UnityCatalogConfig",
    # observability config (YAML-loadable)
    "ObservabilityConfig",
    "RunSinkConfig",
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
