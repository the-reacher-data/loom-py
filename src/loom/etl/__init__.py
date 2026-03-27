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

Internal modules (``_*.py``) and ``loom.etl.compiler._*`` are not part of
the public API and may change without notice.
"""

from loom.etl.io._format import Format
from loom.etl.io._read_options import (
    CsvReadOptions,
    ExcelReadOptions,
    JsonReadOptions,
    ParquetReadOptions,
    ReadOptions,
)
from loom.etl.io._source import FromFile, FromTable, FromTemp, Sources, SourceSet
from loom.etl.io._target import IntoFile, IntoTable, IntoTemp, SchemaMode
from loom.etl.io._write_options import CsvWriteOptions, ParquetWriteOptions, WriteOptions
from loom.etl.model._params import ETLParams
from loom.etl.model._pipeline import ETLPipeline
from loom.etl.model._process import ETLProcess
from loom.etl.model._proxy import ParamExpr, params
from loom.etl.model._step import ETLStep
from loom.etl.runner import ETLRunner, InvalidStageError
from loom.etl.schema._schema import (
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
)
from loom.etl.schema._table import TableRef, col
from loom.etl.sql._step_sql import StepSQL
from loom.etl.storage._config import DeltaConfig, StorageBackend, StorageConfig, UnityCatalogConfig
from loom.etl.storage._io import SourceReader, TableDiscovery, TargetWriter
from loom.etl.storage._locator import MappingLocator, PrefixLocator, TableLocation, TableLocator
from loom.etl.storage._observability import ObservabilityConfig, RunSinkConfig
from loom.etl.temp._cleaners import (
    AutoTempCleaner,
    DbutilsTempCleaner,
    FsspecTempCleaner,
    LocalTempCleaner,
    TempCleaner,
)
from loom.etl.temp._scope import TempScope
from loom.etl.temp._store import IntermediateStore

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
