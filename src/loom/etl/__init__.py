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

from loom.etl._format import Format
from loom.etl._io import SourceReader, TableDiscovery, TargetWriter
from loom.etl._locator import MappingLocator, PrefixLocator, TableLocation, TableLocator
from loom.etl._observability_config import ObservabilityConfig, RunSinkConfig
from loom.etl._params import ETLParams
from loom.etl._pipeline import ETLPipeline
from loom.etl._process import ETLProcess
from loom.etl._proxy import ParamExpr, params
from loom.etl._read_options import (
    CsvReadOptions,
    ExcelReadOptions,
    JsonReadOptions,
    ParquetReadOptions,
    ReadOptions,
)
from loom.etl._runner import ETLRunner, InvalidStageError
from loom.etl._schema import (
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
from loom.etl._source import FromFile, FromTable, FromTemp, Sources, SourceSet
from loom.etl._step import ETLStep
from loom.etl._step_sql import StepSQL
from loom.etl._storage_config import DeltaConfig, StorageBackend, StorageConfig, UnityCatalogConfig
from loom.etl._table import TableRef, col
from loom.etl._target import IntoFile, IntoTable, IntoTemp, SchemaMode
from loom.etl._temp import TempScope
from loom.etl._temp_cleaners import (
    AutoTempCleaner,
    DbutilsTempCleaner,
    FsspecTempCleaner,
    LocalTempCleaner,
    TempCleaner,
)
from loom.etl._temp_store import IntermediateStore
from loom.etl._write_options import CsvWriteOptions, ParquetWriteOptions, WriteOptions

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
