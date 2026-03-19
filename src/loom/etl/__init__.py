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

Compilation::

    from loom.etl.compiler import ETLCompiler, ETLCompilationError

Internal modules (``_*.py``) and ``loom.etl.compiler._*`` are not part of
the public API and may change without notice.
"""

from loom.etl._format import Format
from loom.etl._params import ETLParams
from loom.etl._pipeline import ETLPipeline
from loom.etl._process import ETLProcess
from loom.etl._proxy import ParamExpr, params
from loom.etl._source import FromFile, FromTable, Sources, SourceSet
from loom.etl._step import ETLStep
from loom.etl._table import TableRef, col
from loom.etl._target import IntoFile, IntoTable

__all__ = [
    # params
    "ETLParams",
    # step / process / pipeline
    "ETLStep",
    "ETLProcess",
    "ETLPipeline",
    # sources
    "Sources",
    "SourceSet",
    "FromTable",
    "FromFile",
    # targets
    "IntoTable",
    "IntoFile",
    # format
    "Format",
    # table / column refs
    "TableRef",
    "col",
    # params proxy
    "params",
    "ParamExpr",
]
