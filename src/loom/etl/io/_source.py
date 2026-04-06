"""Backward-compatibility shim — canonical location is :mod:`loom.etl.io.source`.

This module is kept for reload-contract tests.  New code should import from
:mod:`loom.etl.io.source` or :mod:`loom.etl.io` directly.
"""

from loom.etl.io._format import Format  # noqa: F401 — preserved for attribute access compat
from loom.etl.io.source import (
    FileSourceSpec,
    FromFile,
    FromTable,
    FromTemp,
    JsonColumnSpec,
    SourceKind,
    Sources,
    SourceSet,
    SourceSpec,
    TableSourceSpec,
    TempSourceSpec,
)

__all__ = [
    "SourceSpec",
    "SourceKind",
    "TableSourceSpec",
    "FileSourceSpec",
    "TempSourceSpec",
    "JsonColumnSpec",
    "FromTable",
    "FromFile",
    "FromTemp",
    "Sources",
    "SourceSet",
]
