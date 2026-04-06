"""ETL source declaration API.

Builders (user-facing):

* :class:`FromTable`  — Delta table source
* :class:`FromFile`   — file source (CSV, JSON, XLSX, Parquet)
* :class:`FromTemp`   — intermediate store source
* :class:`Sources`    — named group of sources
* :class:`SourceSet`  — reusable, extensible source group

Spec types (internal — compiler and executor only):

* :class:`TableSourceSpec`
* :class:`FileSourceSpec`
* :class:`TempSourceSpec`
* :data:`SourceSpec`   — union type alias
* :class:`SourceKind`  — physical kind enum
* :class:`JsonColumnSpec`
"""

from loom.etl.io.source._from import FromFile, FromTable, FromTemp, Sources, SourceSet
from loom.etl.io.source._specs import (
    FileSourceSpec,
    JsonColumnSpec,
    SourceKind,
    SourceSpec,
    TableSourceSpec,
    TempSourceSpec,
)

__all__ = [
    "FromTable",
    "FromFile",
    "FromTemp",
    "Sources",
    "SourceSet",
    "SourceSpec",
    "SourceKind",
    "TableSourceSpec",
    "FileSourceSpec",
    "TempSourceSpec",
    "JsonColumnSpec",
]
