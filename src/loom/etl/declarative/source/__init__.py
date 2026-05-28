"""ETL source declaration API.

Builders (user-facing):

* :class:`FromTable`  — Delta table source
* :class:`FromFile`   — file source (CSV, JSON, XLSX, Parquet)
* :class:`FromTemp`   — intermediate store source
* :class:`FromMongo`  — MongoDB collection source
* :class:`FromClickHouse` — ClickHouse table/view source
* :class:`SourceRef`  — cross-step column reference for ``isin()``
* :class:`Sources`    — named group of sources
* :class:`SourceSet`  — reusable, extensible source group

Spec types (internal — compiler and executor only):

* :class:`TableSourceSpec`
* :class:`FileSourceSpec`
* :class:`TempSourceSpec`
* :class:`MongoSourceSpec`
* :class:`ClickHouseSourceSpec`
* :data:`SourceSpec`   — union type alias
* :class:`SourceKind`  — physical kind enum
* :class:`JsonColumnSpec`
"""

from loom.etl.declarative.source._from import FromFile, FromTable, FromTemp, Sources, SourceSet
from loom.etl.declarative.source._from_clickhouse import FromClickHouse
from loom.etl.declarative.source._from_mongo import FromMongo, SourceRef
from loom.etl.declarative.source._specs import (
    ClickHouseSourceSpec,
    FileSourceSpec,
    JsonColumnSpec,
    MongoSourceSpec,
    SourceKind,
    SourceSpec,
    TableSourceSpec,
    TempSourceSpec,
)

__all__ = [
    "FromTable",
    "FromFile",
    "FromTemp",
    "FromMongo",
    "FromClickHouse",
    "SourceRef",
    "Sources",
    "SourceSet",
    "SourceSpec",
    "SourceKind",
    "TableSourceSpec",
    "FileSourceSpec",
    "TempSourceSpec",
    "MongoSourceSpec",
    "ClickHouseSourceSpec",
    "JsonColumnSpec",
]
