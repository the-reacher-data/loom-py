"""ETL target variant specs.

Each variant carries exactly the fields required for its write semantic.
The union :data:`TargetSpec` is the type used throughout the compiler,
executor, and backend writers.

TABLE targets (Delta Lake):

* :class:`AppendSpec`             — append rows
* :class:`ReplaceSpec`            — full table overwrite
* :class:`ReplacePartitionsSpec`  — overwrite partitions present in the frame
* :class:`ReplaceWhereSpec`       — overwrite rows matching a static predicate
* :class:`UpsertSpec`             — MERGE with explicit merge keys

Non-table targets:

* :class:`FileSpec`       — write to a file (CSV / JSON / Parquet / XLSX)
* :class:`TempSpec`       — write to intermediate store (single writer)
* :class:`TempFanInSpec`  — write to intermediate store (fan-in; multiple writers)
"""

from loom.etl.io.target._file import FileSpec
from loom.etl.io.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpsertSpec,
)
from loom.etl.io.target._temp import TempFanInSpec, TempSpec

TargetSpec = (
    AppendSpec
    | ReplaceSpec
    | ReplacePartitionsSpec
    | ReplaceWhereSpec
    | UpsertSpec
    | FileSpec
    | TempSpec
    | TempFanInSpec
)
"""Union of all concrete target spec variants.

Used as the public type annotation for any value produced by
:meth:`IntoTable._to_spec`, :meth:`IntoFile._to_spec`, or
:meth:`IntoTemp._to_spec`.
"""

__all__ = [
    "TargetSpec",
    "AppendSpec",
    "ReplaceSpec",
    "ReplacePartitionsSpec",
    "ReplaceWhereSpec",
    "UpsertSpec",
    "FileSpec",
    "TempSpec",
    "TempFanInSpec",
]
