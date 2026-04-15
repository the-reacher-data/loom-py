"""ETL target declaration API — builders and compiled spec types.

Builders (user-facing):

* :class:`IntoTable`   — Delta table target
* :class:`IntoFile`    — file target (CSV, JSON, XLSX, Parquet)
* :class:`IntoTemp`    — intermediate store target
* :class:`IntoHistory` — SCD Type 2 history target
* :class:`SchemaMode`  — schema evolution strategy

Spec types (internal — compiler, executor, and backend writers):

TABLE targets (Delta Lake):

* :class:`AppendSpec`             — append rows
* :class:`ReplaceSpec`            — full table overwrite
* :class:`ReplacePartitionsSpec`  — overwrite partitions present in the frame
* :class:`ReplaceWhereSpec`       — overwrite rows matching a static predicate
* :class:`UpsertSpec`             — MERGE with explicit merge keys
* :class:`HistorifySpec`          — SCD Type 2 merge with history tracking

Non-table targets:

* :class:`FileSpec`       — write to a file (CSV / JSON / Parquet / XLSX)
* :class:`TempSpec`       — write to intermediate store (single writer)
* :class:`TempFanInSpec`  — write to intermediate store (fan-in; multiple writers)

SCD2 support types:

* :class:`HistorifyInputMode`          — snapshot vs log input semantics
* :class:`DeletePolicy`                — action for absent keys in snapshot mode
* :class:`HistoryDateType`             — date vs timestamp boundary columns
* :class:`HistorifyEngine`             — protocol for backend SCD2 engines
* :class:`HistorifyRepairReport`       — re-weave operation report
* :class:`HistorifyKeyConflictError`   — duplicate entity state vectors
* :class:`HistorifyDateCollisionError` — same-date LOG mode collisions
* :class:`HistorifyTemporalConflictError` — future-open records without re-weave
"""

from loom.etl.declarative.target._file import FileSpec
from loom.etl.declarative.target._history import (
    DeletePolicy,
    HistorifyDateCollisionError,
    HistorifyEngine,
    HistorifyInputMode,
    HistorifyKeyConflictError,
    HistorifyRepairReport,
    HistorifySpec,
    HistorifyTemporalConflictError,
    HistoryDateType,
    IntoHistory,
)
from loom.etl.declarative.target._into import IntoFile, IntoTable, IntoTemp
from loom.etl.declarative.target._schema_mode import SchemaMode
from loom.etl.declarative.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpsertSpec,
)
from loom.etl.declarative.target._temp import TempFanInSpec, TempSpec

TargetSpec = (
    AppendSpec
    | ReplaceSpec
    | ReplacePartitionsSpec
    | ReplaceWhereSpec
    | UpsertSpec
    | HistorifySpec
    | FileSpec
    | TempSpec
    | TempFanInSpec
)
"""Union of all concrete target spec variants.

Used as the public type annotation for any value produced by
:meth:`IntoTable._to_spec`, :meth:`IntoFile._to_spec`,
:meth:`IntoTemp._to_spec`, or :meth:`IntoHistory._to_spec`.
"""

__all__ = [
    # builders
    "IntoTable",
    "IntoFile",
    "IntoTemp",
    "IntoHistory",
    # schema
    "SchemaMode",
    # union
    "TargetSpec",
    # table specs
    "AppendSpec",
    "ReplaceSpec",
    "ReplacePartitionsSpec",
    "ReplaceWhereSpec",
    "UpsertSpec",
    "HistorifySpec",
    # file / temp specs
    "FileSpec",
    "TempSpec",
    "TempFanInSpec",
    # SCD2 enums
    "HistorifyInputMode",
    "DeletePolicy",
    "HistoryDateType",
    # SCD2 protocol and report
    "HistorifyEngine",
    "HistorifyRepairReport",
    # SCD2 errors
    "HistorifyKeyConflictError",
    "HistorifyDateCollisionError",
    "HistorifyTemporalConflictError",
]
