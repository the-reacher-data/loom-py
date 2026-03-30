"""PolarsDeltaWriter — TargetWriter backed by Polars + delta-rs.

Enforces schema validation/evolution via ``apply_schema`` before each write.
The existing schema is read directly from the Delta transaction log —
no external catalog is consulted or updated.

OVERWRITE mode is the sole exception to the "schema must be registered" rule:
the table and its schema are created from the frame on first write, which
is safe because the user explicitly requested a full schema replacement.

All write-time options (``storage_options``, ``writer_properties``,
``configuration``, ``commit_properties``) are resolved from the
:class:`~loom.etl._locator.TableLocation` returned by the locator and
forwarded **verbatim** to delta-rs.

See https://delta-io.github.io/delta-rs/api/delta_writer/ for the full
list of accepted parameters.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import polars as pl
from deltalake import write_deltalake

from loom.etl.backends.polars._schema import apply_schema
from loom.etl.io._format import Format as _Format
from loom.etl.io._target import SchemaMode
from loom.etl.io._write_options import CsvWriteOptions, ParquetWriteOptions
from loom.etl.io.target import TargetSpec
from loom.etl.io.target._file import FileSpec
from loom.etl.io.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpsertSpec,
)
from loom.etl.schema._table import TableRef
from loom.etl.sql._predicate_sql import predicate_to_sql
from loom.etl.sql._upsert import (
    SOURCE_ALIAS,
    TARGET_ALIAS,
    _build_insert_values,
    _build_partition_predicate,
    _build_update_set,
    _build_upsert_predicate,
    _build_upsert_update_cols,
    _log_partition_combos,
    _warn_no_partition_cols,
)
from loom.etl.storage._locator import TableLocation, TableLocator, _as_locator

_log = logging.getLogger(__name__)


class PolarsDeltaWriter:
    """Write ETL step results to Delta tables using Polars + delta-rs.

    Implements :class:`~loom.etl._io.TargetWriter`.

    Validates or evolves the frame schema via :func:`~loom.etl.backends.polars._schema.apply_schema`
    before each write.  The Delta transaction log is the authoritative schema
    store — no explicit catalog update is needed after a successful write.

    OVERWRITE mode is the only mode that may create a new table from scratch
    — the caller explicitly accepts whatever schema the frame carries.

    Args:
        locator:  Root URI string, :class:`pathlib.Path`, or any
                  :class:`~loom.etl._locator.TableLocator`.  A plain string or
                  path is wrapped in :class:`~loom.etl._locator.PrefixLocator`
                  automatically.
        catalog:  Catalog used for schema lookup and post-write update.

    Example::

        from loom.etl.backends.polars import PolarsDeltaWriter

        writer = PolarsDeltaWriter("s3://my-lake/")
        writer.write(frame, spec, params)
    """

    def __init__(self, locator: str | os.PathLike[str] | TableLocator) -> None:
        self._locator = _as_locator(locator)

    def write(self, frame: pl.LazyFrame, spec: TargetSpec, params_instance: Any) -> None:
        """Validate schema and write the frame to the declared target.

        Args:
            frame:           Lazy frame produced by the step's ``execute()``.
            spec:            Compiled target spec variant.
            params_instance: Concrete params for predicate resolution.

        Raises:
            SchemaNotFoundError: When the table has no registered schema and
                                 mode is not OVERWRITE.
            SchemaError:         When the frame violates the registered schema.
            TypeError:           If an unsupported spec type is received.
        """
        if isinstance(spec, UpsertSpec):
            self._write_upsert_delta(frame, spec)
        elif isinstance(spec, (AppendSpec, ReplaceSpec, ReplacePartitionsSpec, ReplaceWhereSpec)):
            self._write_delta(frame, spec, params_instance)
        elif isinstance(spec, FileSpec):
            self._write_file(frame, spec)
        else:
            raise TypeError(f"PolarsDeltaWriter: unsupported spec type: {type(spec)!r}")

    def _write_delta(
        self,
        frame: pl.LazyFrame,
        spec: AppendSpec | ReplaceSpec | ReplacePartitionsSpec | ReplaceWhereSpec,
        params_instance: Any,
    ) -> None:
        table_ref = spec.table_ref
        _log.debug(
            "write delta table=%s schema_mode=%s",
            table_ref.ref,
            spec.schema_mode,
        )
        existing_schema = _native_polars_schema(self._locator, table_ref)

        if existing_schema is None and spec.schema_mode is SchemaMode.OVERWRITE:
            _log.debug("write delta new table table=%s (no prior schema, OVERWRITE)", table_ref.ref)
            self._write_frame(frame.collect(), spec, params_instance)
            return

        validated = apply_schema(frame, existing_schema, spec.schema_mode)
        self._write_frame(validated.collect(), spec, params_instance)

    def _write_upsert_delta(self, frame: pl.LazyFrame, spec: UpsertSpec) -> None:
        table_ref = spec.table_ref
        existing_schema = _native_polars_schema(self._locator, table_ref)

        if existing_schema is None:
            _log.debug("upsert delta first run — creating table=%s", table_ref.ref)
            loc = self._locator.locate(table_ref)
            _first_run_overwrite_polars(loc, frame.collect())
            return

        validated = apply_schema(frame, existing_schema, spec.schema_mode)
        df = validated.collect()
        loc = self._locator.locate(table_ref)
        _merge_polars(loc, df, spec)

    def _write_file(self, frame: pl.LazyFrame, spec: FileSpec) -> None:
        _log.debug("write file path=%s format=%s", spec.path, spec.format)
        _FILE_WRITERS[spec.format](frame.collect(), spec.path, spec.write_options)

    def _write_frame(
        self,
        df: pl.DataFrame,
        spec: AppendSpec | ReplaceSpec | ReplacePartitionsSpec | ReplaceWhereSpec,
        params_instance: Any,
    ) -> None:
        loc = self._locator.locate(spec.table_ref)
        match spec:
            case AppendSpec():
                write_deltalake(loc.uri, df.to_arrow(), mode="append", **_write_kwargs(loc))
            case ReplaceSpec():
                write_deltalake(loc.uri, df.to_arrow(), mode="overwrite", **_write_kwargs(loc))
            case ReplacePartitionsSpec():
                _write_replace_partitions(loc, df, spec)
            case ReplaceWhereSpec():
                _write_replace_where(loc, df, spec, params_instance)


def _native_polars_schema(locator: TableLocator, ref: TableRef) -> pl.Schema | None:
    """Return the native Polars schema for the Delta table at *ref*, or ``None``.

    Reads the Delta transaction log via delta-rs and converts the PyArrow
    schema to a Polars schema without any intermediate LoomType conversion.
    Returns ``None`` when the table does not yet exist.

    Args:
        locator: Resolves the logical table reference to a physical URI.
        ref:     Logical table reference.

    Returns:
        Native :class:`~polars.Schema` reflecting the on-disk column types, or
        ``None`` when the table has not been written yet.
    """
    import pyarrow as pa
    from deltalake import DeltaTable
    from deltalake.exceptions import TableNotFoundError

    loc = locator.locate(ref)
    try:
        dt = DeltaTable(loc.uri, storage_options=loc.storage_options or None)
    except TableNotFoundError:
        return None
    arrow_schema: pa.Schema = dt.schema().to_pyarrow()
    return pl.DataFrame(pa.Table.from_batches([], schema=arrow_schema)).schema


def _write_kwargs(loc: TableLocation) -> dict[str, Any]:
    """Build delta-rs keyword arguments from a TableLocation.

    All option dicts are forwarded verbatim — Loom does not validate or
    restrict their contents.  Invalid keys surface as errors from delta-rs.
    See https://delta-io.github.io/delta-rs/api/delta_writer/
    """
    from deltalake import CommitProperties
    from deltalake.table import WriterProperties

    return {
        "storage_options": loc.storage_options or None,
        "configuration": loc.delta_config or None,
        "writer_properties": WriterProperties(**loc.writer) if loc.writer else None,
        "commit_properties": CommitProperties(**loc.commit) if loc.commit else None,
    }


def _write_replace_partitions(
    loc: TableLocation, df: pl.DataFrame, spec: ReplacePartitionsSpec
) -> None:
    if df.is_empty():
        _log.warning("replace_partitions table=%s has 0 rows — nothing written", loc.uri)
        return
    predicate = _build_partition_predicate(
        df.select(list(spec.partition_cols)).unique().iter_rows(named=True),
        spec.partition_cols,
    )
    write_deltalake(
        loc.uri, df.to_arrow(), mode="overwrite", predicate=predicate, **_write_kwargs(loc)
    )


def _write_replace_where(
    loc: TableLocation, df: pl.DataFrame, spec: ReplaceWhereSpec, params: Any
) -> None:
    predicate = predicate_to_sql(spec.replace_predicate, params)
    write_deltalake(
        loc.uri, df.to_arrow(), mode="overwrite", predicate=predicate, **_write_kwargs(loc)
    )


def _write_csv_file(df: pl.DataFrame, path: str, options: Any) -> None:
    opts = options if isinstance(options, CsvWriteOptions) else CsvWriteOptions()
    df.write_csv(path, separator=opts.separator, include_header=opts.has_header)


def _write_parquet_file(df: pl.DataFrame, path: str, options: Any) -> None:
    opts = options if isinstance(options, ParquetWriteOptions) else ParquetWriteOptions()
    df.write_parquet(path, compression=opts.compression)


def _write_json_file(df: pl.DataFrame, path: str, _options: Any) -> None:
    df.write_ndjson(path)


_FILE_WRITERS: dict[_Format, Any] = {
    _Format.CSV: _write_csv_file,
    _Format.PARQUET: _write_parquet_file,
    _Format.JSON: _write_json_file,
}


def _first_run_overwrite_polars(loc: TableLocation, df: pl.DataFrame) -> None:
    """Create the Delta table on the first UPSERT run (no existing table).

    Uses a plain overwrite so the table and its schema are initialised from
    the frame.  Subsequent runs will use MERGE instead.
    """
    write_deltalake(loc.uri, df.to_arrow(), mode="overwrite", **_write_kwargs(loc))


def _collect_partition_combos_polars(
    df: pl.DataFrame,
    partition_cols: tuple[str, ...],
) -> list[dict[str, Any]]:
    """Collect distinct partition value combinations from the source frame.

    Uses ``df.unique(subset=…)`` — purely in-memory, no I/O.  Partition
    columns have low cardinality by design so the result is always small.

    Args:
        df:             Source frame.
        partition_cols: Partition column names.

    Returns:
        List of dicts, one per distinct partition combination.
    """
    return df.unique(subset=list(partition_cols)).select(list(partition_cols)).to_dicts()


def _merge_polars(loc: TableLocation, df: pl.DataFrame, spec: UpsertSpec) -> None:
    """Execute a Delta MERGE (UPSERT) for the Polars backend.

    Args:
        loc:  Resolved table location (URI + storage options).
        df:   Collected source frame.
        spec: Upsert spec carrying keys, partition cols, and exclude/include.
    """
    from deltalake import DeltaTable

    table_ref_str = spec.table_ref.ref
    combos = _collect_partition_combos_for_merge_polars(df, spec.partition_cols, table_ref_str)
    predicate = _build_upsert_predicate(combos, spec, TARGET_ALIAS, SOURCE_ALIAS)
    update_cols = _build_upsert_update_cols(tuple(df.columns), spec)
    update_set = _build_update_set(update_cols, SOURCE_ALIAS)
    insert_values = _build_insert_values(tuple(df.columns), SOURCE_ALIAS)

    dt = DeltaTable(loc.uri, storage_options=loc.storage_options or {})
    (
        dt.merge(
            source=df.to_arrow(),
            predicate=predicate,
            source_alias=SOURCE_ALIAS,
            target_alias=TARGET_ALIAS,
        )
        .when_matched_update(updates=update_set)
        .when_not_matched_insert(updates=insert_values)
        .execute()
    )


def _collect_partition_combos_for_merge_polars(
    df: pl.DataFrame,
    partition_cols: tuple[str, ...],
    table_ref: str,
) -> list[dict[str, Any]]:
    """Collect partition combos and emit observability signals.

    Emits a WARNING when no partition columns are declared (full table scan),
    and a DEBUG message with the number of combos otherwise.

    Args:
        df:             Source frame.
        partition_cols: Partition column names.
        table_ref:      Logical table reference for log messages.

    Returns:
        List of partition combination dicts (empty when no partition cols).
    """
    if not partition_cols:
        _warn_no_partition_cols(table_ref)
        return []
    combos = _collect_partition_combos_polars(df, partition_cols)
    _log_partition_combos(combos, table_ref)
    return combos
