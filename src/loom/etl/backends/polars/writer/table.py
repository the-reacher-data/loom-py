"""Polars Delta table writer."""

from __future__ import annotations

import logging
import os
from typing import Any

import polars as pl
from deltalake import write_deltalake

from loom.etl.backends.polars._schema import apply_schema
from loom.etl.io.target import SchemaMode
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

_TableWriteSpec = AppendSpec | ReplaceSpec | ReplacePartitionsSpec | ReplaceWhereSpec


class PolarsDeltaTableWriter:
    """Write TABLE targets to Delta Lake using Polars + delta-rs.

    Args:
        locator: Root URI string, :class:`pathlib.Path`, or any
            :class:`~loom.etl.storage._locator.TableLocator`.

    Example::

        writer = PolarsDeltaTableWriter("s3://my-lake/")
        writer.write(frame, append_spec, params)
    """

    def __init__(self, locator: str | os.PathLike[str] | TableLocator) -> None:
        self._locator = _as_locator(locator)

    def write(
        self,
        frame: pl.LazyFrame,
        spec: object,
        params_instance: Any,
        *,
        streaming: bool = False,
    ) -> None:
        """Validate schema and write the frame to the declared table target.

        Args:
            frame: Lazy frame produced by the step's ``execute()``.
            spec: TABLE target spec variant.
            params_instance: Concrete params for predicate resolution.
            streaming: When ``True``, uses ``collect(engine=\"streaming\")``
                to reduce peak memory. Ignored for UPSERT (MERGE requires full
                materialisation).

        Raises:
            TypeError: If ``spec`` is not a TABLE target spec.
        """
        if isinstance(spec, UpsertSpec):
            self._write_upsert_delta(frame, spec, streaming=streaming)
            return
        if not isinstance(spec, (AppendSpec, ReplaceSpec, ReplacePartitionsSpec, ReplaceWhereSpec)):
            raise TypeError(
                f"PolarsDeltaTableWriter only supports TABLE targets; got: {type(spec)!r}"
            )
        self._write_delta(frame, spec, params_instance, streaming=streaming)

    def _write_delta(
        self,
        frame: pl.LazyFrame,
        spec: _TableWriteSpec,
        params_instance: Any,
        *,
        streaming: bool,
    ) -> None:
        existing_schema = _native_polars_schema(self._locator, spec.table_ref)
        if existing_schema is None and spec.schema_mode is SchemaMode.OVERWRITE:
            self._write_frame(_collect_frame(frame, streaming), spec, params_instance)
            return

        validated = apply_schema(frame, existing_schema, spec.schema_mode)
        self._write_frame(_collect_frame(validated, streaming), spec, params_instance)

    def _write_upsert_delta(
        self, frame: pl.LazyFrame, spec: UpsertSpec, *, streaming: bool
    ) -> None:
        if streaming:
            _log.warning(
                "streaming=True has no effect on UPSERT (table=%s) — "
                "MERGE requires full materialisation; ignoring",
                spec.table_ref.ref,
            )

        existing_schema = _native_polars_schema(self._locator, spec.table_ref)
        if existing_schema is None:
            _first_run_overwrite_polars(self._locator.locate(spec.table_ref), frame.collect())
            return

        validated = apply_schema(frame, existing_schema, spec.schema_mode)
        _merge_polars(self._locator.locate(spec.table_ref), validated.collect(), spec)

    def _write_frame(self, df: pl.DataFrame, spec: _TableWriteSpec, params_instance: Any) -> None:
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


def _collect_frame(frame: pl.LazyFrame, streaming: bool) -> pl.DataFrame:
    return frame.collect(engine="streaming" if streaming else "auto")


def _native_polars_schema(locator: TableLocator, ref: TableRef) -> pl.Schema | None:
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


def _first_run_overwrite_polars(loc: TableLocation, df: pl.DataFrame) -> None:
    write_deltalake(loc.uri, df.to_arrow(), mode="overwrite", **_write_kwargs(loc))


def _collect_partition_combos_polars(
    df: pl.DataFrame,
    partition_cols: tuple[str, ...],
) -> list[dict[str, Any]]:
    return df.unique(subset=list(partition_cols)).select(list(partition_cols)).to_dicts()


def _collect_partition_combos_for_merge_polars(
    df: pl.DataFrame,
    partition_cols: tuple[str, ...],
    table_ref: str,
) -> list[dict[str, Any]]:
    if not partition_cols:
        _warn_no_partition_cols(table_ref)
        return []
    combos = _collect_partition_combos_polars(df, partition_cols)
    _log_partition_combos(combos, table_ref)
    return combos


def _merge_polars(loc: TableLocation, df: pl.DataFrame, spec: UpsertSpec) -> None:
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
