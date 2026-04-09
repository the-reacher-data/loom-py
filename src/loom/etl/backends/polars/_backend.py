"""PolarsBackend — compute backend for Polars + delta-rs."""

from __future__ import annotations

import logging
import os
from typing import Any

import polars as pl
from deltalake import CommitProperties, DeltaTable, WriterProperties, write_deltalake

from loom.etl.backends._predicate_sql import predicate_to_sql
from loom.etl.backends._upsert import (
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
from loom.etl.backends.polars._dtype import polars_to_loom_type
from loom.etl.backends.polars._predicate import predicate_to_polars
from loom.etl.backends.polars._reader import (
    _apply_json_decode,
    _apply_source_schema,
    read_file_scan,
)
from loom.etl.backends.polars._schema import apply_schema
from loom.etl.io._format import Format
from loom.etl.io.target import SchemaMode
from loom.etl.io.target._file import FileSpec
from loom.etl.schema._schema import ColumnSchema, SchemaNotFoundError
from loom.etl.schema._table import TableRef
from loom.etl.storage._locator import TableLocation, TableLocator, _as_locator
from loom.etl.storage.routing import CatalogTarget, PathTarget, ResolvedTarget
from loom.etl.storage.schema import PolarsPhysicalSchema, SchemaReader
from loom.etl.storage.schema.delta import DeltaSchemaReader, read_delta_physical_schema

from ._file_writer import PolarsFileWriter

_log = logging.getLogger(__name__)


class PolarsReadOps:
    """Polars read operations for table and file sources."""

    def table(
        self,
        target: ResolvedTarget,
        *,
        columns: tuple[str, ...] | None = None,
        predicates: tuple[Any, ...] = (),
        params: Any = None,
        schema: tuple[ColumnSchema, ...] = (),
        json_columns: tuple[Any, ...] = (),
    ) -> pl.LazyFrame:
        """Read table target as lazy frame with pushdown-friendly transforms."""
        if isinstance(target, CatalogTarget):
            raise ValueError(
                "Polars backend only supports path targets for table reads; "
                f"got catalog target for {target.logical_ref.ref!r}."
            )
        location = target.location
        frame = pl.scan_delta(location.uri, storage_options=location.storage_options or None)
        for predicate in predicates:
            frame = frame.filter(predicate_to_polars(predicate, params))
        if columns:
            frame = frame.select(list(columns))
        frame = _apply_source_schema(frame, schema)
        return _apply_json_decode(frame, json_columns)

    def file(
        self,
        path: str,
        format: Format | str,
        options: Any = None,
        *,
        columns: tuple[str, ...] | None = None,
        schema: tuple[ColumnSchema, ...] = (),
        json_columns: tuple[Any, ...] = (),
    ) -> pl.LazyFrame:
        """Read file source as lazy frame."""
        frame = read_file_scan(path, format, options)
        if columns:
            frame = frame.select(list(columns))
        frame = _apply_source_schema(frame, schema)
        return _apply_json_decode(frame, json_columns)

    def sql(self, frames: dict[str, pl.LazyFrame], query: str) -> pl.LazyFrame:
        """Execute SQL query against in-memory Polars frames."""
        if not frames:
            raise ValueError("Polars SQL execution requires at least one input frame.")
        return pl.SQLContext(frames).execute(query)


class PolarsSchemaOps:
    """Polars schema operations for discovery and write-time validation."""

    def __init__(self, locator: TableLocator, *, schema_reader: SchemaReader | None = None) -> None:
        self._locator = locator
        self._schema_reader = schema_reader or DeltaSchemaReader()

    def exists(self, ref: TableRef) -> bool:
        """Return ``True`` when a Delta table exists for ``ref``."""
        try:
            location = self._locator.locate(ref)
        except Exception:
            return False
        return DeltaTable.is_deltatable(location.uri, location.storage_options or None)

    def columns(self, ref: TableRef) -> tuple[str, ...]:
        """Return table columns from physical schema."""
        schema = self.schema(ref)
        return tuple(column.name for column in schema) if schema is not None else ()

    def schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        """Return table schema as Loom column schemas."""
        try:
            location = self._locator.locate(ref)
        except Exception:
            return None
        physical = read_delta_physical_schema(location.uri, location.storage_options)
        if physical is None:
            return None
        if not isinstance(physical, PolarsPhysicalSchema):
            raise TypeError(f"Expected PolarsPhysicalSchema, got {type(physical)!r}.")
        return tuple(
            ColumnSchema(name=name, dtype=polars_to_loom_type(dtype), nullable=True)
            for name, dtype in physical.schema.items()
        )

    def physical(self, target: ResolvedTarget) -> PolarsPhysicalSchema | None:
        """Return native physical schema for one resolved target."""
        if isinstance(target, CatalogTarget):
            raise ValueError(
                "Polars backend only supports path targets for schema reads; "
                f"got catalog target for {target.logical_ref.ref!r}."
            )
        physical = self._schema_reader.read_schema(target)
        if physical is None:
            return None
        if not isinstance(physical, PolarsPhysicalSchema):
            raise TypeError(f"Expected PolarsPhysicalSchema, got {type(physical)!r}.")
        return physical

    def align(
        self,
        frame: pl.LazyFrame,
        existing_schema: PolarsPhysicalSchema | None,
        mode: SchemaMode,
    ) -> pl.LazyFrame:
        """Align frame schema to destination schema according to mode."""
        schema = existing_schema.schema if existing_schema is not None else None
        return apply_schema(frame, schema, mode)

    def materialize(self, frame: pl.LazyFrame, streaming: bool) -> pl.DataFrame:
        """Collect lazy frame to materialized frame."""
        return frame.collect(engine="streaming" if streaming else "auto")

    def to_sql(self, predicate: Any, params: Any) -> str:
        """Translate predicate node to SQL expression."""
        return predicate_to_sql(predicate, params)


class PolarsWriteOps:
    """Polars write operations for table and file targets."""

    def __init__(self) -> None:
        self._file_writer = PolarsFileWriter()

    def create(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
        partition_cols: tuple[str, ...] = (),
    ) -> None:
        """Create table on first run."""
        _ = schema_mode
        location = _target_location(target)
        _warn_uc_first_create(target)
        kwargs = _write_kwargs(location)
        if partition_cols:
            write_deltalake(
                location.uri,
                frame,
                mode="overwrite",
                partition_by=list(partition_cols),
                **kwargs,
            )
            return
        write_deltalake(location.uri, frame, mode="overwrite", **kwargs)

    def append(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Append rows into destination table."""
        _ = schema_mode
        location = _target_location(target)
        write_deltalake(location.uri, frame, mode="append", **_write_kwargs(location))

    def replace(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Overwrite whole destination table."""
        _ = schema_mode
        location = _target_location(target)
        write_deltalake(location.uri, frame, mode="overwrite", **_write_kwargs(location))

    def replace_partitions(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
    ) -> None:
        """Overwrite only partitions present in frame."""
        _ = schema_mode
        if frame.is_empty():
            _log.warning("replace_partitions table=%s has 0 rows — nothing written", target)
            return
        location = _target_location(target)
        predicate = _build_partition_predicate(
            frame.select(list(partition_cols)).unique().iter_rows(named=True),
            partition_cols,
        )
        write_deltalake(
            location.uri,
            frame,
            mode="overwrite",
            predicate=predicate,
            **_write_kwargs(location),
        )

    def replace_where(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        predicate: str,
        schema_mode: SchemaMode,
    ) -> None:
        """Overwrite destination rows matched by SQL predicate."""
        _ = schema_mode
        location = _target_location(target)
        write_deltalake(
            location.uri,
            frame,
            mode="overwrite",
            predicate=predicate,
            **_write_kwargs(location),
        )

    def upsert(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        keys: tuple[str, ...],
        partition_cols: tuple[str, ...],
        upsert_exclude: tuple[str, ...] = (),
        upsert_include: tuple[str, ...] = (),
        schema_mode: SchemaMode,
        existing_schema: PolarsPhysicalSchema | None,
        streaming: bool = False,
    ) -> None:
        """Merge frame into destination table."""
        if streaming:
            _log.warning(
                "streaming=True has no effect on UPSERT (table=%s) — "
                "MERGE requires full materialisation; ignoring",
                target.logical_ref.ref,
            )
        if existing_schema is None:
            raise SchemaNotFoundError(
                "UPSERT requires an existing destination table. "
                "Use create/replace flow for first write."
            )
        _ = schema_mode
        location = _target_location(target)
        combos = _collect_partition_combos(frame, partition_cols, target.logical_ref.ref)
        merge_spec = _UpsertSpec(
            upsert_keys=keys,
            partition_cols=partition_cols,
            upsert_exclude=upsert_exclude,
            upsert_include=upsert_include,
        )
        predicate = _build_upsert_predicate(
            combos,
            merge_spec,
            TARGET_ALIAS,
            SOURCE_ALIAS,
        )
        update_cols = _build_upsert_update_cols(tuple(frame.columns), merge_spec)
        update_set = _build_update_set(update_cols, SOURCE_ALIAS)
        insert_values = _build_insert_values(tuple(frame.columns), SOURCE_ALIAS)
        dt = DeltaTable(location.uri, storage_options=location.storage_options or {})
        (
            dt.merge(
                source=frame,
                predicate=predicate,
                source_alias=SOURCE_ALIAS,
                target_alias=TARGET_ALIAS,
            )
            .when_matched_update(updates=update_set)
            .when_not_matched_insert(updates=insert_values)
            .execute()
        )

    def file(
        self,
        frame: pl.LazyFrame,
        path: str,
        format: Format | str,
        options: Any = None,
        *,
        streaming: bool = False,
    ) -> None:
        """Write file target using Polars file writer."""
        fmt = format if isinstance(format, Format) else Format(format)
        self._file_writer.write(
            frame,
            FileSpec(path=path, format=fmt, write_options=options),
            streaming=streaming,
        )


class PolarsBackend:
    """Compute backend for Polars + delta-rs."""

    def __init__(
        self,
        locator: str | os.PathLike[str] | TableLocator,
        *,
        schema_reader: SchemaReader | None = None,
    ) -> None:
        normalized_locator = _as_locator(locator)
        self.read = PolarsReadOps()
        self.schema = PolarsSchemaOps(normalized_locator, schema_reader=schema_reader)
        self.write = PolarsWriteOps()


class _UpsertSpec:
    """Adapter carrying the minimum fields required by SQL upsert helpers."""

    def __init__(
        self,
        upsert_keys: tuple[str, ...],
        partition_cols: tuple[str, ...],
        upsert_exclude: tuple[str, ...] = (),
        upsert_include: tuple[str, ...] = (),
    ) -> None:
        self.upsert_keys = upsert_keys
        self.partition_cols = partition_cols
        self.upsert_exclude = upsert_exclude
        self.upsert_include = upsert_include


def _target_location(target: ResolvedTarget) -> TableLocation:
    if isinstance(target, CatalogTarget):
        raise ValueError(
            "Polars backend only supports path targets; "
            f"got catalog target for {target.logical_ref.ref!r}."
        )
    return target.location


def _warn_uc_first_create(target: ResolvedTarget) -> None:
    if not isinstance(target, PathTarget):
        return
    if not target.location.uri.lower().startswith("uc://"):
        return
    _log.warning(
        "polars write first-create for Unity Catalog ref=%s uri=%s. "
        "delta-rs writes Delta log data, but catalog registration is not guaranteed; "
        "pre-create the table in UC (Spark SQL/Databricks) before this write.",
        target.logical_ref.ref,
        target.location.uri,
    )


def _write_kwargs(loc: TableLocation) -> dict[str, Any]:
    return {
        "storage_options": loc.storage_options or None,
        "configuration": loc.delta_config or None,
        "writer_properties": WriterProperties(**loc.writer) if loc.writer else None,
        "commit_properties": CommitProperties(**loc.commit) if loc.commit else None,
    }


def _collect_partition_combos(
    frame: pl.DataFrame,
    partition_cols: tuple[str, ...],
    table_ref: str,
) -> list[dict[str, Any]]:
    if not partition_cols:
        _warn_no_partition_cols(table_ref)
        return []
    combos = frame.unique(subset=list(partition_cols)).select(list(partition_cols)).to_dicts()
    _log_partition_combos(combos, table_ref)
    return combos
