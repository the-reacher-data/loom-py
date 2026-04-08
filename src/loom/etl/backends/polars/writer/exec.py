"""Polars write executor for planned table operations."""

from __future__ import annotations

import logging
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
from loom.etl.storage._locator import TableLocation, TableLocator
from loom.etl.storage.route import CatalogTarget, PathTarget
from loom.etl.storage.schema.model import PhysicalSchema, PolarsPhysicalSchema
from loom.etl.storage.write import (
    AppendOp,
    ReplaceOp,
    ReplacePartitionsOp,
    ReplaceWhereOp,
    UpsertOp,
    WriteOperation,
)

_log = logging.getLogger(__name__)


class PolarsWriteExecutor:
    """Execute write operations against Delta-RS table targets."""

    def __init__(self) -> None:
        self._handlers = {
            AppendOp: self._exec_append,
            ReplaceOp: self._exec_replace,
            ReplacePartitionsOp: self._exec_replace_partitions,
            ReplaceWhereOp: self._exec_replace_where,
            UpsertOp: self._exec_upsert,
        }

    def execute(self, frame: pl.LazyFrame, op: WriteOperation, params_instance: Any) -> None:
        """Execute one write operation."""
        handler = self._handlers.get(type(op))
        if handler is None:
            raise TypeError(f"Unsupported write operation: {type(op)!r}")
        handler(frame, op, params_instance)

    def _exec_append(self, frame: pl.LazyFrame, op: WriteOperation, params_instance: Any) -> None:
        if not isinstance(op, AppendOp):
            raise TypeError(f"Expected AppendOp, got {type(op)!r}")
        locator = _locator_for_target(op.target)
        spec = AppendSpec(table_ref=op.target.logical_ref, schema_mode=op.schema_mode)
        validated = _validated_frame(frame, op.existing_schema, op.schema_mode)
        _write_frame(
            _collect_frame(validated, streaming=op.streaming), spec, params_instance, locator
        )

    def _exec_replace(self, frame: pl.LazyFrame, op: WriteOperation, params_instance: Any) -> None:
        if not isinstance(op, ReplaceOp):
            raise TypeError(f"Expected ReplaceOp, got {type(op)!r}")
        locator = _locator_for_target(op.target)
        spec = ReplaceSpec(table_ref=op.target.logical_ref, schema_mode=op.schema_mode)
        if op.existing_schema is None and op.schema_mode is SchemaMode.OVERWRITE:
            _warn_uc_first_create(op.target)
            _write_frame(
                _collect_frame(frame, streaming=op.streaming), spec, params_instance, locator
            )
            return
        validated = _validated_frame(frame, op.existing_schema, op.schema_mode)
        _write_frame(
            _collect_frame(validated, streaming=op.streaming), spec, params_instance, locator
        )

    def _exec_replace_partitions(
        self, frame: pl.LazyFrame, op: WriteOperation, _params_instance: Any
    ) -> None:
        if not isinstance(op, ReplacePartitionsOp):
            raise TypeError(f"Expected ReplacePartitionsOp, got {type(op)!r}")
        locator = _locator_for_target(op.target)
        spec = ReplacePartitionsSpec(
            table_ref=op.target.logical_ref,
            partition_cols=op.partition_cols,
            schema_mode=op.schema_mode,
        )
        if op.existing_schema is None and op.schema_mode is SchemaMode.OVERWRITE:
            _warn_uc_first_create(op.target)
            _first_run_overwrite_partitions_polars(
                locator.locate(spec.table_ref),
                _collect_frame(frame, streaming=op.streaming),
                spec.partition_cols,
            )
            return

        validated = _validated_frame(frame, op.existing_schema, op.schema_mode)
        _write_replace_partitions(
            locator.locate(spec.table_ref),
            _collect_frame(validated, streaming=op.streaming),
            spec,
        )

    def _exec_replace_where(
        self, frame: pl.LazyFrame, op: WriteOperation, params_instance: Any
    ) -> None:
        if not isinstance(op, ReplaceWhereOp):
            raise TypeError(f"Expected ReplaceWhereOp, got {type(op)!r}")
        locator = _locator_for_target(op.target)
        spec = ReplaceWhereSpec(
            table_ref=op.target.logical_ref,
            replace_predicate=op.replace_predicate,
            schema_mode=op.schema_mode,
        )
        if op.existing_schema is None and op.schema_mode is SchemaMode.OVERWRITE:
            _warn_uc_first_create(op.target)
            _write_frame(
                _collect_frame(frame, streaming=op.streaming), spec, params_instance, locator
            )
            return
        validated = _validated_frame(frame, op.existing_schema, op.schema_mode)
        _write_frame(
            _collect_frame(validated, streaming=op.streaming), spec, params_instance, locator
        )

    def _exec_upsert(self, frame: pl.LazyFrame, op: WriteOperation, _params_instance: Any) -> None:
        if not isinstance(op, UpsertOp):
            raise TypeError(f"Expected UpsertOp, got {type(op)!r}")
        locator = _locator_for_target(op.target)
        spec = UpsertSpec(
            table_ref=op.target.logical_ref,
            upsert_keys=op.upsert_keys,
            partition_cols=op.partition_cols,
            upsert_exclude=op.upsert_exclude,
            upsert_include=op.upsert_include,
            schema_mode=op.schema_mode,
        )
        if op.streaming:
            _log.warning(
                "streaming=True has no effect on UPSERT (table=%s) — "
                "MERGE requires full materialisation; ignoring",
                spec.table_ref.ref,
            )

        loc = locator.locate(spec.table_ref)
        if op.existing_schema is None:
            _warn_uc_first_create(op.target)
            _first_run_overwrite_polars(loc, frame.collect())
            return

        validated = _validated_frame(frame, op.existing_schema, op.schema_mode)
        _merge_polars(loc, validated.collect(), spec)


def _validated_frame(
    frame: pl.LazyFrame, existing_schema: PhysicalSchema | None, mode: SchemaMode
) -> pl.LazyFrame:
    return apply_schema(frame, _as_polars_schema(existing_schema), mode)


def _as_polars_schema(schema: PhysicalSchema | None) -> pl.Schema | None:
    if schema is None:
        return None
    if not isinstance(schema, PolarsPhysicalSchema):
        raise TypeError(f"Polars executor expected PolarsPhysicalSchema, got {type(schema)!r}.")
    return schema.schema


def _collect_frame(frame: pl.LazyFrame, *, streaming: bool) -> pl.DataFrame:
    return frame.collect(engine="streaming" if streaming else "auto")


def _warn_uc_first_create(target: CatalogTarget | PathTarget) -> None:
    if isinstance(target, CatalogTarget):
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


def _write_frame(
    df: pl.DataFrame,
    spec: AppendSpec | ReplaceSpec | ReplaceWhereSpec,
    params: Any,
    locator: TableLocator,
) -> None:
    loc = locator.locate(spec.table_ref)
    match spec:
        case AppendSpec():
            write_deltalake(loc.uri, df.to_arrow(), mode="append", **_write_kwargs(loc))
        case ReplaceSpec():
            write_deltalake(loc.uri, df.to_arrow(), mode="overwrite", **_write_kwargs(loc))
        case ReplaceWhereSpec():
            _write_replace_where(loc, df, spec, params)


def _locator_for_target(target: CatalogTarget | PathTarget) -> TableLocator:
    if isinstance(target, CatalogTarget):
        raise ValueError(
            "Polars writer only supports path targets; "
            f"got catalog target for {target.logical_ref.ref!r}."
        )
    return _SingleLocationLocator(target.logical_ref, target.location)


class _SingleLocationLocator:
    def __init__(self, expected_ref: TableRef, location: TableLocation) -> None:
        self._expected_ref = expected_ref
        self._location = location

    def locate(self, ref: TableRef) -> TableLocation:
        if ref != self._expected_ref:
            raise KeyError(
                f"Single-location locator cannot resolve {ref.ref!r}; "
                f"expected {self._expected_ref.ref!r}."
            )
        return self._location


def _write_kwargs(loc: TableLocation) -> dict[str, Any]:
    from deltalake import CommitProperties, WriterProperties

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


def _first_run_overwrite_partitions_polars(
    loc: TableLocation, df: pl.DataFrame, partition_cols: tuple[str, ...]
) -> None:
    kwargs = _write_kwargs(loc)
    if partition_cols:
        write_deltalake(
            loc.uri,
            df.to_arrow(),
            mode="overwrite",
            partition_by=list(partition_cols),
            **kwargs,
        )
        return
    write_deltalake(loc.uri, df.to_arrow(), mode="overwrite", **kwargs)


def _collect_partition_combos_for_merge_polars(
    df: pl.DataFrame,
    partition_cols: tuple[str, ...],
    table_ref: str,
) -> list[dict[str, Any]]:
    if not partition_cols:
        _warn_no_partition_cols(table_ref)
        return []
    combos = df.unique(subset=list(partition_cols)).select(list(partition_cols)).to_dicts()
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
