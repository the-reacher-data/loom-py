"""Polars target writer implementing _WritePolicy hooks."""

from __future__ import annotations

import logging
from typing import Any

import polars as pl
from deltalake import CommitProperties, DeltaTable, WriterProperties, write_deltalake

from loom.etl.backends._merge import (
    SOURCE_ALIAS,
    TARGET_ALIAS,
    _build_merge_plan,
    _build_partition_predicate,
    _log_partition_combos,
    _warn_no_partition_cols,
)
from loom.etl.backends._predicate import predicate_to_sql
from loom.etl.backends._write_policy import _WritePolicy
from loom.etl.backends.polars._schema import (
    PolarsPhysicalSchema,
    apply_schema_polars,
    read_delta_physical_schema,
)
from loom.etl.declarative.target import SchemaMode
from loom.etl.declarative.target._file import FileSpec
from loom.etl.declarative.target._table import AppendSpec, UpsertSpec
from loom.etl.storage import (
    MissingTablePolicy,
    PathRouteResolver,
    PathTarget,
    TableLocation,
    TableRouteResolver,
)
from loom.etl.storage._locator import _as_locator

from ._file_writer import PolarsFileWriter

_log = logging.getLogger(__name__)


class PolarsTargetWriter(_WritePolicy[pl.LazyFrame, pl.DataFrame, PolarsPhysicalSchema]):
    """Polars target writer using delta-rs for Delta tables."""

    def __init__(
        self,
        locator: str,
        *,
        route_resolver: TableRouteResolver | None = None,
        missing_table_policy: MissingTablePolicy = MissingTablePolicy.SCHEMA_MODE,
    ) -> None:
        self._locator = _as_locator(locator)
        super().__init__(
            resolver=route_resolver or PathRouteResolver(self._locator),
            missing_table_policy=missing_table_policy,
        )
        self._file_writer = PolarsFileWriter()

    def append(
        self,
        frame: pl.LazyFrame,
        table_ref: Any,
        params_instance: Any,
        *,
        streaming: bool = False,
    ) -> None:
        """Append frame to table (legacy API, creates table on first write)."""
        spec = AppendSpec(table_ref=table_ref, schema_mode=SchemaMode.EVOLVE)
        self.write(frame, spec, params_instance, streaming=streaming)

    # ====================================================================
    # Schema Hooks
    # ====================================================================

    def _physical_schema(self, target: PathTarget) -> PolarsPhysicalSchema | None:
        """Read physical schema from Delta log."""
        physical = read_delta_physical_schema(
            target.location.uri,
            target.location.storage_options,
        )
        if physical is None:
            return None
        if not isinstance(physical, PolarsPhysicalSchema):
            raise TypeError(f"Expected PolarsPhysicalSchema, got {type(physical)!r}.")
        return physical

    def _align(
        self,
        frame: pl.LazyFrame,
        existing_schema: PolarsPhysicalSchema | None,
        mode: SchemaMode,
    ) -> pl.LazyFrame:
        """Align frame schema with existing."""
        schema = existing_schema.schema if existing_schema is not None else None
        return apply_schema_polars(frame, schema, mode)

    def _materialize_for_write(self, frame: pl.LazyFrame, streaming: bool) -> pl.DataFrame:
        """Collect lazy frame to DataFrame before Delta write."""
        return frame.collect(engine="streaming" if streaming else "auto")

    def _predicate_to_sql(self, predicate: Any, params: Any) -> str:
        """Convert predicate to SQL (Polars uses internal SQL representation)."""
        return predicate_to_sql(predicate, params)

    # ====================================================================
    # Write Hooks
    # ====================================================================

    def _create(
        self,
        frame: pl.DataFrame,
        target: PathTarget,
        *,
        schema_mode: SchemaMode,
        partition_cols: tuple[str, ...] = (),
    ) -> None:
        """Create new Delta table."""
        _ = schema_mode
        location = target.location
        self._warn_uc_first_create(target)
        kwargs = self._write_kwargs(location)

        if partition_cols:
            write_deltalake(
                location.uri,
                frame,
                mode="overwrite",
                partition_by=list(partition_cols),
                **kwargs,
            )
        else:
            write_deltalake(location.uri, frame, mode="overwrite", **kwargs)

    def _append(
        self,
        frame: pl.DataFrame,
        target: PathTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Append to existing Delta table."""
        _ = schema_mode
        location = target.location
        write_deltalake(location.uri, frame, mode="append", **self._write_kwargs(location))

    def _replace(
        self,
        frame: pl.DataFrame,
        target: PathTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Overwrite existing Delta table."""
        _ = schema_mode
        location = target.location
        write_deltalake(location.uri, frame, mode="overwrite", **self._write_kwargs(location))

    def _replace_partitions(
        self,
        frame: pl.DataFrame,
        target: PathTarget,
        *,
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
    ) -> None:
        """Overwrite partitions present in frame."""
        _ = schema_mode
        if frame.is_empty():
            _log.warning("replace_partitions table=%s has 0 rows — nothing written", target)
            return

        location = target.location
        predicate = _build_partition_predicate(
            frame.select(list(partition_cols)).unique().iter_rows(named=True),
            partition_cols,
        )
        write_deltalake(
            location.uri,
            frame,
            mode="overwrite",
            predicate=predicate,
            **self._write_kwargs(location),
        )

    def _replace_where(
        self,
        frame: pl.DataFrame,
        target: PathTarget,
        *,
        predicate: str,
        schema_mode: SchemaMode,
    ) -> None:
        """Overwrite rows matching SQL predicate."""
        _ = schema_mode
        location = target.location
        write_deltalake(
            location.uri,
            frame,
            mode="overwrite",
            predicate=predicate,
            **self._write_kwargs(location),
        )

    def _upsert(
        self,
        frame: pl.DataFrame,
        target: PathTarget,
        *,
        spec: UpsertSpec,
        existing_schema: PolarsPhysicalSchema,
    ) -> None:
        """Merge frame into target using Delta MERGE."""
        _ = existing_schema
        location = target.location

        # Collect partition combinations for pre-filter
        combos = self._collect_partition_combos(frame, spec.partition_cols, target.logical_ref.ref)

        merge_plan = _build_merge_plan(
            combos=combos,
            spec=spec,
            df_columns=tuple(frame.columns),
            target_alias=TARGET_ALIAS,
            source_alias=SOURCE_ALIAS,
        )

        dt = DeltaTable(location.uri, storage_options=location.storage_options or {})
        (
            dt.merge(
                source=frame,
                predicate=merge_plan.predicate,
                source_alias=SOURCE_ALIAS,
                target_alias=TARGET_ALIAS,
            )
            .when_matched_update(updates=merge_plan.update_set)
            .when_not_matched_insert(updates=merge_plan.insert_values)
            .execute()
        )

    def _write_file(
        self,
        frame: pl.LazyFrame,
        spec: FileSpec,
        *,
        streaming: bool,
    ) -> None:
        """Write to file (CSV, JSON, Parquet)."""
        self._file_writer.write(frame, spec, streaming=streaming)

    # ====================================================================
    # Helpers
    # ====================================================================

    @staticmethod
    def _warn_uc_first_create(target: PathTarget) -> None:
        """Warn about UC table creation limitations."""
        if not target.location.uri.lower().startswith("uc://"):
            return
        _log.warning(
            "Polars write first-create for Unity Catalog ref=%s uri=%s. "
            "delta-rs writes Delta log data, but catalog registration is not guaranteed; "
            "pre-create the table in UC (Spark SQL/Databricks) before this write.",
            target.logical_ref.ref,
            target.location.uri,
        )

    @staticmethod
    def _write_kwargs(loc: TableLocation) -> dict[str, Any]:
        """Build write kwargs from TableLocation."""
        return {
            "storage_options": loc.storage_options or None,
            "configuration": loc.delta_config or None,
            "writer_properties": WriterProperties(**loc.writer) if loc.writer else None,
            "commit_properties": CommitProperties(**loc.commit) if loc.commit else None,
        }

    @staticmethod
    def _collect_partition_combos(
        frame: pl.DataFrame,
        partition_cols: tuple[str, ...],
        table_ref: str,
    ) -> list[dict[str, Any]]:
        """Collect unique partition value combinations."""
        if not partition_cols:
            _warn_no_partition_cols(table_ref)
            return []
        combos = frame.unique(subset=list(partition_cols)).select(list(partition_cols)).to_dicts()
        _log_partition_combos(combos, table_ref)
        return combos


__all__ = ["PolarsTargetWriter"]
