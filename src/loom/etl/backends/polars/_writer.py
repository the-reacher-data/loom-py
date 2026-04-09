"""Polars target writer implementing _WritePolicy hooks."""

from __future__ import annotations

import logging
from typing import Any

import polars as pl
from deltalake import CommitProperties, DeltaTable, WriterProperties, write_deltalake

from loom.etl.backends._predicate import predicate_to_sql
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
from loom.etl.backends._write_policy import _WritePolicy
from loom.etl.io.target import SchemaMode
from loom.etl.io.target._file import FileSpec
from loom.etl.io.target._table import AppendSpec, UpsertSpec
from loom.etl.schema._table import TableRef
from loom.etl.storage._config import MissingTablePolicy
from loom.etl.storage._locator import TableLocation, _as_locator
from loom.etl.storage.routing import (
    CatalogTarget,
    PathRouteResolver,
    PathTarget,
    ResolvedTarget,
    TableRouteResolver,
)
from loom.etl.storage.schema import PolarsPhysicalSchema
from loom.etl.storage.schema.delta import read_delta_physical_schema

from ._file_writer import PolarsFileWriter
from ._schema import apply_schema

_log = logging.getLogger(__name__)


class PolarsTargetWriter(_WritePolicy[pl.DataFrame, PolarsPhysicalSchema]):
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
        frame: pl.DataFrame,
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

    def _physical_schema(self, target: ResolvedTarget) -> PolarsPhysicalSchema | None:
        """Read physical schema from Delta log."""
        if isinstance(target, CatalogTarget):
            raise ValueError(
                "Polars backend only supports path targets; "
                f"got catalog target for {target.logical_ref.ref!r}."
            )

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
        return apply_schema(frame, schema, mode)

    def _materialize(self, frame: pl.LazyFrame, streaming: bool) -> pl.DataFrame:
        """Collect lazy frame to DataFrame."""
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
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
        partition_cols: tuple[str, ...] = (),
    ) -> None:
        """Create new Delta table."""
        _ = schema_mode
        location = self._target_location(target)
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
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Append to existing Delta table."""
        _ = schema_mode
        location = self._target_location(target)
        write_deltalake(location.uri, frame, mode="append", **self._write_kwargs(location))

    def _replace(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Overwrite existing Delta table."""
        _ = schema_mode
        location = self._target_location(target)
        write_deltalake(location.uri, frame, mode="overwrite", **self._write_kwargs(location))

    def _replace_partitions(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
    ) -> None:
        """Overwrite partitions present in frame."""
        _ = schema_mode
        if frame.is_empty():
            _log.warning("replace_partitions table=%s has 0 rows — nothing written", target)
            return

        location = self._target_location(target)
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
        target: ResolvedTarget,
        *,
        predicate: str,
        schema_mode: SchemaMode,
    ) -> None:
        """Overwrite rows matching SQL predicate."""
        _ = schema_mode
        location = self._target_location(target)
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
        target: ResolvedTarget,
        *,
        spec: UpsertSpec,
        existing_schema: PolarsPhysicalSchema,
    ) -> None:
        """Merge frame into target using Delta MERGE."""
        _ = existing_schema
        location = self._target_location(target)

        # Collect partition combinations for pre-filter
        combos = self._collect_partition_combos(frame, spec.partition_cols, target.logical_ref.ref)

        # Build MERGE spec adapter for shared helpers
        merge_spec = self._MergeSpecAdapter(
            upsert_keys=spec.upsert_keys,
            partition_cols=spec.partition_cols,
            upsert_exclude=spec.upsert_exclude,
            upsert_include=spec.upsert_include,
        )

        # Build predicate and column sets using shared helpers
        predicate = _build_upsert_predicate(combos, merge_spec, TARGET_ALIAS, SOURCE_ALIAS)
        update_cols = _build_upsert_update_cols(tuple(frame.columns), merge_spec)
        update_set = _build_update_set(update_cols, SOURCE_ALIAS)
        insert_values = _build_insert_values(tuple(frame.columns), SOURCE_ALIAS)

        # Execute MERGE
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

    def _write_file(
        self,
        frame: pl.DataFrame,
        spec: FileSpec,
        *,
        streaming: bool,
    ) -> None:
        """Write to file (CSV, JSON, Parquet)."""
        _ = streaming
        self._file_writer.write(frame, spec, streaming=False)

    # ====================================================================
    # Helpers
    # ====================================================================

    @staticmethod
    def _target_location(target: ResolvedTarget) -> TableLocation:
        """Extract TableLocation from ResolvedTarget."""
        if isinstance(target, CatalogTarget):
            raise ValueError(
                "Polars backend only supports path targets; "
                f"got catalog target for {target.logical_ref.ref!r}."
            )
        return target.location

    @staticmethod
    def _warn_uc_first_create(target: ResolvedTarget) -> None:
        """Warn about UC table creation limitations."""
        if not isinstance(target, PathTarget):
            return
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

    class _MergeSpecAdapter:
        """Adapter to make UpsertSpec compatible with shared _upsert.py helpers."""

        def __init__(
            self,
            upsert_keys: tuple[str, ...],
            partition_cols: tuple[str, ...],
            upsert_exclude: tuple[str, ...],
            upsert_include: tuple[str, ...],
        ) -> None:
            self.upsert_keys = upsert_keys
            self.partition_cols = partition_cols
            self.upsert_exclude = upsert_exclude
            self.upsert_include = upsert_include


__all__ = ["PolarsTargetWriter"]
