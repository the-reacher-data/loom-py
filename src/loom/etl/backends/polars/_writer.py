"""Polars target writer implementing _WritePolicy hooks."""

from __future__ import annotations

import logging
from collections.abc import Sequence
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
from loom.etl.backends.polars._historify import PolarsHistorifyEngine
from loom.etl.backends.polars._schema import (
    PolarsPhysicalSchema,
    apply_schema_polars,
    read_delta_physical_schema,
)
from loom.etl.declarative.target import SchemaMode
from loom.etl.declarative.target._file import FileSpec
from loom.etl.declarative.target._history import HistorifyRepairReport, HistorifySpec
from loom.etl.declarative.target._table import AppendSpec, UpsertSpec
from loom.etl.observability.records import ExecutionRecord, get_record_schema
from loom.etl.storage import (
    MissingTablePolicy,
    PathRouteResolver,
    PathTarget,
    ResolvedTarget,
    TableLocation,
    TableRouteResolver,
)
from loom.etl.storage._file_locator import FileLocator
from loom.etl.storage._locator import TableLocator, _as_locator

from ._dtype import loom_type_to_polars
from ._file_writer import PolarsFileWriter

_log = logging.getLogger(__name__)


class PolarsTargetWriter(_WritePolicy[pl.LazyFrame, pl.DataFrame, PolarsPhysicalSchema]):
    """Polars target writer using delta-rs for Delta tables."""

    def __init__(
        self,
        locator: str | TableLocator,
        *,
        route_resolver: TableRouteResolver | None = None,
        missing_table_policy: MissingTablePolicy = MissingTablePolicy.SCHEMA_MODE,
        file_locator: FileLocator | None = None,
    ) -> None:
        self._locator = _as_locator(locator)
        super().__init__(
            resolver=route_resolver or PathRouteResolver(self._locator),
            missing_table_policy=missing_table_policy,
        )
        self._file_writer = PolarsFileWriter()
        self._file_locator = file_locator

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

    def to_frame(self, records: Sequence[ExecutionRecord], /) -> pl.LazyFrame:
        """Convert execution records into a Polars LazyFrame."""
        if not records:
            raise ValueError("PolarsTargetWriter.to_frame requires at least one record.")
        first = records[0]
        record_type = type(first)
        if any(type(record) is not record_type for record in records):
            raise TypeError(
                "PolarsTargetWriter.to_frame requires homogeneous record types per batch."
            )
        rows = [record.to_row() for record in records]
        return pl.from_dicts(rows, schema=_polars_record_schema(first)).lazy()

    # ====================================================================
    # Schema Hooks
    # ====================================================================

    def _physical_schema(self, target: ResolvedTarget) -> PolarsPhysicalSchema | None:
        """Read physical schema from Delta log."""
        path_target = self._as_path_target(target)
        physical = read_delta_physical_schema(
            path_target.location.uri,
            path_target.location.storage_options,
        )
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
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
        partition_cols: tuple[str, ...] = (),
    ) -> None:
        """Create new Delta table."""
        path_target = self._as_path_target(target)
        _ = schema_mode
        location = path_target.location
        self._warn_uc_first_create(path_target)
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
        path_target = self._as_path_target(target)
        _ = schema_mode
        location = path_target.location
        write_deltalake(location.uri, frame, mode="append", **self._write_kwargs(location))

    def _replace(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Overwrite existing Delta table."""
        path_target = self._as_path_target(target)
        _ = schema_mode
        location = path_target.location
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
        path_target = self._as_path_target(target)
        _ = schema_mode
        if frame.is_empty():
            _log.warning("replace_partitions table=%s has 0 rows — nothing written", path_target)
            return

        location = path_target.location
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
        path_target = self._as_path_target(target)
        _ = schema_mode
        location = path_target.location
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
        path_target = self._as_path_target(target)
        _ = existing_schema
        location = path_target.location

        # Collect partition combinations for pre-filter
        combos = self._collect_partition_combos(
            frame,
            spec.partition_cols,
            path_target.logical_ref.ref,
        )

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

    def _historify(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        spec: HistorifySpec,
        params_instance: Any,
    ) -> HistorifyRepairReport | None:
        """Apply SCD Type 2 via :class:`PolarsHistorifyEngine`."""
        path_target = self._as_path_target(target)
        loc = path_target.location
        engine = PolarsHistorifyEngine()
        return engine.apply(
            frame,
            loc.uri,
            loc.storage_options or None,
            spec,
            params_instance,
        )

    def _write_file(
        self,
        frame: pl.LazyFrame,
        spec: FileSpec,
        *,
        streaming: bool,
    ) -> None:
        """Write to file (CSV, JSON, Parquet), resolving alias if needed."""
        resolved = self._resolve_file_spec(spec)
        self._file_writer.write(frame, resolved, streaming=streaming)

    def _resolve_file_spec(self, spec: FileSpec) -> FileSpec:
        """Return a FileSpec with a physical URI, resolving alias when required."""
        if not spec.is_alias:
            return spec
        if self._file_locator is None:
            raise ValueError(
                f"IntoFile.alias({spec.path!r}) requires storage.files to be configured. "
                "Set storage.files in your config YAML."
            )
        location = self._file_locator.locate(spec.path)
        return FileSpec(
            path=location.uri_template,
            format=spec.format,
            is_alias=False,
            write_options=spec.write_options,
        )

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
    def _as_path_target(target: ResolvedTarget) -> PathTarget:
        """Validate and narrow resolved target to path target for Polars writes."""
        if isinstance(target, PathTarget):
            return target
        raise TypeError(
            "PolarsTargetWriter requires path-resolved targets. "
            "Configure routing so catalog refs resolve to PathTarget "
            "(e.g. uc://catalog.schema.table)."
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


def _polars_record_schema(record: ExecutionRecord) -> pl.Schema:
    cols = get_record_schema(type(record))
    return pl.Schema({c.name: loom_type_to_polars(c.dtype) for c in cols})
