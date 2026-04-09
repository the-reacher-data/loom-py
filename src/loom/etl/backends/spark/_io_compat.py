"""SourceReader and TargetWriter wrappers for SparkBackend.

These provide the public API contract while delegating to SparkBackend.
"""

from __future__ import annotations

import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from loom.etl.io.source import FileSourceSpec, SourceSpec, TableSourceSpec
from loom.etl.io.target import SchemaMode, TargetSpec
from loom.etl.io.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpsertSpec,
)
from loom.etl.schema._schema import SchemaNotFoundError
from loom.etl.schema._table import TableRef
from loom.etl.sql._predicate_sql import predicate_to_sql
from loom.etl.storage._config import MissingTablePolicy
from loom.etl.storage._io import SourceReader, TargetWriter
from loom.etl.storage._locator import TableLocator, _as_locator
from loom.etl.storage.route import (
    CatalogRouteResolver,
    PathRouteResolver,
    ResolvedTarget,
    TableRouteResolver,
)

from ._backend import SparkBackend


class SparkSourceReader(SourceReader):
    """Spark implementation of SourceReader protocol.

    Thin wrapper around SparkBackend that resolves specs to backend calls.
    """

    def __init__(
        self,
        spark: SparkSession,
        locator: str | os.PathLike[str] | TableLocator | None = None,
        *,
        route_resolver: TableRouteResolver | None = None,
    ) -> None:
        self._backend = SparkBackend(spark)
        if route_resolver is None:
            if locator is None:
                self._resolver: TableRouteResolver = CatalogRouteResolver()
            else:
                self._resolver = PathRouteResolver(_as_locator(locator))
        else:
            self._resolver = route_resolver

    def read(self, spec: SourceSpec, params_instance: Any) -> DataFrame:
        """Read source spec with Spark."""
        if isinstance(spec, TableSourceSpec):
            target = self._resolver.resolve(spec.table_ref)
            return self._backend.read.table(
                target,
                columns=spec.columns,
                predicates=spec.predicates,
                params=params_instance,
            )

        if isinstance(spec, FileSourceSpec):
            return self._backend.read.file(
                spec.path,
                spec.format,
                spec.read_options,
                columns=spec.columns,
                schema=spec.schema,
                json_columns=spec.json_columns,
            )

        raise TypeError(f"Unsupported source spec: {type(spec)!r}")


class SparkTargetWriter(TargetWriter):
    """Spark implementation of TargetWriter protocol.

    Thin wrapper around SparkBackend with write flow orchestration.
    """

    def __init__(
        self,
        spark: SparkSession,
        locator: str | os.PathLike[str] | TableLocator | None,
        *,
        route_resolver: TableRouteResolver | None = None,
        missing_table_policy: MissingTablePolicy = MissingTablePolicy.SCHEMA_MODE,
    ) -> None:
        self._backend = SparkBackend(spark)
        self._missing_table_policy = missing_table_policy
        if route_resolver is None:
            if locator is None:
                self._resolver: TableRouteResolver = CatalogRouteResolver()
            else:
                self._resolver = PathRouteResolver(_as_locator(locator))
        else:
            self._resolver = route_resolver

    def write(
        self,
        frame: DataFrame,
        spec: TargetSpec,
        params_instance: Any,
        /,
        *,
        streaming: bool = False,
    ) -> None:
        """Write target spec with Spark."""
        from loom.etl.io.target._file import FileSpec

        _ = streaming

        # File targets
        if isinstance(spec, FileSpec):
            self._backend.write.file(frame, spec.path, spec.format, spec.write_options)
            return

        table_specs = (AppendSpec, ReplaceSpec, ReplacePartitionsSpec, ReplaceWhereSpec, UpsertSpec)
        if not isinstance(spec, table_specs):
            raise TypeError(f"Unsupported target spec: {type(spec)!r}")

        # Table targets - resolve and dispatch
        target = self._resolver.resolve(spec.table_ref)

        if isinstance(spec, AppendSpec):
            self._write_append(frame, target, spec)
            return

        if isinstance(spec, ReplaceSpec):
            self._write_replace(frame, target, spec)
            return

        if isinstance(spec, ReplacePartitionsSpec):
            self._write_replace_partitions(frame, target, spec)
            return

        if isinstance(spec, ReplaceWhereSpec):
            self._write_replace_where(frame, target, spec, params_instance)
            return

        if isinstance(spec, UpsertSpec):
            self._write_upsert(frame, target, spec, streaming)
            return

        raise TypeError(f"Unsupported target spec: {type(spec)!r}")

    def _write_append(self, frame: DataFrame, target: ResolvedTarget, spec: AppendSpec) -> None:
        """Handle APPEND with first-run detection."""
        existing = self._backend.schema.physical(target)

        if existing is None:
            self._ensure_can_create(target, spec.schema_mode)
            self._backend.write.create(frame, target, schema_mode=spec.schema_mode)
            return

        aligned = self._backend.schema.align(frame, existing, spec.schema_mode)
        self._backend.write.append(aligned, target, schema_mode=spec.schema_mode)

    def _write_replace(self, frame: DataFrame, target: ResolvedTarget, spec: ReplaceSpec) -> None:
        """Handle REPLACE with first-run detection."""
        existing = self._backend.schema.physical(target)

        if existing is None:
            self._ensure_can_create(target, spec.schema_mode)
            self._backend.write.create(frame, target, schema_mode=spec.schema_mode)
            return

        aligned = self._backend.schema.align(frame, existing, spec.schema_mode)
        self._backend.write.replace(aligned, target, schema_mode=spec.schema_mode)

    def _write_replace_partitions(
        self, frame: DataFrame, target: ResolvedTarget, spec: ReplacePartitionsSpec
    ) -> None:
        """Handle REPLACE PARTITIONS with first-run detection."""
        existing = self._backend.schema.physical(target)

        if existing is None:
            self._ensure_can_create(target, spec.schema_mode)
            self._backend.write.create(
                frame,
                target,
                schema_mode=spec.schema_mode,
                partition_cols=spec.partition_cols,
            )
            return

        aligned = self._backend.schema.align(frame, existing, spec.schema_mode)
        self._backend.write.replace_partitions(
            aligned,
            target,
            partition_cols=spec.partition_cols,
            schema_mode=spec.schema_mode,
        )

    def _write_replace_where(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        spec: ReplaceWhereSpec,
        params: Any,
    ) -> None:
        """Handle REPLACE WHERE with first-run detection."""
        existing = self._backend.schema.physical(target)

        if existing is None:
            self._ensure_can_create(target, spec.schema_mode)
            self._backend.write.create(frame, target, schema_mode=spec.schema_mode)
            return

        predicate = predicate_to_sql(spec.replace_predicate, params)
        aligned = self._backend.schema.align(frame, existing, spec.schema_mode)
        self._backend.write.replace_where(
            aligned, target, predicate=predicate, schema_mode=spec.schema_mode
        )

    def _write_upsert(
        self, frame: DataFrame, target: ResolvedTarget, spec: UpsertSpec, streaming: bool
    ) -> None:
        """Handle UPSERT with first-run detection."""
        existing = self._backend.schema.physical(target)

        # Materialize if needed (Polars needs this, Spark no-op)
        materialized = self._backend.schema.materialize(frame, streaming)

        if existing is None:
            self._ensure_can_create(target, spec.schema_mode)
            self._backend.write.create(
                materialized,
                target,
                schema_mode=spec.schema_mode,
                partition_cols=spec.partition_cols,
            )
            return

        aligned = self._backend.schema.align(materialized, existing, spec.schema_mode)
        self._backend.write.upsert(
            aligned,
            target,
            keys=spec.upsert_keys,
            partition_cols=spec.partition_cols,
            schema_mode=spec.schema_mode,
            existing_schema=existing,
        )

    def append(
        self,
        frame: DataFrame,
        table_ref: TableRef,
        params_instance: Any,
        *,
        streaming: bool = False,
    ) -> None:
        """Append to table, creating it on first write."""
        self.write(
            frame,
            AppendSpec(table_ref=table_ref, schema_mode=SchemaMode.EVOLVE),
            params_instance,
            streaming=streaming,
        )

    def _ensure_can_create(self, target: ResolvedTarget, schema_mode: SchemaMode) -> None:
        if self._can_create_on_missing(schema_mode):
            return
        raise SchemaNotFoundError(
            f"Destination table does not yet exist: {target}. "
            "Use SchemaMode.OVERWRITE or set storage.missing_table_policy='create'."
        )

    def _can_create_on_missing(self, schema_mode: SchemaMode) -> bool:
        if self._missing_table_policy is MissingTablePolicy.CREATE:
            return True
        return schema_mode is SchemaMode.OVERWRITE
