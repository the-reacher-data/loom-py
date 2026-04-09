"""Shared write policy via Template Method pattern.

This module provides the base class for all backend-specific target writers.
It implements the common "check-exists → create OR align+write" policy,
while delegating backend-specific operations to abstract hooks.
"""

from __future__ import annotations

from typing import Any, Generic, TypeVar

from loom.etl.io.target import SchemaMode, TargetSpec
from loom.etl.io.target._file import FileSpec
from loom.etl.io.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpsertSpec,
)
from loom.etl.schema._schema import SchemaNotFoundError
from loom.etl.storage._config import MissingTablePolicy
from loom.etl.storage.protocols import TargetWriter
from loom.etl.storage.routing import ResolvedTarget, TableRouteResolver

FrameT = TypeVar("FrameT")
PhysicalSchemaT = TypeVar("PhysicalSchemaT")


def _ensure_can_create_missing_table(
    *,
    target: ResolvedTarget,
    schema_mode: SchemaMode,
    missing_table_policy: MissingTablePolicy,
) -> None:
    """Validate whether the write path may create a missing destination table."""
    if _can_create_missing_table(
        schema_mode=schema_mode, missing_table_policy=missing_table_policy
    ):
        return
    raise SchemaNotFoundError(
        f"Destination table does not yet exist: {target}. "
        "Use SchemaMode.OVERWRITE or set storage.missing_table_policy='create'."
    )


def _can_create_missing_table(
    *,
    schema_mode: SchemaMode,
    missing_table_policy: MissingTablePolicy,
) -> bool:
    """Return ``True`` when table creation is allowed for missing destination."""
    if missing_table_policy is MissingTablePolicy.CREATE:
        return True
    return schema_mode is SchemaMode.OVERWRITE


class _WritePolicy(TargetWriter, Generic[FrameT, PhysicalSchemaT]):
    """Base class for backend-specific target writers using Template Method pattern.

    Implements the shared write policy:
    1. Check if target exists
    2. If not exists → validate can create → create
    3. If exists → align schema → materialize → write

    Backend subclasses implement the hooks (_physical_schema, _append, etc.).
    """

    def __init__(
        self,
        *,
        resolver: TableRouteResolver,
        missing_table_policy: MissingTablePolicy,
    ) -> None:
        self._resolver = resolver
        self._missing_table_policy = missing_table_policy

    # ========================================================================
    # Public API (from TargetWriter)
    # ========================================================================

    def write(
        self,
        frame: FrameT,
        spec: TargetSpec,
        params_instance: Any,
        /,
        *,
        streaming: bool = False,
    ) -> None:
        """Write frame according to spec."""
        if isinstance(spec, FileSpec):
            self._write_file(frame, spec, streaming=streaming)
            return

        target = self._resolver.resolve(spec.table_ref)

        if isinstance(spec, AppendSpec):
            self._do_append(frame, target, spec, streaming)
        elif isinstance(spec, ReplaceSpec):
            self._do_replace(frame, target, spec, streaming)
        elif isinstance(spec, ReplacePartitionsSpec):
            self._do_replace_partitions(frame, target, spec, streaming)
        elif isinstance(spec, ReplaceWhereSpec):
            self._do_replace_where(frame, target, spec, params_instance, streaming)
        elif isinstance(spec, UpsertSpec):
            self._do_upsert(frame, target, spec, streaming)
        else:
            raise TypeError(f"Unsupported target spec: {type(spec)!r}")

    # ========================================================================
    # Template Methods (shared policy)
    # ========================================================================

    def _do_append(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        spec: AppendSpec,
        streaming: bool,
    ) -> None:
        """Append policy: check exists → create OR align → write."""
        existing = self._physical_schema(target)
        if existing is None:
            _ensure_can_create_missing_table(
                target=target,
                schema_mode=spec.schema_mode,
                missing_table_policy=self._missing_table_policy,
            )
            materialized = self._materialize(frame, streaming)
            self._create(materialized, target, schema_mode=spec.schema_mode)
            return
        aligned = self._align(frame, existing, spec.schema_mode)
        materialized = self._materialize(aligned, streaming)
        self._append(materialized, target, schema_mode=spec.schema_mode)

    def _do_replace(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        spec: ReplaceSpec,
        streaming: bool,
    ) -> None:
        """Replace policy: check exists → create OR align → write."""
        existing = self._physical_schema(target)
        if existing is None:
            _ensure_can_create_missing_table(
                target=target,
                schema_mode=spec.schema_mode,
                missing_table_policy=self._missing_table_policy,
            )
            materialized = self._materialize(frame, streaming)
            self._create(materialized, target, schema_mode=spec.schema_mode)
            return
        aligned = self._align(frame, existing, spec.schema_mode)
        materialized = self._materialize(aligned, streaming)
        self._replace(materialized, target, schema_mode=spec.schema_mode)

    def _do_replace_partitions(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        spec: ReplacePartitionsSpec,
        streaming: bool,
    ) -> None:
        """Replace partitions policy: check exists → create OR align → write."""
        existing = self._physical_schema(target)
        if existing is None:
            _ensure_can_create_missing_table(
                target=target,
                schema_mode=spec.schema_mode,
                missing_table_policy=self._missing_table_policy,
            )
            materialized = self._materialize(frame, streaming)
            self._create(
                materialized,
                target,
                schema_mode=spec.schema_mode,
                partition_cols=spec.partition_cols,
            )
            return
        aligned = self._align(frame, existing, spec.schema_mode)
        materialized = self._materialize(aligned, streaming)
        self._replace_partitions(
            materialized,
            target,
            partition_cols=spec.partition_cols,
            schema_mode=spec.schema_mode,
        )

    def _do_replace_where(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        spec: ReplaceWhereSpec,
        params_instance: Any,
        streaming: bool,
    ) -> None:
        """Replace where policy: check exists → create OR align → write."""
        existing = self._physical_schema(target)
        if existing is None:
            _ensure_can_create_missing_table(
                target=target,
                schema_mode=spec.schema_mode,
                missing_table_policy=self._missing_table_policy,
            )
            materialized = self._materialize(frame, streaming)
            self._create(materialized, target, schema_mode=spec.schema_mode)
            return
        predicate = self._predicate_to_sql(spec.replace_predicate, params_instance)
        aligned = self._align(frame, existing, spec.schema_mode)
        materialized = self._materialize(aligned, streaming)
        self._replace_where(
            materialized,
            target,
            predicate=predicate,
            schema_mode=spec.schema_mode,
        )

    def _do_upsert(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        spec: UpsertSpec,
        streaming: bool,
    ) -> None:
        """Upsert policy: check exists → create OR align+write."""
        existing = self._physical_schema(target)
        if existing is None:
            _ensure_can_create_missing_table(
                target=target,
                schema_mode=spec.schema_mode,
                missing_table_policy=self._missing_table_policy,
            )
            materialized = self._materialize(frame, streaming=False)
            self._create(
                materialized,
                target,
                schema_mode=spec.schema_mode,
                partition_cols=spec.partition_cols,
            )
            return
        aligned = self._align(frame, existing, spec.schema_mode)
        materialized = self._materialize(aligned, streaming=False)
        self._upsert(
            materialized,
            target,
            spec=spec,
            existing_schema=existing,
        )

    # ========================================================================
    # Abstract Hooks (backend-specific implementations)
    # ========================================================================

    def _physical_schema(self, target: ResolvedTarget) -> PhysicalSchemaT | None:
        """Read physical schema for target, or None if not exists."""
        raise NotImplementedError

    def _align(
        self,
        frame: FrameT,
        existing_schema: PhysicalSchemaT | None,
        mode: SchemaMode,
    ) -> FrameT:
        """Align frame schema with existing."""
        raise NotImplementedError

    def _materialize(self, frame: FrameT, streaming: bool) -> FrameT:
        """Materialize frame (collect if lazy)."""
        raise NotImplementedError

    def _predicate_to_sql(self, predicate: Any, params: Any) -> str:
        """Convert predicate to SQL string."""
        raise NotImplementedError

    def _create(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
        partition_cols: tuple[str, ...] = (),
    ) -> None:
        """Create new table."""
        raise NotImplementedError

    def _append(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Append to existing table."""
        raise NotImplementedError

    def _replace(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Replace existing table."""
        raise NotImplementedError

    def _replace_partitions(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
    ) -> None:
        """Replace partitions in existing table."""
        raise NotImplementedError

    def _replace_where(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        predicate: str,
        schema_mode: SchemaMode,
    ) -> None:
        """Replace where predicate matches."""
        raise NotImplementedError

    def _upsert(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        spec: UpsertSpec,
        existing_schema: PhysicalSchemaT,
    ) -> None:
        """Upsert/merge into existing table."""
        raise NotImplementedError

    def _write_file(
        self,
        frame: FrameT,
        spec: FileSpec,
        *,
        streaming: bool,
    ) -> None:
        """Write to file (CSV, JSON, Parquet)."""
        raise NotImplementedError


__all__ = ["_WritePolicy"]
