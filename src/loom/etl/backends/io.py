"""Generic SourceReader/TargetWriter adapters backed by an injected backend."""

from __future__ import annotations

from typing import Any, Generic, TypeVar, cast

from loom.etl.backends._protocols import ComputeBackend
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
from loom.etl.storage._config import MissingTablePolicy
from loom.etl.storage.protocols import SourceReader, TargetWriter
from loom.etl.storage.routing import ResolvedTarget, TableRouteResolver

_InputFrameT = TypeVar("_InputFrameT")


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


class GenericSourceReader(SourceReader, Generic[_InputFrameT]):
    """Agnostic SourceReader using an injected backend and route resolver."""

    def __init__(
        self,
        *,
        backend: ComputeBackend[_InputFrameT, Any],
        resolver: TableRouteResolver,
        reader_name: str,
        unsupported_source_hint: str | None = None,
    ) -> None:
        self._backend = backend
        self._resolver = resolver
        self._reader_name = reader_name
        self._unsupported_source_hint = unsupported_source_hint

    def read(self, spec: SourceSpec, params_instance: Any) -> _InputFrameT:
        if isinstance(spec, TableSourceSpec):
            target = self._resolver.resolve(spec.table_ref)
            return cast(
                _InputFrameT,
                self._backend.read.table(
                    target,
                    columns=spec.columns,
                    predicates=spec.predicates,
                    params=params_instance,
                    schema=spec.schema,
                    json_columns=spec.json_columns,
                ),
            )

        if isinstance(spec, FileSourceSpec):
            return cast(
                _InputFrameT,
                self._backend.read.file(
                    spec.path,
                    spec.format,
                    spec.read_options,
                    columns=spec.columns,
                    schema=spec.schema,
                    json_columns=spec.json_columns,
                ),
            )

        if self._unsupported_source_hint is not None:
            raise TypeError(
                f"{self._reader_name} cannot read source kind {spec.kind!r}. "
                f"{self._unsupported_source_hint}"
            )
        raise TypeError(f"{self._reader_name} does not support source spec: {type(spec)!r}")


class GenericTargetWriter(TargetWriter, Generic[_InputFrameT]):
    """Agnostic TargetWriter using an injected backend and route resolver."""

    def __init__(
        self,
        *,
        backend: ComputeBackend[_InputFrameT, Any],
        resolver: TableRouteResolver,
        missing_table_policy: MissingTablePolicy,
        writer_name: str,
    ) -> None:
        self._backend = backend
        self._resolver = resolver
        self._missing_table_policy = missing_table_policy
        self._writer_name = writer_name

    def write(
        self,
        frame: _InputFrameT,
        spec: TargetSpec,
        params_instance: Any,
        /,
        *,
        streaming: bool = False,
    ) -> None:
        from loom.etl.io.target._file import FileSpec

        if isinstance(spec, FileSpec):
            self._backend.write.file(
                frame,
                spec.path,
                spec.format,
                spec.write_options,
                streaming=streaming,
            )
            return

        table_specs = (AppendSpec, ReplaceSpec, ReplacePartitionsSpec, ReplaceWhereSpec, UpsertSpec)
        if not isinstance(spec, table_specs):
            raise TypeError(f"{self._writer_name} does not support target spec: {type(spec)!r}")

        target = self._resolver.resolve(spec.table_ref)

        if isinstance(spec, AppendSpec):
            self._write_append(frame, target, spec, streaming)
            return
        if isinstance(spec, ReplaceSpec):
            self._write_replace(frame, target, spec, streaming)
            return
        if isinstance(spec, ReplacePartitionsSpec):
            self._write_replace_partitions(frame, target, spec, streaming)
            return
        if isinstance(spec, ReplaceWhereSpec):
            self._write_replace_where(frame, target, spec, params_instance, streaming)
            return
        if isinstance(spec, UpsertSpec):
            self._write_upsert(frame, target, spec, streaming)
            return

        raise TypeError(f"{self._writer_name} does not support target spec: {type(spec)!r}")

    def append(
        self,
        frame: _InputFrameT,
        table_ref: TableRef,
        params_instance: Any,
        *,
        streaming: bool = False,
    ) -> None:
        self.write(
            frame,
            AppendSpec(table_ref=table_ref, schema_mode=SchemaMode.EVOLVE),
            params_instance,
            streaming=streaming,
        )

    def _write_append(
        self,
        frame: _InputFrameT,
        target: ResolvedTarget,
        spec: AppendSpec,
        streaming: bool,
    ) -> None:
        existing = self._backend.schema.physical(target)
        if existing is None:
            _ensure_can_create_missing_table(
                target=target,
                schema_mode=spec.schema_mode,
                missing_table_policy=self._missing_table_policy,
            )
            materialized = self._backend.schema.materialize(frame, streaming)
            self._backend.write.create(materialized, target, schema_mode=spec.schema_mode)
            return
        aligned = self._backend.schema.align(frame, existing, spec.schema_mode)
        materialized = self._backend.schema.materialize(aligned, streaming)
        self._backend.write.append(materialized, target, schema_mode=spec.schema_mode)

    def _write_replace(
        self,
        frame: _InputFrameT,
        target: ResolvedTarget,
        spec: ReplaceSpec,
        streaming: bool,
    ) -> None:
        existing = self._backend.schema.physical(target)
        if existing is None:
            _ensure_can_create_missing_table(
                target=target,
                schema_mode=spec.schema_mode,
                missing_table_policy=self._missing_table_policy,
            )
            materialized = self._backend.schema.materialize(frame, streaming)
            self._backend.write.create(materialized, target, schema_mode=spec.schema_mode)
            return
        aligned = self._backend.schema.align(frame, existing, spec.schema_mode)
        materialized = self._backend.schema.materialize(aligned, streaming)
        self._backend.write.replace(materialized, target, schema_mode=spec.schema_mode)

    def _write_replace_partitions(
        self,
        frame: _InputFrameT,
        target: ResolvedTarget,
        spec: ReplacePartitionsSpec,
        streaming: bool,
    ) -> None:
        existing = self._backend.schema.physical(target)
        if existing is None:
            _ensure_can_create_missing_table(
                target=target,
                schema_mode=spec.schema_mode,
                missing_table_policy=self._missing_table_policy,
            )
            materialized = self._backend.schema.materialize(frame, streaming)
            self._backend.write.create(
                materialized,
                target,
                schema_mode=spec.schema_mode,
                partition_cols=spec.partition_cols,
            )
            return
        aligned = self._backend.schema.align(frame, existing, spec.schema_mode)
        materialized = self._backend.schema.materialize(aligned, streaming)
        self._backend.write.replace_partitions(
            materialized,
            target,
            partition_cols=spec.partition_cols,
            schema_mode=spec.schema_mode,
        )

    def _write_replace_where(
        self,
        frame: _InputFrameT,
        target: ResolvedTarget,
        spec: ReplaceWhereSpec,
        params_instance: Any,
        streaming: bool,
    ) -> None:
        existing = self._backend.schema.physical(target)
        if existing is None:
            _ensure_can_create_missing_table(
                target=target,
                schema_mode=spec.schema_mode,
                missing_table_policy=self._missing_table_policy,
            )
            materialized = self._backend.schema.materialize(frame, streaming)
            self._backend.write.create(materialized, target, schema_mode=spec.schema_mode)
            return
        predicate = self._backend.schema.to_sql(spec.replace_predicate, params_instance)
        aligned = self._backend.schema.align(frame, existing, spec.schema_mode)
        materialized = self._backend.schema.materialize(aligned, streaming)
        self._backend.write.replace_where(
            materialized,
            target,
            predicate=predicate,
            schema_mode=spec.schema_mode,
        )

    def _write_upsert(
        self,
        frame: _InputFrameT,
        target: ResolvedTarget,
        spec: UpsertSpec,
        streaming: bool,
    ) -> None:
        existing = self._backend.schema.physical(target)
        if existing is None:
            _ensure_can_create_missing_table(
                target=target,
                schema_mode=spec.schema_mode,
                missing_table_policy=self._missing_table_policy,
            )
            materialized = self._backend.schema.materialize(frame, streaming=False)
            self._backend.write.create(
                materialized,
                target,
                schema_mode=spec.schema_mode,
                partition_cols=spec.partition_cols,
            )
            return
        aligned = self._backend.schema.align(frame, existing, spec.schema_mode)
        materialized = self._backend.schema.materialize(aligned, streaming=False)
        self._backend.write.upsert(
            materialized,
            target,
            keys=spec.upsert_keys,
            partition_cols=spec.partition_cols,
            upsert_exclude=spec.upsert_exclude,
            upsert_include=spec.upsert_include,
            schema_mode=spec.schema_mode,
            existing_schema=existing,
            streaming=streaming,
        )
