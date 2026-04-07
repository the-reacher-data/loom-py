"""Write planner that resolves routes and snapshots schema once."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from loom.etl.io.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpsertSpec,
)
from loom.etl.storage.route.model import ResolvedTarget
from loom.etl.storage.route.resolver import TableRouteResolver
from loom.etl.storage.schema.model import PhysicalSchema
from loom.etl.storage.schema.reader import SchemaReader
from loom.etl.storage.write.ops import (
    AppendOp,
    ReplaceOp,
    ReplacePartitionsOp,
    ReplaceWhereOp,
    UpsertOp,
    WriteOperation,
)

TableWriteSpec = AppendSpec | ReplaceSpec | ReplacePartitionsSpec | ReplaceWhereSpec | UpsertSpec


class PlanHandler(Protocol):
    """Build a write operation for one target spec type."""

    def build(
        self,
        spec: TableWriteSpec,
        *,
        target: ResolvedTarget,
        existing_schema: PhysicalSchema | None,
        streaming: bool,
    ) -> WriteOperation:
        """Build one write operation."""
        ...


@dataclass(frozen=True)
class _AppendHandler:
    def build(
        self,
        spec: TableWriteSpec,
        *,
        target: ResolvedTarget,
        existing_schema: PhysicalSchema | None,
        streaming: bool,
    ) -> WriteOperation:
        if not isinstance(spec, AppendSpec):
            raise TypeError(f"Expected AppendSpec, got {type(spec)!r}")
        return AppendOp(
            target=target,
            schema_mode=spec.schema_mode,
            streaming=streaming,
            existing_schema=existing_schema,
        )


@dataclass(frozen=True)
class _ReplaceHandler:
    def build(
        self,
        spec: TableWriteSpec,
        *,
        target: ResolvedTarget,
        existing_schema: PhysicalSchema | None,
        streaming: bool,
    ) -> WriteOperation:
        if not isinstance(spec, ReplaceSpec):
            raise TypeError(f"Expected ReplaceSpec, got {type(spec)!r}")
        return ReplaceOp(
            target=target,
            schema_mode=spec.schema_mode,
            streaming=streaming,
            existing_schema=existing_schema,
        )


@dataclass(frozen=True)
class _ReplacePartitionsHandler:
    def build(
        self,
        spec: TableWriteSpec,
        *,
        target: ResolvedTarget,
        existing_schema: PhysicalSchema | None,
        streaming: bool,
    ) -> WriteOperation:
        if not isinstance(spec, ReplacePartitionsSpec):
            raise TypeError(f"Expected ReplacePartitionsSpec, got {type(spec)!r}")
        return ReplacePartitionsOp(
            target=target,
            partition_cols=spec.partition_cols,
            schema_mode=spec.schema_mode,
            streaming=streaming,
            existing_schema=existing_schema,
        )


@dataclass(frozen=True)
class _ReplaceWhereHandler:
    def build(
        self,
        spec: TableWriteSpec,
        *,
        target: ResolvedTarget,
        existing_schema: PhysicalSchema | None,
        streaming: bool,
    ) -> WriteOperation:
        if not isinstance(spec, ReplaceWhereSpec):
            raise TypeError(f"Expected ReplaceWhereSpec, got {type(spec)!r}")
        return ReplaceWhereOp(
            target=target,
            replace_predicate=spec.replace_predicate,
            schema_mode=spec.schema_mode,
            streaming=streaming,
            existing_schema=existing_schema,
        )


@dataclass(frozen=True)
class _UpsertHandler:
    def build(
        self,
        spec: TableWriteSpec,
        *,
        target: ResolvedTarget,
        existing_schema: PhysicalSchema | None,
        streaming: bool,
    ) -> WriteOperation:
        if not isinstance(spec, UpsertSpec):
            raise TypeError(f"Expected UpsertSpec, got {type(spec)!r}")
        return UpsertOp(
            target=target,
            upsert_keys=spec.upsert_keys,
            partition_cols=spec.partition_cols,
            upsert_exclude=spec.upsert_exclude,
            upsert_include=spec.upsert_include,
            schema_mode=spec.schema_mode,
            streaming=streaming,
            existing_schema=existing_schema,
        )


class WritePlanner:
    """Plan write operations from table target specs."""

    def __init__(
        self,
        resolver: TableRouteResolver,
        schema_reader: SchemaReader,
        handlers: dict[type[object], PlanHandler] | None = None,
    ) -> None:
        self._resolver = resolver
        self._schema_reader = schema_reader
        self._handlers: dict[type[object], PlanHandler] = handlers or _default_handlers()

    def register(self, spec_type: type[object], handler: PlanHandler) -> None:
        """Register/override the planner handler for *spec_type*."""
        self._handlers[spec_type] = handler

    def plan(self, spec: TableWriteSpec, *, streaming: bool = False) -> WriteOperation:
        """Resolve route + schema once and return a write operation."""
        target = self._resolver.resolve(spec.table_ref)
        existing_schema = self._schema_reader.read_schema(target)
        handler = self._handlers.get(type(spec))
        if handler is None:
            raise TypeError(f"Unsupported table target spec: {type(spec)!r}")
        return handler.build(
            spec,
            target=target,
            existing_schema=existing_schema,
            streaming=streaming,
        )


def _default_handlers() -> dict[type[object], PlanHandler]:
    return {
        AppendSpec: _AppendHandler(),
        ReplaceSpec: _ReplaceHandler(),
        ReplacePartitionsSpec: _ReplacePartitionsHandler(),
        ReplaceWhereSpec: _ReplaceWhereHandler(),
        UpsertSpec: _UpsertHandler(),
    }
