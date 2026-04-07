"""Write planner that resolves routes and snapshots schema once."""

from __future__ import annotations

from loom.etl.io.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpsertSpec,
)
from loom.etl.storage.route.resolver import TableRouteResolver
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


class WritePlanner:
    """Plan write operations from table target specs."""

    def __init__(self, resolver: TableRouteResolver, schema_reader: SchemaReader) -> None:
        self._resolver = resolver
        self._schema_reader = schema_reader

    def plan(self, spec: TableWriteSpec, *, streaming: bool = False) -> WriteOperation:
        """Resolve route + schema once and return a write operation."""
        target = self._resolver.resolve(spec.table_ref)
        existing_schema = self._schema_reader.read_schema(target)

        if isinstance(spec, AppendSpec):
            return AppendOp(
                target=target,
                schema_mode=spec.schema_mode,
                streaming=streaming,
                existing_schema=existing_schema,
            )
        if isinstance(spec, ReplaceSpec):
            return ReplaceOp(
                target=target,
                schema_mode=spec.schema_mode,
                streaming=streaming,
                existing_schema=existing_schema,
            )
        if isinstance(spec, ReplacePartitionsSpec):
            return ReplacePartitionsOp(
                target=target,
                partition_cols=spec.partition_cols,
                schema_mode=spec.schema_mode,
                streaming=streaming,
                existing_schema=existing_schema,
            )
        if isinstance(spec, ReplaceWhereSpec):
            return ReplaceWhereOp(
                target=target,
                replace_predicate=spec.replace_predicate,
                schema_mode=spec.schema_mode,
                streaming=streaming,
                existing_schema=existing_schema,
            )
        if isinstance(spec, UpsertSpec):
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
        raise TypeError(f"Unsupported table target spec: {type(spec)!r}")
