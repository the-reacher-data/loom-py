"""Write planner that resolves routes and snapshots schema once."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

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

# Callable contract for handler functions registered in WritePlanner.
# Signature: (spec, *, target, existing_schema, streaming) -> WriteOperation
PlanBuildFn = Callable[..., WriteOperation]


def _build_append(
    spec: AppendSpec,
    *,
    target: ResolvedTarget,
    existing_schema: PhysicalSchema | None,
    streaming: bool,
) -> WriteOperation:
    return AppendOp(
        target=target,
        schema_mode=spec.schema_mode,
        streaming=streaming,
        existing_schema=existing_schema,
    )


def _build_replace(
    spec: ReplaceSpec,
    *,
    target: ResolvedTarget,
    existing_schema: PhysicalSchema | None,
    streaming: bool,
) -> WriteOperation:
    return ReplaceOp(
        target=target,
        schema_mode=spec.schema_mode,
        streaming=streaming,
        existing_schema=existing_schema,
    )


def _build_replace_partitions(
    spec: ReplacePartitionsSpec,
    *,
    target: ResolvedTarget,
    existing_schema: PhysicalSchema | None,
    streaming: bool,
) -> WriteOperation:
    return ReplacePartitionsOp(
        target=target,
        partition_cols=spec.partition_cols,
        schema_mode=spec.schema_mode,
        streaming=streaming,
        existing_schema=existing_schema,
    )


def _build_replace_where(
    spec: ReplaceWhereSpec,
    *,
    target: ResolvedTarget,
    existing_schema: PhysicalSchema | None,
    streaming: bool,
) -> WriteOperation:
    return ReplaceWhereOp(
        target=target,
        replace_predicate=spec.replace_predicate,
        schema_mode=spec.schema_mode,
        streaming=streaming,
        existing_schema=existing_schema,
    )


def _build_upsert(
    spec: UpsertSpec,
    *,
    target: ResolvedTarget,
    existing_schema: PhysicalSchema | None,
    streaming: bool,
) -> WriteOperation:
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
    """Plan write operations from table target specs.

    Resolves the storage route and snapshots the existing physical schema
    exactly once per spec, then delegates construction of the concrete
    :class:`~loom.etl.storage.write.WriteOperation` to a registered handler.

    Args:
        resolver:     Resolves a :class:`~loom.etl.schema._table.TableRef` to
                      its :class:`~loom.etl.storage.route.model.ResolvedTarget`.
        schema_reader: Reads the current physical schema of a resolved target.
        handlers:     Override the default spec-type → builder mapping.
                      Keys must be concrete spec classes; values must be
                      :data:`PlanBuildFn` callables.

    Example::

        planner = WritePlanner(resolver=my_resolver, schema_reader=my_schema_reader)
        op = planner.plan(IntoTable("raw.orders").replace()._to_spec())
    """

    def __init__(
        self,
        resolver: TableRouteResolver,
        schema_reader: SchemaReader,
        handlers: dict[type[Any], PlanBuildFn] | None = None,
    ) -> None:
        self._resolver = resolver
        self._schema_reader = schema_reader
        self._handlers: dict[type[Any], PlanBuildFn] = handlers or _default_handlers()

    def register(self, spec_type: type[Any], handler: PlanBuildFn) -> None:
        """Register or override the builder for *spec_type*.

        Args:
            spec_type: Concrete spec class (e.g. ``AppendSpec``).  Registering
                       an already-known type silently replaces the existing builder.
            handler:   Callable with signature
                       ``(spec, *, target, existing_schema, streaming) -> WriteOperation``.
        """
        self._handlers[spec_type] = handler

    def plan(self, spec: TableWriteSpec, *, streaming: bool = False) -> WriteOperation:
        """Resolve route + schema once and return a write operation.

        Args:
            spec:      Compiled table write spec.
            streaming: Pass ``True`` to request streaming collection where
                       supported by the backend executor.

        Returns:
            A fully resolved :class:`~loom.etl.storage.write.WriteOperation`.

        Raises:
            TypeError: When no handler is registered for ``type(spec)``.
        """
        target = self._resolver.resolve(spec.table_ref)
        existing_schema = self._schema_reader.read_schema(target)
        handler = self._handlers.get(type(spec))
        if handler is None:
            raise TypeError(f"Unsupported table target spec: {type(spec)!r}")
        return handler(
            spec,
            target=target,
            existing_schema=existing_schema,
            streaming=streaming,
        )


def _default_handlers() -> dict[type[Any], PlanBuildFn]:
    return {
        AppendSpec: _build_append,
        ReplaceSpec: _build_replace,
        ReplacePartitionsSpec: _build_replace_partitions,
        ReplaceWhereSpec: _build_replace_where,
        UpsertSpec: _build_upsert,
    }
