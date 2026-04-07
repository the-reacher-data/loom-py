"""Write operation model used by generic table writers."""

from __future__ import annotations

from dataclasses import dataclass

from loom.etl.io.target import SchemaMode
from loom.etl.sql._predicate import PredicateNode
from loom.etl.storage.route.model import ResolvedTarget
from loom.etl.storage.schema.model import PhysicalSchema


@dataclass(frozen=True)
class AppendOp:
    """Append operation planned for a resolved target."""

    target: ResolvedTarget
    schema_mode: SchemaMode
    streaming: bool
    existing_schema: PhysicalSchema | None


@dataclass(frozen=True)
class ReplaceOp:
    """Full replace operation planned for a resolved target."""

    target: ResolvedTarget
    schema_mode: SchemaMode
    streaming: bool
    existing_schema: PhysicalSchema | None


@dataclass(frozen=True)
class ReplacePartitionsOp:
    """Replace-partitions operation planned for a resolved target."""

    target: ResolvedTarget
    partition_cols: tuple[str, ...]
    schema_mode: SchemaMode
    streaming: bool
    existing_schema: PhysicalSchema | None


@dataclass(frozen=True)
class ReplaceWhereOp:
    """Replace-where operation planned for a resolved target."""

    target: ResolvedTarget
    replace_predicate: PredicateNode
    schema_mode: SchemaMode
    streaming: bool
    existing_schema: PhysicalSchema | None


@dataclass(frozen=True)
class UpsertOp:
    """Upsert operation planned for a resolved target."""

    target: ResolvedTarget
    upsert_keys: tuple[str, ...]
    partition_cols: tuple[str, ...]
    upsert_exclude: tuple[str, ...]
    upsert_include: tuple[str, ...]
    schema_mode: SchemaMode
    streaming: bool
    existing_schema: PhysicalSchema | None


WriteOperation = AppendOp | ReplaceOp | ReplacePartitionsOp | ReplaceWhereOp | UpsertOp
