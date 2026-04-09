"""Shared helpers for backend SourceReader/TargetWriter adapters."""

from __future__ import annotations

from loom.etl.io.target import SchemaMode
from loom.etl.schema._schema import SchemaNotFoundError
from loom.etl.storage._config import MissingTablePolicy
from loom.etl.storage.route import ResolvedTarget


def ensure_can_create_missing_table(
    *,
    target: ResolvedTarget,
    schema_mode: SchemaMode,
    missing_table_policy: MissingTablePolicy,
) -> None:
    """Validate whether the write path may create a missing destination table."""
    if can_create_missing_table(schema_mode=schema_mode, missing_table_policy=missing_table_policy):
        return
    raise SchemaNotFoundError(
        f"Destination table does not yet exist: {target}. "
        "Use SchemaMode.OVERWRITE or set storage.missing_table_policy='create'."
    )


def can_create_missing_table(
    *,
    schema_mode: SchemaMode,
    missing_table_policy: MissingTablePolicy,
) -> bool:
    """Return ``True`` when table creation is allowed for missing destination."""
    if missing_table_policy is MissingTablePolicy.CREATE:
        return True
    return schema_mode is SchemaMode.OVERWRITE
