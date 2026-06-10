"""Internal operation specs for Delta table maintenance.

These dataclasses are the compiled form of MaintainTable / MaintainSchema
declarations — they carry the final parameter values that the backend will
execute.  They are not part of the public API.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class VacuumSpec:
    """Parameters for a VACUUM operation.

    Args:
        retention_hours: Files older than this are eligible for deletion.
            ``None`` uses the table's ``delta.deletedFileRetentionDuration``
            property (default 168 hours / 7 days).
        dry_run: When ``True`` (the default), lists eligible files without
            deleting them.  Set ``dry_run=False`` explicitly in production.
    """

    retention_hours: int | None = None
    dry_run: bool = True


@dataclass(frozen=True)
class CompactSpec:
    """Parameters for a file compaction (bin-packing) operation.

    Args:
        target_size: Target output file size in bytes.  ``None`` lets
            delta-rs choose based on the table's configuration.
    """

    target_size: int | None = None


@dataclass(frozen=True)
class ZOrderSpec:
    """Parameters for a Z-Order clustering operation.

    Args:
        columns: Columns to Z-Order by.  Must be non-empty.
        target_size: Target output file size in bytes.  ``None`` uses
            delta-rs default.
    """

    columns: list[str] = field(default_factory=list)
    target_size: int | None = None


@dataclass(frozen=True)
class MaintenanceSpec:
    """All ops to execute against one concrete table (already resolved to its
    logical ref — the runner resolves it to a URI via the locator).

    Exactly one of ``compact`` or ``z_order`` may be set; setting both is a
    definition-time error caught by the builder.
    """

    table_ref: str
    vacuum: VacuumSpec | None = None
    compact: CompactSpec | None = None
    z_order: ZOrderSpec | None = None
