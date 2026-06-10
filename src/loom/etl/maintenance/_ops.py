"""Internal operation specs for Delta table maintenance.

These dataclasses are the compiled form of MaintainTable / MaintainSchema
declarations — they carry the final parameter values that the backend will
execute.  They are not part of the public API.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from loom.etl.maintenance._protocol import DeltaTableMaintainer, OptimizeResult, VacuumResult
    from loom.etl.storage._locator import TableLocation


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

    name: ClassVar[str] = "vacuum"
    retention_hours: int | None = None
    dry_run: bool = True

    def execute(
        self,
        maintainer: DeltaTableMaintainer,
        uri: str,
        location: TableLocation,
    ) -> VacuumResult:
        return maintainer.vacuum(uri, self, location)


@dataclass(frozen=True)
class CompactSpec:
    """Parameters for a file compaction (bin-packing) operation.

    Args:
        target_size: Target output file size in bytes.  ``None`` lets
            delta-rs choose based on the table's configuration.
    """

    name: ClassVar[str] = "compact"
    target_size: int | None = None

    def execute(
        self,
        maintainer: DeltaTableMaintainer,
        uri: str,
        location: TableLocation,
    ) -> OptimizeResult:
        return maintainer.compact(uri, self, location)


@dataclass(frozen=True)
class ZOrderSpec:
    """Parameters for a Z-Order clustering operation.

    Args:
        columns: Columns to Z-Order by.  Must be non-empty.
        target_size: Target output file size in bytes.  ``None`` uses
            delta-rs default.
    """

    name: ClassVar[str] = "z_order"
    columns: list[str] = field(default_factory=list)
    target_size: int | None = None

    def execute(
        self,
        maintainer: DeltaTableMaintainer,
        uri: str,
        location: TableLocation,
    ) -> OptimizeResult:
        return maintainer.z_order(uri, self, location)


@dataclass(frozen=True)
class MaintenanceSpec:
    """All ops to execute against one concrete table (already resolved to its
    logical ref — the runner resolves it to a URI via the locator).
    """

    table_ref: str
    ops: tuple[VacuumSpec | CompactSpec | ZOrderSpec, ...] = ()
