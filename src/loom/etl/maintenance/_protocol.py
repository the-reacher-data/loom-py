"""Backend protocol and result types for Delta table maintenance."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from loom.etl.maintenance._ops import CompactSpec, VacuumSpec, ZOrderSpec
    from loom.etl.storage._locator import TableLocation


@dataclass
class VacuumResult:
    """Result of a VACUUM operation.

    Args:
        files_deleted: Paths of the files that were deleted (or that *would*
            be deleted when ``dry_run=True``).
    """

    files_deleted: list[str] = field(default_factory=list)


@dataclass
class OptimizeResult:
    """Result of a compaction or Z-Order operation.

    Uses counts (not lists) — consistent with the ``numFilesAdded`` /
    ``numFilesRemoved`` keys returned by the deltalake metrics dict.
    """

    num_files_added: int = 0
    num_files_removed: int = 0


@dataclass
class TableMaintenanceResult:
    """Per-table maintenance outcome.

    Args:
        table_ref: Logical table reference (e.g. ``"raw.events"``).
        vacuum: Result of the vacuum step, or ``None`` if not requested.
        compact: Result of the compact step, or ``None`` if not requested.
        z_order: Result of the z-order step, or ``None`` if not requested.
        error: Exception caught during execution, or ``None`` on success.
        duration_seconds: Wall-clock time for all ops on this table.
    """

    table_ref: str
    vacuum: VacuumResult | None = None
    compact: OptimizeResult | None = None
    z_order: OptimizeResult | None = None
    error: Exception | None = None
    duration_seconds: float = 0.0

    @property
    def ok(self) -> bool:
        """``True`` when no error occurred."""
        return self.error is None


@runtime_checkable
class DeltaTableMaintainer(Protocol):
    """Protocol for executing maintenance operations on a single Delta table.

    Implementations receive the full :class:`~loom.etl.storage._locator.TableLocation`
    so they can propagate ``storage_options``, ``writer_properties``, and
    ``commit_properties`` to every delta-rs call — matching the behaviour of
    :meth:`loom.etl.backends.polars._writer.PolarsTargetWriter._write_kwargs`.
    """

    def vacuum(
        self,
        uri: str,
        spec: VacuumSpec,
        location: TableLocation,
    ) -> VacuumResult:
        """Run VACUUM on the table at *uri*."""
        ...

    def compact(
        self,
        uri: str,
        spec: CompactSpec,
        location: TableLocation,
    ) -> OptimizeResult:
        """Run bin-packing compaction on the table at *uri*."""
        ...

    def z_order(
        self,
        uri: str,
        spec: ZOrderSpec,
        location: TableLocation,
    ) -> OptimizeResult:
        """Run Z-Order clustering on the table at *uri*."""
        ...
