"""delta-rs (deltalake) implementation of DeltaTableMaintainer.

All deltalake imports are lazy (inside method bodies) so that importing
``loom.etl.maintenance`` does not force the ``deltalake`` package at module
load time for users who haven't installed the ``[delta]`` extra.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from loom.etl.maintenance._ops import CompactSpec, VacuumSpec, ZOrderSpec
    from loom.etl.maintenance._protocol import OptimizeResult, VacuumResult
    from loom.etl.storage._locator import TableLocation

_log = logging.getLogger(__name__)


class DeltaRsMaintainer:
    """Execute maintenance operations via the ``deltalake`` Python package (delta-rs)."""

    def vacuum(
        self,
        uri: str,
        spec: VacuumSpec,
        location: TableLocation,
    ) -> VacuumResult:
        """Run VACUUM on the Delta table at *uri*."""
        from deltalake import CommitProperties, DeltaTable

        from loom.etl.maintenance._protocol import VacuumResult

        dt = DeltaTable(uri, storage_options=location.storage_options or {})
        commit = CommitProperties(**location.commit) if location.commit else None
        deleted = dt.vacuum(
            retention_hours=spec.retention_hours,
            dry_run=spec.dry_run,
            commit_properties=commit,
        )
        _log.info(
            "vacuum uri=%s dry_run=%s files=%d",
            uri,
            spec.dry_run,
            len(deleted),
        )
        return VacuumResult(files_deleted=deleted)

    def compact(
        self,
        uri: str,
        spec: CompactSpec,
        location: TableLocation,
    ) -> OptimizeResult:
        """Run bin-packing compaction on the Delta table at *uri*."""
        from deltalake import CommitProperties, DeltaTable, WriterProperties

        from loom.etl.maintenance._protocol import OptimizeResult

        dt = DeltaTable(uri, storage_options=location.storage_options or {})
        writer = WriterProperties(**location.writer) if location.writer else None
        commit = CommitProperties(**location.commit) if location.commit else None
        result = dt.optimize.compact(
            target_size=spec.target_size,
            writer_properties=writer,
            commit_properties=commit,
        )
        _log.info(
            "compact uri=%s added=%d removed=%d",
            uri,
            result["numFilesAdded"],
            result["numFilesRemoved"],
        )
        return OptimizeResult(
            num_files_added=result["numFilesAdded"],
            num_files_removed=result["numFilesRemoved"],
        )

    def z_order(
        self,
        uri: str,
        spec: ZOrderSpec,
        location: TableLocation,
    ) -> OptimizeResult:
        """Run Z-Order clustering on the Delta table at *uri*."""
        from deltalake import CommitProperties, DeltaTable, WriterProperties

        from loom.etl.maintenance._protocol import OptimizeResult

        dt = DeltaTable(uri, storage_options=location.storage_options or {})
        writer = WriterProperties(**location.writer) if location.writer else None
        commit = CommitProperties(**location.commit) if location.commit else None
        result = dt.optimize.z_order(
            columns=spec.columns,
            target_size=spec.target_size,
            writer_properties=writer,
            commit_properties=commit,
        )
        _log.info(
            "z_order uri=%s columns=%s added=%d removed=%d",
            uri,
            spec.columns,
            result["numFilesAdded"],
            result["numFilesRemoved"],
        )
        return OptimizeResult(
            num_files_added=result["numFilesAdded"],
            num_files_removed=result["numFilesRemoved"],
        )
