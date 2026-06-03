"""Ephemeral retry-cycle manifest for ETL flows.

The manifest tracks which steps have been attempted within a single
correlation_id's retry window. It is stored in S3 only on failure and
deleted at the end of the last attempt — leaving no residue on the happy path.

Design decisions:
- PENDING is modelled as absence (a step absent from ``steps`` has not run yet).
- ``RunStatus`` from loom lineage is reused directly — no new enum.
- All model types are frozen msgspec structs for immutability and fast (de)serialisation.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Protocol

import msgspec

from loom.etl.lineage._records import RunStatus


class StepEntry(msgspec.Struct, frozen=True, kw_only=True):
    """State of one attempted step in the manifest.

    Absent from ``RunManifest.steps`` means the step is pending.

    Args:
        step: Step class name (used as the stable step identifier).
        status: ``SUCCESS`` or ``FAILED``. ``PENDING`` is never persisted.
        error: Human-readable error message, set only when ``status=FAILED``.
    """

    step: str
    status: RunStatus
    error: str | None = None


class RunManifest(msgspec.Struct, frozen=True, kw_only=True):
    """Ephemeral state for the retry cycle of one ``correlation_id``.

    Only attempted steps appear in ``steps``; absent steps are pending.

    Args:
        correlation_id: Business identifier this manifest belongs to.
        steps: Tuple of step entries — only steps that have been attempted.
        updated_at: Timestamp of the last manifest mutation.
    """

    correlation_id: str
    steps: tuple[StepEntry, ...]
    updated_at: datetime


class ManifestStore(Protocol):
    """Persistence backend for run manifests.

    Implementors must tolerate concurrent load/save from a single process.
    The manifest is small (< 1 KB) and written only on failure.
    """

    def load(self, correlation_id: str) -> RunManifest | None:
        """Return the stored manifest, or ``None`` if none exists.

        Args:
            correlation_id: Manifest key.
        """
        ...

    def save(self, manifest: RunManifest) -> None:
        """Persist the manifest.

        Args:
            manifest: Current state to persist.
        """
        ...

    def delete(self, correlation_id: str) -> None:
        """Remove the stored manifest.  No-op if it does not exist.

        Args:
            correlation_id: Manifest key.
        """
        ...


def completed_steps(manifest: RunManifest) -> frozenset[str]:
    """Return step names whose status is ``SUCCESS``.

    These steps are skipped in the next flow retry.

    Args:
        manifest: Current run manifest.

    Returns:
        Frozen set of step class names that completed successfully.
    """
    return frozenset(e.step for e in manifest.steps if e.status == RunStatus.SUCCESS)


def mark_step(
    manifest: RunManifest,
    step: str,
    status: RunStatus,
    *,
    error: str | None = None,
) -> RunManifest:
    """Return a new manifest with the given step added or updated.

    Pure function — the original manifest is not mutated.

    Args:
        manifest: Current manifest to derive the new one from.
        step: Step class name to record.
        status: ``SUCCESS`` or ``FAILED``.
        error: Error message, used only when ``status=FAILED``.

    Returns:
        New ``RunManifest`` with the step entry replaced or appended and
        ``updated_at`` set to the current UTC time.
    """
    entry = StepEntry(step=step, status=status, error=error)
    updated_steps = tuple(e for e in manifest.steps if e.step != step) + (entry,)
    return RunManifest(
        correlation_id=manifest.correlation_id,
        steps=updated_steps,
        updated_at=datetime.now(tz=UTC),
    )


__all__ = [
    "ManifestStore",
    "RunManifest",
    "StepEntry",
    "completed_steps",
    "mark_step",
]
