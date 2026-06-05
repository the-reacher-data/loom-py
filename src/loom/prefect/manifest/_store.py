"""Persistence protocol for run manifests."""

from __future__ import annotations

from typing import Protocol

from loom.prefect.manifest._model import RunManifest


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


__all__ = ["ManifestStore"]
