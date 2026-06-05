"""Run manifest types and stores for ``loom.prefect``.

The manifest tracks which steps of a single correlation_id have already
completed across retries so the next attempt can skip them. It is the
only piece of cross-attempt state ``loom.prefect`` keeps; the rest of
each run is recomputed from scratch.

Public surface
--------------
- :class:`StepEntry`, :class:`RunManifest` — frozen manifest models.
- :class:`ManifestStore` — persistence protocol.
- :class:`S3JsonManifestStore` — production S3 backend.
- :func:`completed_steps`, :func:`mark_step` — pure helpers.
"""

from loom.prefect.manifest._model import (
    RunManifest,
    StepEntry,
    completed_steps,
    mark_step,
)
from loom.prefect.manifest._s3 import S3JsonManifestStore
from loom.prefect.manifest._store import ManifestStore

__all__ = [
    "ManifestStore",
    "RunManifest",
    "S3JsonManifestStore",
    "StepEntry",
    "completed_steps",
    "mark_step",
]
