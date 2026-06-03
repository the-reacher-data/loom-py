"""loom.prefect — Prefect integration for ETL observability.

Provides the building blocks to expose loom ETL pipelines as observable
Prefect flows where each ``ETLStep`` becomes an individual ``@task`` visible
in the Prefect UI.

Install the optional extra to get ``prefect`` as a dependency::

    pip install loom-kernel[prefect]

Public API
----------
- :class:`FlowCtx` — operational context shared by all ETL flows.
- :func:`build_etl_flow` — factory that wraps an ``ETLPipeline`` in a
  Prefect ``@flow``.
- :class:`ManifestStore` — protocol for the ephemeral retry manifest backend.
- :class:`RunManifest` / :class:`StepEntry` — manifest data model.
- :class:`S3JsonManifestStore` — S3-backed manifest store via fsspec.
- :class:`PrefectObserver` — ``LifecycleObserver`` that forwards loom events
  to the Prefect run logger.

Architecture contract
---------------------
``loom.core`` and ``loom.etl`` must never import from ``loom.prefect`` or
``prefect``.  The dependency arrow points inward: ``loom.prefect`` depends on
``loom.etl`` and ``prefect``, never the other way around.
"""

from loom.prefect._ctx import FlowCtx
from loom.prefect._flow_builder import build_etl_flow
from loom.prefect._manifest import ManifestStore, RunManifest, StepEntry
from loom.prefect._manifest_s3 import S3JsonManifestStore
from loom.prefect._observer import PrefectObserver

__all__ = [
    "FlowCtx",
    "ManifestStore",
    "PrefectObserver",
    "RunManifest",
    "S3JsonManifestStore",
    "StepEntry",
    "build_etl_flow",
]
