"""Prefect flow factories for loom pipelines.

Public surface
--------------
- :func:`etl_flow` — build one ``@prefect.flow`` per ETL pipeline with a
  typed, synthesised signature derived from the pipeline's ``ParamsT``.
- :func:`maintenance_flow` — build one ``@prefect.flow`` per
  :class:`~loom.etl.maintenance.MaintenanceStep` (vacuum, compact, z-order).
- :func:`backfill_flow` — build one ``@prefect.flow`` that runs an
  ``ETLPipeline`` one natural day at a time, then finalizes once.

The sub-modules here split the factory's concerns:

- ``_factory`` — the ``etl_flow()`` entrypoint and the closure that
  becomes the flow body.
- ``_maintenance`` — the ``maintenance_flow()`` entrypoint.
- ``_common`` — helpers shared by both factories (tag coercion, …).
- ``_signature`` — synthesising the flow's parameter signature from a
  ``msgspec.Struct`` plus naive-datetime → UTC coercion.
- ``_run_name`` — computing each run's display name (cron slot vs.
  correlation id vs. fallback timestamp).
- ``_hooks`` — Prefect ``on_failure`` hook (deactivate the deployment's
  schedules when the flow finally fails).
"""

from loom.prefect.flow._backfill import backfill_flow
from loom.prefect.flow._factory import etl_flow
from loom.prefect.flow._maintenance import maintenance_flow

__all__ = ["backfill_flow", "etl_flow", "maintenance_flow"]
