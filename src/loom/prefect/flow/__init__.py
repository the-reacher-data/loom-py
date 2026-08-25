"""Prefect flow factories for loom pipelines.

Public surface
--------------
- :func:`etl_flow` — build one ``@prefect.flow`` per ETL pipeline with a
  typed, synthesised signature derived from the pipeline's ``ParamsT``.
- :func:`maintenance_flow` — build one ``@prefect.flow`` per
  :class:`~loom.etl.maintenance.MaintenanceStep` (vacuum, compact, z-order).
- :func:`backfill_flow` — build one ``@prefect.flow`` that runs an
  ``ETLPipeline`` one chunk at a time (:data:`BackfillChunk` — hour, day,
  month or year), then finalizes once after all chunks.
- :data:`BackfillChunk` — chunk granularity literal accepted by
  :func:`backfill_flow`.

The sub-modules here split the factory's concerns:

- ``_factory`` / ``_body`` — the ``etl_flow()`` entrypoint and its runtime
  flow body.
- ``_maintenance`` — the ``maintenance_flow()`` entrypoint.
- ``_backfill`` / ``_backfill_body`` — the ``backfill_flow()`` entrypoint
  and its chunk-slicing runtime body.
- ``_assemble`` — YAML settings loading plus the ``prefect.flow`` decoration
  and deployer-metadata attachment shared by every factory.
- ``_runtime`` — manifest lifecycle and observer wiring shared by the flow
  bodies.
- ``_stages`` — process/step name validation against the compiled plan.
- ``_common`` — small helpers shared by the factories (tag coercion, …).
- ``_signature`` — synthesising the flow's parameter signature from a
  ``msgspec.Struct`` plus naive-datetime → UTC coercion.
- ``_run_name`` — computing each run's display name (cron slot vs.
  correlation id vs. fallback timestamp).
- ``_hooks`` — Prefect ``on_failure`` hook (deactivate the deployment's
  schedules when the flow finally fails).
"""

from loom.prefect.flow._backfill import BackfillChunk, backfill_flow
from loom.prefect.flow._factory import etl_flow
from loom.prefect.flow._maintenance import maintenance_flow

__all__ = ["BackfillChunk", "backfill_flow", "etl_flow", "maintenance_flow"]
