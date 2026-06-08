"""Prefect flow factory for loom ETL pipelines.

Public surface
--------------
- :func:`etl_flow` — build one ``@prefect.flow`` per ETL with a typed,
  synthesised signature derived from the pipeline's ``ParamsT``.

The sub-modules here split the factory's concerns:

- ``_factory`` — the ``etl_flow()`` entrypoint and the closure that
  becomes the flow body.
- ``_signature`` — synthesising the flow's parameter signature from a
  ``msgspec.Struct`` plus naive-datetime → UTC coercion.
- ``_run_name`` — computing each run's display name (cron slot vs.
  correlation id vs. fallback timestamp).
- ``_hooks`` — Prefect ``on_failure`` hook (deactivate the deployment's
  schedules when the flow finally fails).
"""

from loom.prefect.flow._factory import etl_flow

__all__ = ["etl_flow"]
