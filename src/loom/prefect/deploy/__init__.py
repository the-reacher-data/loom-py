"""Deployment discovery + scheduling for loom Prefect flows.

Public surface
--------------
- :func:`discover_and_deploy_etls` — walk a flows package and register
  one Prefect deployment per ETL.

Internal helpers split by concern:

- ``_discovery`` — walks the flows package and drives ``_deploy_single``.
- ``_schedule`` — builds Prefect ``DeploymentScheduleCreate`` objects
  and pulls work-pool overrides out of each ETL's YAML.
- ``_yaml`` — loads the per-ETL YAML with a single level of ``extends:``.
"""

from loom.prefect.deploy._discovery import discover_and_deploy_etls

__all__ = ["discover_and_deploy_etls"]
