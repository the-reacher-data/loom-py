"""Deployment discovery + scheduling for loom Prefect flows.

Public surface
--------------
- :func:`discover_and_deploy_etls` — register one Prefect deployment per
  ETL, found either in a flows package or in YAML declarations.
- ``entrypoint`` — the module Prefect imports to rebuild a YAML-declared
  flow; it reads the declaration file from ``LOOM_ETL_CONFIG``.

Internal helpers split by concern:

- ``_discovery`` — walks the flows package or the YAML declarations and
  registers one deployment per ETL.
- ``_yaml_etls`` — reads and type-checks ETL declarations from YAML.
- ``_schedule`` — builds Prefect ``DeploymentScheduleCreate`` objects.

Per-ETL YAML loading and work-pool extraction live in
``loom.prefect._flow_yaml``, shared with ``loom.prefect.flow``; this package
imports from the flow package, never the reverse.
"""

from loom.prefect.deploy._discovery import discover_and_deploy_etls

__all__ = ["discover_and_deploy_etls"]
