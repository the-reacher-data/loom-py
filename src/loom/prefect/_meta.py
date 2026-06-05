"""Cross-package metadata bridging the flow factory and the deployer.

The ``etl_flow()`` factory attaches a frozen ``ETLFlowMeta`` to each
decorated flow at ``__loom_etl_meta__``. The deployer reads it back to
build the matching Prefect Deployment. Keeping the type in its own
module breaks the otherwise-cyclic dependency between ``loom.prefect.flow``
and ``loom.prefect.deploy``.
"""

from __future__ import annotations

from typing import Any

from loom.core.model import LoomFrozenStruct

# Attribute name used to attach discovery metadata to each flow.
LOOM_ETL_META_ATTR = "__loom_etl_meta__"


class ETLFlowMeta(LoomFrozenStruct, frozen=True, kw_only=True):
    """Per-flow metadata consumed by :func:`discover_and_deploy_etls`.

    Args:
        name: Logical ETL name (used as the Prefect flow name AND the
            deployment name).
        config_path: Absolute path to the per-ETL YAML.
        source_file: Absolute path to the user's flow module (``__file__``).
        correlation_field: Name of the parameter used as correlation
            value, or ``None`` for random suffixes.
        schedule: The ``schedule`` block from the YAML, or ``None``.
        raw_params: Default parameter mapping pre-bound at deploy time.
        pool_config: Per-environment work-pool overrides
            (``environment → {"work_pool", "job_variables"}``).
    """

    name: str
    config_path: str
    source_file: str
    correlation_field: str | None
    schedule: dict[str, Any] | None
    raw_params: dict[str, Any]
    pool_config: dict[str, dict[str, Any]]


__all__ = ["LOOM_ETL_META_ATTR", "ETLFlowMeta"]
