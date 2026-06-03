"""Flow configuration loader for loom.prefect."""

from __future__ import annotations

from pathlib import Path

import msgspec
import yaml


class FlowConfig(msgspec.Struct, frozen=True, kw_only=True):
    """Retry and execution policy for a single ETL flow.

    Args:
        flow_retries: Number of full-flow retries Prefect will attempt before
            declaring the run failed (each retry is a new Fargate container).
        flow_retry_delay_seconds: Seconds Prefect waits between flow retries.
        task_retries: Number of in-place step retries before the step is marked
            failed (within the same Fargate container).

    Example::

        cfg = _load_flow_config("config/etl_flows.yaml", "daily_orders")
    """

    flow_retries: int = 2
    flow_retry_delay_seconds: int = 60
    task_retries: int = 1


def _load_flow_config(config_path: str, flow_name: str) -> FlowConfig:
    """Load ``FlowConfig`` for *flow_name* from a YAML file.

    YAML structure::

        flows:
          my_etl:
            flow_retries: 2
            flow_retry_delay_seconds: 60
            task_retries: 1

    Args:
        config_path: Path to the YAML configuration file.
        flow_name: Key under ``flows`` to load.

    Returns:
        ``FlowConfig`` for the named flow, using ``FlowConfig`` defaults for
        any keys not present in the YAML section.

    Raises:
        KeyError: When ``flows`` key is missing or *flow_name* is not found.
    """
    raw = yaml.safe_load(Path(config_path).read_text())
    flows = raw["flows"]
    section = flows[flow_name]
    _defaults = FlowConfig()
    return FlowConfig(
        flow_retries=section.get("flow_retries", _defaults.flow_retries),
        flow_retry_delay_seconds=section.get(
            "flow_retry_delay_seconds",
            _defaults.flow_retry_delay_seconds,
        ),
        task_retries=section.get("task_retries", _defaults.task_retries),
    )


__all__ = ["FlowConfig"]
