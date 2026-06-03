"""Tests for loom.prefect._config.FlowConfig and _load_flow_config.

Verifies:
- FlowConfig is a frozen msgspec.Struct with correct defaults.
- FlowConfig fields: flow_retries (int=2), flow_retry_delay_seconds (int=60),
  task_retries (int=1).
- FlowConfig is immutable (frozen=True).
- _load_flow_config() returns a FlowConfig from a valid YAML file.
- _load_flow_config() raises KeyError when flow_name is not found in YAML.
- _load_flow_config() picks the correct flow section from multi-flow YAML.
- _load_flow_config() reads only the flows.<flow_name> section, ignoring others.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from loom.prefect._config import FlowConfig, _load_flow_config

# ---------------------------------------------------------------------------
# FlowConfig struct tests
# ---------------------------------------------------------------------------


def test_flow_config_default_values() -> None:
    """FlowConfig has correct default values for all fields."""
    config = FlowConfig()
    assert config.flow_retries == 2
    assert config.flow_retry_delay_seconds == 60
    assert config.task_retries == 1


def test_flow_config_custom_values() -> None:
    """FlowConfig accepts explicit values for all fields."""
    config = FlowConfig(flow_retries=5, flow_retry_delay_seconds=120, task_retries=3)
    assert config.flow_retries == 5
    assert config.flow_retry_delay_seconds == 120
    assert config.task_retries == 3


def test_flow_config_is_immutable() -> None:
    """FlowConfig must be frozen (immutable)."""
    config = FlowConfig()
    with pytest.raises((TypeError, AttributeError)):
        config.flow_retries = 99  # type: ignore[misc]


def test_flow_config_partial_overrides() -> None:
    """FlowConfig allows partial overrides; non-specified fields keep defaults."""
    config = FlowConfig(task_retries=0)
    assert config.flow_retries == 2
    assert config.flow_retry_delay_seconds == 60
    assert config.task_retries == 0


# ---------------------------------------------------------------------------
# _load_flow_config tests
# ---------------------------------------------------------------------------


@pytest.fixture
def yaml_file(tmp_path: Path) -> Path:
    """Create a temp YAML file with two flow configs."""
    content = textwrap.dedent("""\
        flows:
          my_etl:
            flow_retries: 3
            flow_retry_delay_seconds: 90
            task_retries: 2
          other_etl:
            flow_retries: 1
            flow_retry_delay_seconds: 30
            task_retries: 0
    """)
    config_file = tmp_path / "flows.yaml"
    config_file.write_text(content)
    return config_file


def test_load_flow_config_reads_correct_flow(yaml_file: Path) -> None:
    """_load_flow_config returns config for the specified flow name."""
    config = _load_flow_config(str(yaml_file), "my_etl")
    assert config.flow_retries == 3
    assert config.flow_retry_delay_seconds == 90
    assert config.task_retries == 2


def test_load_flow_config_reads_second_flow(yaml_file: Path) -> None:
    """_load_flow_config can load the second flow from a multi-flow YAML."""
    config = _load_flow_config(str(yaml_file), "other_etl")
    assert config.flow_retries == 1
    assert config.flow_retry_delay_seconds == 30
    assert config.task_retries == 0


def test_load_flow_config_returns_flow_config_instance(yaml_file: Path) -> None:
    """_load_flow_config returns a FlowConfig instance."""
    config = _load_flow_config(str(yaml_file), "my_etl")
    assert isinstance(config, FlowConfig)


def test_load_flow_config_raises_key_error_when_flow_not_found(yaml_file: Path) -> None:
    """_load_flow_config raises KeyError when flow_name is not in YAML."""
    with pytest.raises(KeyError):
        _load_flow_config(str(yaml_file), "nonexistent_flow")


def test_load_flow_config_with_defaults_yaml(tmp_path: Path) -> None:
    """_load_flow_config uses FlowConfig defaults for missing optional keys."""
    content = textwrap.dedent("""\
        flows:
          minimal_etl:
            flow_retries: 1
    """)
    config_file = tmp_path / "minimal.yaml"
    config_file.write_text(content)

    config = _load_flow_config(str(config_file), "minimal_etl")
    assert config.flow_retries == 1
    # Non-specified keys must fall back to FlowConfig defaults
    assert config.flow_retry_delay_seconds == 60
    assert config.task_retries == 1


def test_load_flow_config_raises_when_flows_key_missing(tmp_path: Path) -> None:
    """_load_flow_config raises KeyError when 'flows' top-level key is absent."""
    content = textwrap.dedent("""\
        settings:
          debug: true
    """)
    config_file = tmp_path / "no_flows.yaml"
    config_file.write_text(content)

    with pytest.raises(KeyError):
        _load_flow_config(str(config_file), "any_flow")
