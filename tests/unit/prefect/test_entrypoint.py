"""Tests for ``loom.prefect.deploy.entrypoint`` (the module Prefect imports)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from prefect.flows import load_flow_from_entrypoint

from loom.core.config import ConfigError
from loom.prefect._meta import LOOM_ETL_META_ATTR, ETLFlowMeta
from loom.prefect.deploy import entrypoint

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "prefect" / "etls"
ENV_VAR = "LOOM_ETL_CONFIG"


def test_public_attribute_without_variable_raises_config_error_naming_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(ENV_VAR, raising=False)
    with pytest.raises(ConfigError, match=ENV_VAR):
        load_flow_from_entrypoint("loom.prefect.deploy.entrypoint.daily_orders")


def test_private_probe_without_variable_behaves_like_a_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(ENV_VAR, raising=False)
    assert hasattr(entrypoint, "__path__") is False
    with pytest.raises(AttributeError):
        entrypoint._private  # noqa: B018 - attribute access is the behaviour under test


def test_attribute_rebuilds_flow_from_declaration(monkeypatch: pytest.MonkeyPatch) -> None:
    config_uri = str(FIXTURES / "daily_orders.yaml")
    monkeypatch.setenv(ENV_VAR, config_uri)
    flow_obj = load_flow_from_entrypoint("loom.prefect.deploy.entrypoint.daily_orders")
    meta: ETLFlowMeta = getattr(flow_obj, LOOM_ETL_META_ATTR)
    assert flow_obj.name == "daily-orders"
    assert meta.name == "daily-orders"
    assert meta.config_path == config_uri
    assert meta.source_file == entrypoint.__file__
    assert meta.correlation_field == "run_date"
    assert meta.tags == ("orders", "daily")
    assert flow_obj.retries == 3
    assert flow_obj.retry_delay_seconds == 30


def test_unknown_attribute_lists_known_ones(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_VAR, str(FIXTURES / "billing.yaml"))
    with pytest.raises(ConfigError, match="invoice_sync.*monthly_close"):
        entrypoint.nope  # noqa: B018 - attribute access is the behaviour under test


@pytest.mark.parametrize(
    "module",
    [
        "loom.prefect.flow",
        "loom.prefect.deploy",
        "loom.prefect.deploy.entrypoint",
        "loom.prefect.flow._assemble",
    ],
)
def test_module_imports_first_in_a_fresh_interpreter(module: str) -> None:
    result = subprocess.run(
        [sys.executable, "-c", f"import {module}"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
