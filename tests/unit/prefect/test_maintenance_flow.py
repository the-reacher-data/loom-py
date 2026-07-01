"""Tests for Prefect maintenance-flow observability."""

from __future__ import annotations

import uuid
from pathlib import Path
from unittest.mock import MagicMock

import msgspec
import pytest

from loom.etl.maintenance._runner import MaintenanceReport
from loom.etl.maintenance._step import MaintenanceStep
from loom.prefect.flow import _maintenance


class _MaintenanceParams(msgspec.Struct, frozen=True):
    dry_run: bool = False


class _MaintenanceStep(MaintenanceStep[_MaintenanceParams]):
    operations = []


def _write_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "maintenance.yaml"
    config_path.write_text("params: {}\n", encoding="utf-8")
    return config_path


def _build_flow(tmp_path: Path) -> object:
    return _maintenance.maintenance_flow(
        name="maintenance",
        step=_MaintenanceStep,
        params_type=_MaintenanceParams,
        config_path=str(_write_config(tmp_path)),
        source_file=__file__,
    )


def test_maintenance_flow_bridges_logs_to_current_flow_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    flow_run_id = uuid.uuid4()
    runner = MagicMock()
    runner.run.return_value = MaintenanceReport()
    install = MagicMock()
    uninstall = MagicMock()

    monkeypatch.setattr(_maintenance, "prefect_flow_run_id", lambda: flow_run_id)
    monkeypatch.setattr(_maintenance, "install_log_bridge", install)
    monkeypatch.setattr(_maintenance, "uninstall_log_bridge", uninstall)
    monkeypatch.setattr(_maintenance, "_load_yaml", lambda path: (MagicMock(), path))
    monkeypatch.setattr(_maintenance.MaintenanceRunner, "from_config", lambda config: runner)

    flow = _build_flow(tmp_path)
    flow.fn(dry_run=True)  # type: ignore[attr-defined]

    install.assert_called_once_with(flow_run_id)
    uninstall.assert_called_once_with()
    runner.run.assert_called_once()


def test_maintenance_flow_uninstalls_log_bridge_after_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    uninstall = MagicMock()

    monkeypatch.setattr(_maintenance, "prefect_flow_run_id", uuid.uuid4)
    monkeypatch.setattr(_maintenance, "install_log_bridge", MagicMock())
    monkeypatch.setattr(_maintenance, "uninstall_log_bridge", uninstall)
    monkeypatch.setattr(
        _maintenance,
        "_load_yaml",
        MagicMock(side_effect=RuntimeError("storage config failed")),
    )

    flow = _build_flow(tmp_path)
    with pytest.raises(RuntimeError, match="storage config failed"):
        flow.fn(dry_run=False)  # type: ignore[attr-defined]

    uninstall.assert_called_once_with()
