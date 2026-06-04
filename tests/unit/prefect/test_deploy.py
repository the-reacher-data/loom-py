"""Tests for loom.prefect._deploy.

Verifies:
- ``_generic_etl_flow`` lives at module level and is picklable.
- ``deploy_etl`` wires config_path, env, params and schedule into
  ``.with_options(name=...).deploy(...)``.
- ``_compute_correlation_id`` strategies.
"""

from __future__ import annotations

import pickle
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import msgspec
import pytest

from loom.prefect._deploy import (
    _compute_correlation_id,
    _generic_etl_flow,
    deploy_etl,
)


def _write_etl_yaml(tmp_path: Path, *, with_schedule: bool = True) -> Path:
    schedule_block = (
        """
schedule:
  cron: "0 6 * * *"
  timezone: Europe/Madrid
  enabled: true
"""
        if with_schedule
        else ""
    )
    content = f"""
etl: daily-orders
pipeline: my_app.pipelines.daily_orders:DailyOrdersETL
params_type: my_app.pipelines.daily_orders:DailyOrdersParams
correlation_field: business_date
{schedule_block}
params:
  business_date: ${{today-1d}}
  countries: ["ES", "FR"]
environments:
  prod:
    type: fargate
    cluster: c
    task_definition: td:1
    subnets: ["subnet-a"]
    security_groups: ["sg-1"]
  local:
    type: docker
    image: my-etl:dev
    network: loom_net
"""
    cfg = tmp_path / "etl.yaml"
    cfg.write_text(content, encoding="utf-8")
    return cfg


def test_generic_etl_flow_is_module_level_and_picklable() -> None:
    # Module-level flow must be picklable (no closure).
    pickle.dumps(_generic_etl_flow)


def test_deploy_etl_invokes_with_options_and_deploy(tmp_path: Path) -> None:
    cfg = _write_etl_yaml(tmp_path)

    deploy_mock = MagicMock(return_value="deployment-uuid-123")
    options_mock = MagicMock()
    options_mock.deploy = deploy_mock

    with patch.object(
        _generic_etl_flow, "with_options", return_value=options_mock
    ) as with_options_mock:
        result = deploy_etl(config_path=str(cfg))

    assert result == "deployment-uuid-123"
    with_options_mock.assert_called_once()
    # name kwarg must equal cfg.etl.
    kwargs = with_options_mock.call_args.kwargs
    assert kwargs.get("name") == "daily-orders"

    deploy_mock.assert_called_once()
    deploy_kwargs = deploy_mock.call_args.kwargs
    assert deploy_kwargs.get("name") == "daily-orders"
    assert deploy_kwargs.get("work_pool_name") == "default"

    parameters = deploy_kwargs["parameters"]
    # Absolute path baked into parameters.
    assert parameters["config_path"] == str(Path(cfg).resolve())
    assert parameters["env"] == "prod"
    # Placeholder strings preserved (resolved at flow run, not deploy).
    assert parameters["business_date"] == "${today-1d}"
    assert list(parameters["countries"]) == ["ES", "FR"]
    # Schedule present when enabled. Prefect 3 prefers the plural ``schedules``
    # kwarg (a list of schedule objects); the singular form is deprecated.
    schedules = deploy_kwargs.get("schedules")
    assert schedules is not None
    assert len(schedules) == 1


def test_deploy_etl_omits_schedule_when_disabled(tmp_path: Path) -> None:
    cfg = _write_etl_yaml(tmp_path, with_schedule=False)

    deploy_mock = MagicMock(return_value="d-1")
    options_mock = MagicMock()
    options_mock.deploy = deploy_mock

    with patch.object(_generic_etl_flow, "with_options", return_value=options_mock):
        deploy_etl(config_path=str(cfg))

    deploy_kwargs = deploy_mock.call_args.kwargs
    # When schedule is disabled, the deploy call must not carry either kwarg.
    assert "schedules" not in deploy_kwargs
    assert "schedule" not in deploy_kwargs


class _FakeParams(msgspec.Struct, frozen=True, kw_only=True):
    """Real msgspec Struct used in place of params_type for unit tests."""

    business_date: date
    countries: list[str]


def test_generic_etl_flow_passes_flow_name_to_run_etl_in_container(
    tmp_path: Path,
) -> None:
    """The inner build_etl_flow must share the outer's name. To make this
    possible, ``_generic_etl_flow`` MUST forward ``flow_name=cfg.etl`` to
    ``run_etl_in_container`` so the container's entrypoint can read
    ``LOOM_FLOW_NAME`` and name its inner flow accordingly.
    """
    cfg = _write_etl_yaml(tmp_path)

    from loom.prefect import _deploy as deploy_mod

    fake_launcher = MagicMock()

    with (
        patch.object(deploy_mod, "build_launcher", return_value=fake_launcher),
        patch.object(deploy_mod, "_import_obj", return_value=_FakeParams),
        patch.object(deploy_mod, "run_etl_in_container", return_value=0) as run_mock,
    ):
        # Deployment runtime would pass cfg.params as **raw_params; mirror that here.
        deploy_mod._generic_etl_flow_impl(
            config_path=str(cfg),
            env="prod",
            business_date="${today-1d}",
            countries=["ES", "FR"],
        )

    run_mock.assert_called_once()
    kwargs = run_mock.call_args.kwargs
    assert kwargs["flow_name"] == "daily-orders"
    assert kwargs["launcher"] is fake_launcher


def test_compute_correlation_id_uuid_when_field_none() -> None:
    cfg = MagicMock()
    cfg.etl = "daily-orders"
    cfg.correlation_field = None
    corr = _compute_correlation_id(cfg, {"business_date": date(2026, 6, 2)})
    assert corr.startswith("daily-orders-")
    # uuid hex suffix is non-empty.
    suffix = corr[len("daily-orders-") :]
    assert len(suffix) > 0


def test_compute_correlation_id_from_date_field() -> None:
    cfg = MagicMock()
    cfg.etl = "daily-orders"
    cfg.correlation_field = "business_date"
    corr = _compute_correlation_id(cfg, {"business_date": date(2026, 6, 2)})
    assert corr == "daily-orders-2026-06-02"


def test_compute_correlation_id_from_str_field() -> None:
    cfg = MagicMock()
    cfg.etl = "daily-orders"
    cfg.correlation_field = "business_date"
    corr = _compute_correlation_id(cfg, {"business_date": "2026-06-02"})
    assert corr == "daily-orders-2026-06-02"


def test_compute_correlation_id_rejects_unsupported_types() -> None:
    cfg = MagicMock()
    cfg.etl = "x"
    cfg.correlation_field = "thing"
    with pytest.raises(TypeError):
        _compute_correlation_id(cfg, {"thing": ["a", "b"]})


def test_compute_correlation_id_absent_field_raises_key_error() -> None:
    cfg = MagicMock()
    cfg.etl = "x"
    cfg.correlation_field = "missing"
    with pytest.raises(KeyError):
        _compute_correlation_id(cfg, {"other": "v"})
