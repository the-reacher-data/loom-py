"""Tests for loom.prefect._etl_flow (factory + discovery)."""

from __future__ import annotations

import textwrap
from datetime import datetime
from pathlib import Path

import pytest

from loom.etl import ETLParams, ETLPipeline, ETLProcess, ETLStep, FromTable, IntoTable
from loom.prefect._etl_flow import _LOOM_ETL_META_ATTR, etl_flow


class _SampleParams(ETLParams, frozen=True):  # type: ignore[misc]
    updated_at_from: datetime
    updated_at_to: datetime


class _SampleStep(ETLStep[_SampleParams]):
    src = FromTable("raw.events")
    target = IntoTable("staging.events").replace()

    def execute(self, params: _SampleParams, *, src):  # type: ignore[override]
        return src


class _SampleProcess(ETLProcess[_SampleParams]):
    steps = [_SampleStep]


class _SamplePipeline(ETLPipeline[_SampleParams]):
    processes = [_SampleProcess]


def _write_cfg(tmp_path: Path) -> Path:
    content = textwrap.dedent(
        """\
        etl: sample-etl
        correlation_field: updated_at_from
        schedule:
          cron: "0 6 * * *"
          timezone: Europe/Madrid
          enabled: true
        params:
          updated_at_from: ${now-1d}
          updated_at_to: ${now}
        environments:
          prod:
            work_pool: loom-fargate
            job_variables:
              task_definition_arn: arn:aws:ecs:::task-definition/sample:1
              cpu: "1024"
          local:
            work_pool: loom-docker
            job_variables:
              image: sample:dev
        """
    )
    yaml_path = tmp_path / "etl.yaml"
    yaml_path.write_text(content, encoding="utf-8")
    return yaml_path


def test_etl_flow_name_matches_argument(tmp_path: Path) -> None:
    flow_obj = etl_flow(
        name="sample-etl",
        pipeline=_SamplePipeline,
        params_type=_SampleParams,
        config_path=str(_write_cfg(tmp_path)),
        source_file=__file__,
    )
    assert flow_obj.name == "sample-etl"


def test_etl_flow_synthesizes_typed_signature(tmp_path: Path) -> None:
    flow_obj = etl_flow(
        name="sample-etl",
        pipeline=_SamplePipeline,
        params_type=_SampleParams,
        config_path=str(_write_cfg(tmp_path)),
        source_file=__file__,
    )
    sig = flow_obj.fn.__signature__
    parameters = sig.parameters
    assert list(parameters.keys()) == [
        "updated_at_from",
        "updated_at_to",
        "env",
        "correlation_id",
    ]
    assert parameters["updated_at_from"].annotation is datetime
    assert parameters["env"].default == "prod"
    assert parameters["correlation_id"].default is None


def test_etl_flow_attaches_discovery_metadata(tmp_path: Path) -> None:
    cfg = _write_cfg(tmp_path)
    flow_obj = etl_flow(
        name="sample-etl",
        pipeline=_SamplePipeline,
        params_type=_SampleParams,
        config_path=str(cfg),
        source_file=__file__,
    )
    meta = getattr(flow_obj, _LOOM_ETL_META_ATTR)
    assert meta.name == "sample-etl"
    assert meta.config_path == str(cfg.resolve())
    assert meta.source_file == str(Path(__file__).resolve())
    assert meta.correlation_field == "updated_at_from"
    assert meta.schedule == {
        "cron": "0 6 * * *",
        "timezone": "Europe/Madrid",
        "enabled": True,
    }
    assert meta.raw_params == {
        "updated_at_from": "${now-1d}",
        "updated_at_to": "${now}",
    }
    assert meta.pool_config["prod"]["work_pool"] == "loom-fargate"
    assert meta.pool_config["prod"]["job_variables"]["cpu"] == "1024"
    assert meta.pool_config["local"]["work_pool"] == "loom-docker"
    assert meta.pool_config["local"]["job_variables"]["image"] == "sample:dev"


def test_etl_flow_rejects_non_struct_params_type(tmp_path: Path) -> None:
    class NotAStruct:
        pass

    with pytest.raises((TypeError, ValueError)):
        etl_flow(
            name="bad-etl",
            pipeline=_SamplePipeline,
            params_type=NotAStruct,  # type: ignore[arg-type]
            config_path=str(_write_cfg(tmp_path)),
            source_file=__file__,
        )
