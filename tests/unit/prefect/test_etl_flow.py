"""Tests for the per-ETL Prefect flow factory."""

from __future__ import annotations

import textwrap
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from loom.etl import ETLParams, ETLPipeline, ETLProcess, ETLStep, FromTable, IntoTable
from loom.prefect import etl_flow
from loom.prefect._meta import LOOM_ETL_META_ATTR as _LOOM_ETL_META_ATTR
from loom.prefect._placeholders import resolve_placeholder
from loom.prefect.flow._signature import (
    normalize_datetime_fields as _normalize_datetime_fields,
)


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
        "processes",
    ]
    assert parameters["updated_at_from"].annotation is datetime
    assert parameters["env"].default == "prod"
    assert parameters["correlation_id"].default is None
    assert parameters["processes"].default is None
    assert parameters["processes"].annotation == list[str] | None


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


def test_etl_flow_captures_tags_when_present(tmp_path: Path) -> None:
    content = textwrap.dedent(
        """\
        etl: sample-etl
        tags:
          - mongo
          - daily
          - team:data-platform
        params: {}
        """
    )
    yaml_path = tmp_path / "tagged.yaml"
    yaml_path.write_text(content, encoding="utf-8")
    flow_obj = etl_flow(
        name="tagged-etl",
        pipeline=_SamplePipeline,
        params_type=_SampleParams,
        config_path=str(yaml_path),
        source_file=__file__,
    )
    meta = getattr(flow_obj, _LOOM_ETL_META_ATTR)
    assert meta.tags == ("mongo", "daily", "team:data-platform")


def test_etl_flow_tags_default_to_empty_tuple(tmp_path: Path) -> None:
    flow_obj = etl_flow(
        name="sample-etl",
        pipeline=_SamplePipeline,
        params_type=_SampleParams,
        config_path=str(_write_cfg(tmp_path)),
        source_file=__file__,
    )
    meta = getattr(flow_obj, _LOOM_ETL_META_ATTR)
    assert meta.tags == ()


def test_deploy_passes_meta_name_and_yaml_tags_no_loom_etl(tmp_path: Path) -> None:
    from unittest.mock import MagicMock

    from loom.prefect.deploy._discovery import _deploy_single

    content = textwrap.dedent(
        """\
        etl: sample-etl
        tags:
          - mongo
          - daily
        params: {}
        environments:
          prod:
            work_pool: loom-fargate
            job_variables:
              image: sample:dev
              cpu: "1024"
        """
    )
    yaml_path = tmp_path / "deploy_tags.yaml"
    yaml_path.write_text(content, encoding="utf-8")
    flow_obj = etl_flow(
        name="deploy-tags-etl",
        pipeline=_SamplePipeline,
        params_type=_SampleParams,
        config_path=str(yaml_path),
        source_file=__file__,
    )
    meta = getattr(flow_obj, _LOOM_ETL_META_ATTR)

    sourced = MagicMock()
    sourced.deploy = MagicMock(return_value="deployment-id-abc")
    flow_obj.from_source = MagicMock(return_value=sourced)

    deployment_id = _deploy_single(flow_obj, meta, work_pool="default-pool", env="prod")

    assert deployment_id == "deployment-id-abc"
    deploy_kwargs = sourced.deploy.call_args.kwargs
    assert deploy_kwargs["tags"] == ["deploy-tags-etl", "mongo", "daily"]
    assert "loom-etl" not in deploy_kwargs["tags"]


def test_prometheus_adapter_wired_when_env_set(monkeypatch: pytest.MonkeyPatch) -> None:
    from loom.prefect.flow._body import _build_observers

    monkeypatch.setenv("PROMETHEUS_PUSHGATEWAY_URL", "https://pushgateway:9091")
    observers = _build_observers(flow_run_id=None, manifest_store=None, manifest=None)
    assert any(type(o).__name__ == "PrometheusLifecycleAdapter" for o in observers)


def test_prometheus_adapter_absent_when_env_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    from loom.prefect.flow._body import _build_observers

    monkeypatch.delenv("PROMETHEUS_PUSHGATEWAY_URL", raising=False)
    observers = _build_observers(flow_run_id=None, manifest_store=None, manifest=None)
    assert not any(type(o).__name__ == "PrometheusLifecycleAdapter" for o in observers)


def test_etl_flow_rejects_non_string_tags(tmp_path: Path) -> None:
    content = textwrap.dedent(
        """\
        etl: sample-etl
        tags:
          - mongo
          - 42
        params: {}
        """
    )
    yaml_path = tmp_path / "bad_tags.yaml"
    yaml_path.write_text(content, encoding="utf-8")
    with pytest.raises(TypeError, match="tags"):
        etl_flow(
            name="sample-etl",
            pipeline=_SamplePipeline,
            params_type=_SampleParams,
            config_path=str(yaml_path),
            source_file=__file__,
        )


class _OptionalDtParams(ETLParams, frozen=True):  # type: ignore[misc]
    updated_at_from: datetime | None = None
    label: str = "x"
    count: int = 0


def test_naive_datetime_string_is_normalized_to_utc() -> None:
    out = _normalize_datetime_fields(
        {"updated_at_from": "2026-06-03T00:00:00", "updated_at_to": "2026-06-04T00:00:00"},
        _SampleParams,
    )
    assert isinstance(out["updated_at_from"], datetime)
    assert out["updated_at_from"].tzinfo is UTC
    assert out["updated_at_to"].tzinfo is UTC


def test_naive_datetime_object_is_normalized_to_utc() -> None:
    naive = datetime(2026, 6, 3, 0, 0, 0)
    out = _normalize_datetime_fields(
        {"updated_at_from": naive, "updated_at_to": naive},
        _SampleParams,
    )
    assert out["updated_at_from"].tzinfo is UTC
    assert out["updated_at_from"].replace(tzinfo=None) == naive


def test_aware_datetime_passes_through() -> None:
    aware = datetime(2026, 6, 3, 0, 0, 0, tzinfo=UTC)
    out = _normalize_datetime_fields(
        {"updated_at_from": aware, "updated_at_to": aware},
        _SampleParams,
    )
    assert out["updated_at_from"] is aware


def test_aware_datetime_with_offset_passes_through() -> None:
    offset = timezone(timedelta(hours=2))
    aware = datetime(2026, 6, 3, 0, 0, 0, tzinfo=offset)
    out = _normalize_datetime_fields(
        {"updated_at_from": aware, "updated_at_to": aware},
        _SampleParams,
    )
    assert out["updated_at_from"].tzinfo is offset
    assert out["updated_at_from"].utcoffset() == timedelta(hours=2)


def test_non_datetime_fields_untouched() -> None:
    out = _normalize_datetime_fields(
        {"updated_at_from": None, "label": "hello", "count": 42},
        _OptionalDtParams,
    )
    assert out["label"] == "hello"
    assert out["count"] == 42


def test_placeholder_now_produces_aware_datetime() -> None:
    resolved = {
        "updated_at_from": resolve_placeholder("${now}"),
        "updated_at_to": resolve_placeholder("${now}"),
    }
    out = _normalize_datetime_fields(resolved, _SampleParams)
    assert out["updated_at_from"].tzinfo is UTC
    # Idempotent: the aware datetime is unchanged.
    assert out["updated_at_from"] is resolved["updated_at_from"]


def test_optional_datetime_none_value() -> None:
    out = _normalize_datetime_fields(
        {"updated_at_from": None},
        _OptionalDtParams,
    )
    assert out["updated_at_from"] is None


class TestValidateProcesses:
    @pytest.fixture
    def known(self) -> frozenset[str]:
        from loom.etl.compiler import ETLCompiler
        from loom.prefect.flow._body import _known_process_names

        return _known_process_names(ETLCompiler().compile(_SamplePipeline))

    def test_accepts_known_name(self, known: frozenset[str]) -> None:
        from loom.prefect.flow._body import _validate_processes

        assert _validate_processes(["_SampleProcess"], known) == ("_SampleProcess",)

    def test_none_passes_through(self, known: frozenset[str]) -> None:
        from loom.prefect.flow._body import _validate_processes

        assert _validate_processes(None, known) is None

    def test_empty_list_normalises_to_none(self, known: frozenset[str]) -> None:
        from loom.prefect.flow._body import _validate_processes

        assert _validate_processes([], known) is None

    def test_rejects_unknown_name(self, known: frozenset[str]) -> None:
        from loom.prefect.flow._body import _validate_processes

        with pytest.raises(ValueError, match="unknown"):
            _validate_processes(["DoesNotExist"], known)

    def test_rejects_non_list_value(self, known: frozenset[str]) -> None:
        from loom.prefect.flow._body import _validate_processes

        with pytest.raises(TypeError, match="processes"):
            _validate_processes("not-a-list", known)

    def test_rejects_non_string_entries(self, known: frozenset[str]) -> None:
        from loom.prefect.flow._body import _validate_processes

        with pytest.raises(TypeError, match="processes"):
            _validate_processes([1, 2], known)


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
