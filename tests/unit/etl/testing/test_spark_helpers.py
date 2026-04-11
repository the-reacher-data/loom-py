"""Tests for etl.testing.spark helper functions and runner internals."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from pathlib import Path
from typing import Any

import pytest

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target._table import ReplaceSpec
from loom.etl.testing import spark as spark_testing


def test_delta_spark_version_returns_none_when_package_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(_: str) -> str:
        raise PackageNotFoundError

    monkeypatch.setattr(spark_testing, "version", _raise)
    assert spark_testing._delta_spark_version() is None


def test_resolve_local_delta_jars_returns_none_when_home_has_no_ivy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(spark_testing.Path, "home", lambda: tmp_path)
    assert spark_testing._resolve_local_delta_jars() is None


def test_resolve_local_delta_jars_uses_expected_version_and_antlr_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    jar_dir = tmp_path / ".ivy2" / "jars"
    jar_dir.mkdir(parents=True)
    (jar_dir / "io.delta_delta-spark_2.12-3.2.0.jar").touch()
    (jar_dir / "io.delta_delta-storage-3.2.0.jar").touch()
    (jar_dir / "org.antlr_antlr4-runtime-4.13.1.jar").touch()

    monkeypatch.setattr(spark_testing.Path, "home", lambda: tmp_path)
    monkeypatch.setattr(spark_testing, "_delta_spark_version", lambda: "3.2.0")

    jars = spark_testing._resolve_local_delta_jars()
    assert jars is not None
    assert len(jars) == 3
    assert all(path.exists() for path in jars)


def test_spark_frame_to_polars_handles_empty_and_rows() -> None:
    class _Row:
        def __init__(self, data: dict[str, Any]) -> None:
            self._data = data

        def __getitem__(self, key: str) -> Any:
            return self._data[key]

    class _Frame:
        columns = ["id", "amount"]

        def collect(self) -> list[_Row]:
            return [_Row({"id": 1, "amount": 10.0}), _Row({"id": 2, "amount": 20.0})]

    df = spark_testing._spark_frame_to_polars(_Frame())
    assert df.to_dict(as_series=False) == {"id": [1, 2], "amount": [10.0, 20.0]}

    class _EmptyFrame:
        columns = ["id"]

        def collect(self) -> list[Any]:
            return []

    empty = spark_testing._spark_frame_to_polars(_EmptyFrame())
    assert empty.columns == ["id"]
    assert empty.is_empty()


def test_spark_stub_reader_and_writer_store_and_return_values() -> None:
    frame = object()
    reader = spark_testing._SparkStubReader({"raw.orders": frame})
    writer = spark_testing._SparkCapturingWriter()

    spec = type("Spec", (), {"table_ref": TableRef("raw.orders"), "alias": "orders"})()
    assert reader.read(spec, None) is frame

    target = ReplaceSpec(table_ref=TableRef("staging.out"))
    writer.write(frame, target, None)
    assert writer.frame is frame
    assert writer.spec is target


def test_spark_step_runner_target_spec_guard_and_run_path(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Spark:
        def _create_dataframe(self, data: list[tuple[Any, ...]], columns: list[str]) -> Any:
            class _Row:
                def __init__(self, d: dict[str, Any]) -> None:
                    self._d = d

                def __getitem__(self, key: str) -> Any:
                    return self._d[key]

            class _Frame:
                def __init__(self, records: list[dict[str, Any]]) -> None:
                    self.columns = list(records[0].keys()) if records else list(columns)
                    self._records = records

                def collect(self) -> list[Any]:
                    return [_Row(rec) for rec in self._records]

            records = [dict(zip(columns, row, strict=True)) for row in data]
            return _Frame(records)

        def __getattr__(self, name: str) -> Any:
            if name == "createDataFrame":
                return self._create_dataframe
            raise AttributeError(name)

    runner = spark_testing.SparkStepRunner(_Spark())
    with pytest.raises(RuntimeError, match="No spec"):
        _ = runner.target_spec

    runner.seed("raw.orders", [(1, 10.0)], ["id", "amount"])

    monkeypatch.setattr(spark_testing.ETLCompiler, "compile_step", lambda _self, _cls: object())

    def _fake_run_step(self: Any, _plan: Any, _params: Any) -> None:
        frame = _Spark()._create_dataframe([(1, 20.0)], ["id", "amount"])
        writer = self._writer
        writer.write(frame, ReplaceSpec(table_ref=TableRef("staging.out")), None)

    monkeypatch.setattr(spark_testing.ETLExecutor, "run_step", _fake_run_step)

    result = runner.run(type("DummyStep", (), {}), object())
    assert result.to_polars().shape == (1, 2)
    assert isinstance(runner.target_spec, ReplaceSpec)


def test_spark_step_runner_raises_when_step_produces_no_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Spark:
        def _create_dataframe(
            self,
            data: list[tuple[Any, ...]],
            columns: list[str],  # noqa: ARG002
        ) -> Any:
            return object()

        def __getattr__(self, name: str) -> Any:
            if name == "createDataFrame":
                return self._create_dataframe
            raise AttributeError(name)

    runner = spark_testing.SparkStepRunner(_Spark()).seed("raw.orders", [(1,)], ["id"])
    monkeypatch.setattr(spark_testing.ETLCompiler, "compile_step", lambda _self, _cls: object())
    monkeypatch.setattr(spark_testing.ETLExecutor, "run_step", lambda _self, _plan, _params: None)

    with pytest.raises(RuntimeError, match="Step produced no output"):
        runner.run(type("DummyStep", (), {}), object())
