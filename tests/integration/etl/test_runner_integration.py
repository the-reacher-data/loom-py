from __future__ import annotations

import importlib
from datetime import date
from pathlib import Path

import pytest

pytest.importorskip("polars")
pytest.importorskip("deltalake")

import polars as pl
from deltalake import DeltaTable, write_deltalake

from loom.etl import (
    ETLParams,
    ETLPipeline,
    ETLProcess,
    ETLRunner,
    ETLStep,
    FromTable,
    IntoTable,
    SchemaMode,
)


def _table_path(root: Path, ref: str) -> Path:
    return root.joinpath(*ref.split("."))


def _seed_table(root: Path, ref: str, frame: pl.DataFrame) -> None:
    path = _table_path(root, ref)
    path.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(path), frame.to_arrow(), mode="overwrite")


def _read_table(root: Path, ref: str) -> pl.DataFrame:
    return pl.from_arrow(DeltaTable(str(_table_path(root, ref))).to_pyarrow_table())


def _fresh_runner_cls() -> type[ETLRunner]:
    """Reload runtime modules that can be stale after module-level reload tests."""
    import loom.etl.backends.polars as polars_pkg
    import loom.etl.backends.polars._predicate as polars_predicate
    import loom.etl.backends.polars._writer as polars_writer
    import loom.etl.runner.core as runner_core
    import loom.etl.sql._predicate_dialect as predicate_dialect
    import loom.etl.storage._factory as storage_factory

    importlib.reload(predicate_dialect)
    importlib.reload(polars_predicate)
    importlib.reload(polars_writer)
    importlib.reload(polars_pkg)
    importlib.reload(storage_factory)
    runner_core = importlib.reload(runner_core)
    return runner_core.ETLRunner


class RunParams(ETLParams):
    run_date: date


class FilterOrdersStep(ETLStep[RunParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.orders").replace(schema=SchemaMode.OVERWRITE)

    def execute(self, params: RunParams, *, orders: pl.LazyFrame) -> pl.LazyFrame:  # type: ignore[override]
        return orders.filter(pl.col("year") == params.run_date.year).select("id", "year", "amount")


class SummarizeOrdersStep(ETLStep[RunParams]):
    orders = FromTable("staging.orders")
    target = IntoTable("mart.daily_orders").replace(schema=SchemaMode.OVERWRITE)

    def execute(self, params: RunParams, *, orders: pl.LazyFrame) -> pl.LazyFrame:  # type: ignore[override]
        return orders.select(
            pl.len().alias("row_count"),
            pl.col("amount").sum().alias("total_amount"),
        )


class DailyProcess(ETLProcess[RunParams]):
    steps = [FilterOrdersStep, SummarizeOrdersStep]


class DailyPipeline(ETLPipeline[RunParams]):
    processes = [DailyProcess]


class CopyOrdersStep(ETLStep[RunParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.orders_only").replace(schema=SchemaMode.OVERWRITE)

    def execute(self, params: RunParams, *, orders: pl.LazyFrame) -> pl.LazyFrame:  # type: ignore[override]
        return orders


class CopyCustomersStep(ETLStep[RunParams]):
    customers = FromTable("raw.customers")
    target = IntoTable("staging.customers_only").replace(schema=SchemaMode.OVERWRITE)

    def execute(self, params: RunParams, *, customers: pl.LazyFrame) -> pl.LazyFrame:  # type: ignore[override]
        return customers


class IncludeProcess(ETLProcess[RunParams]):
    steps = [CopyOrdersStep, CopyCustomersStep]


class IncludePipeline(ETLPipeline[RunParams]):
    processes = [IncludeProcess]


def test_runner_from_dict_executes_end_to_end_pipeline(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    _seed_table(
        root,
        "raw.orders",
        pl.DataFrame(
            {
                "id": [1, 2, 3],
                "year": [2024, 2025, 2024],
                "amount": [10.0, 20.0, 5.5],
            }
        ),
    )

    runner = _fresh_runner_cls().from_dict(
        storage={"root": str(root)},
        observability={"log": False},
    )
    runner.run(DailyPipeline, RunParams(run_date=date(2024, 1, 5)))

    staging = _read_table(root, "staging.orders").sort("id")
    summary = _read_table(root, "mart.daily_orders")

    assert staging["id"].to_list() == [1, 3]
    assert staging["year"].to_list() == [2024, 2024]
    assert staging["amount"].to_list() == pytest.approx([10.0, 5.5])
    assert summary["row_count"].to_list() == [2]
    assert summary["total_amount"].to_list() == pytest.approx([15.5])


def test_runner_from_yaml_applies_include_filter(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    _seed_table(root, "raw.orders", pl.DataFrame({"id": [1, 2], "amount": [1.0, 2.0]}))
    _seed_table(root, "raw.customers", pl.DataFrame({"id": [7], "name": ["alice"]}))

    config_path = tmp_path / "etl.yaml"
    config_path.write_text(
        f"""
storage:
  type: delta
  root: {root}
observability:
  log: false
""".strip(),
        encoding="utf-8",
    )

    runner = _fresh_runner_cls().from_yaml(config_path)
    runner.run(IncludePipeline, RunParams(run_date=date(2024, 1, 5)), include=["CopyOrdersStep"])

    orders_target = _table_path(root, "staging.orders_only")
    customers_target = _table_path(root, "staging.customers_only")

    assert (orders_target / "_delta_log").exists()
    assert not (customers_target / "_delta_log").exists()
    assert _read_table(root, "staging.orders_only")["id"].to_list() == [1, 2]
