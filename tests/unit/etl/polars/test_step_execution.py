"""Integration tests for ETLExecutor.run_step with real Polars + Delta I/O.

All tests use temporary Delta directories created by the ``delta_root``
fixture and cleaned up automatically by pytest's ``tmp_path`` teardown.

Requires ``polars>=1.0`` and ``deltalake>=0.25``.  The module is skipped
automatically when either package is absent (enforced by ``conftest.py``).
"""

from __future__ import annotations

import polars as pl
import pytest
from deltalake import DeltaTable

from loom.etl import ETLParams, ETLStep, FromTable, IntoTable
from loom.etl._schema import LoomDtype
from loom.etl._table import TableRef
from loom.etl.compiler import ETLCompiler
from loom.etl.executor import ETLExecutor, EventName, RunStatus
from loom.etl.testing import StubRunObserver

from .conftest import MinimalPolarsDeltaReader, MinimalPolarsDeltaWriter, table_path


def _read_delta(root, ref: str) -> pl.DataFrame:
    """Read a Delta table via PyArrow to avoid polars ↔ deltalake schema skew."""
    path = table_path(root, TableRef(ref))
    return pl.from_arrow(DeltaTable(str(path)).to_pyarrow_table())


# ---------------------------------------------------------------------------
# Step definitions (module-level so the compiler sees real class objects)
# ---------------------------------------------------------------------------


class NoParams(ETLParams):
    pass


class DoubleAmountStep(ETLStep[NoParams]):
    """Reads raw.orders and writes staging.orders with doubled amounts."""

    orders: FromTable = FromTable("raw.orders")  # type: ignore[assignment]
    target = IntoTable("staging.orders").replace()

    def execute(self, params: NoParams, *, orders: pl.LazyFrame) -> pl.LazyFrame:  # type: ignore[override]
        return orders.with_columns((pl.col("amount") * 2).alias("amount"))


class PassThroughStep(ETLStep[NoParams]):
    """Copies raw.events to staging.events unchanged."""

    events: FromTable = FromTable("raw.events")  # type: ignore[assignment]
    target = IntoTable("staging.events").replace()

    def execute(self, params: NoParams, *, events: pl.LazyFrame) -> pl.LazyFrame:  # type: ignore[override]
        return events


class AppendStep(ETLStep[NoParams]):
    """Appends raw.deltas into staging.ledger."""

    deltas: FromTable = FromTable("raw.deltas")  # type: ignore[assignment]
    target = IntoTable("staging.ledger").append()

    def execute(self, params: NoParams, *, deltas: pl.LazyFrame) -> pl.LazyFrame:  # type: ignore[override]
        return deltas


# ---------------------------------------------------------------------------
# Happy-path tests
# ---------------------------------------------------------------------------


def test_run_step_writes_transformed_data(
    seed_table,
    polars_reader: MinimalPolarsDeltaReader,
    polars_writer: MinimalPolarsDeltaWriter,
    delta_root,
) -> None:
    """execute() result lands in the target Delta table."""
    seed_table("raw.orders", pl.DataFrame({"id": [1, 2, 3], "amount": [10.0, 20.0, 30.0]}))

    plan = ETLCompiler().compile_step(DoubleAmountStep)
    ETLExecutor(polars_reader, polars_writer).run_step(plan, NoParams())

    result = _read_delta(delta_root, "staging.orders")
    assert result["amount"].to_list() == pytest.approx([20.0, 40.0, 60.0])
    assert result["id"].to_list() == [1, 2, 3]


def test_run_step_row_count_preserved(
    seed_table,
    polars_reader: MinimalPolarsDeltaReader,
    polars_writer: MinimalPolarsDeltaWriter,
    delta_root,
) -> None:
    """Target table has the same number of rows as the source."""
    seed_table("raw.events", pl.DataFrame({"ts": [1, 2, 3, 4, 5], "val": [0] * 5}))

    plan = ETLCompiler().compile_step(PassThroughStep)
    ETLExecutor(polars_reader, polars_writer).run_step(plan, NoParams())

    result = _read_delta(delta_root, "staging.events")
    assert len(result) == 5


def test_run_step_replace_overwrites_existing_target(
    seed_table,
    polars_reader: MinimalPolarsDeltaReader,
    polars_writer: MinimalPolarsDeltaWriter,
    delta_root,
) -> None:
    """REPLACE mode overwrites any prior content in the target table."""
    seed_table("raw.orders", pl.DataFrame({"id": [1], "amount": [5.0]}))
    # Pre-populate the target with stale data
    seed_table("staging.orders", pl.DataFrame({"id": [99], "amount": [999.0]}))

    plan = ETLCompiler().compile_step(DoubleAmountStep)
    ETLExecutor(polars_reader, polars_writer).run_step(plan, NoParams())

    result = _read_delta(delta_root, "staging.orders")
    assert result["id"].to_list() == [1]
    assert result["amount"].to_list() == pytest.approx([10.0])


def test_run_step_append_adds_rows(
    seed_table,
    polars_reader: MinimalPolarsDeltaReader,
    polars_writer: MinimalPolarsDeltaWriter,
    delta_root,
) -> None:
    """APPEND mode grows the target table on each write."""
    seed_table("raw.deltas", pl.DataFrame({"id": [1, 2], "v": [10, 20]}))
    # Pre-populate the ledger with existing rows
    seed_table("staging.ledger", pl.DataFrame({"id": [0], "v": [0]}))

    plan = ETLCompiler().compile_step(AppendStep)
    ETLExecutor(polars_reader, polars_writer).run_step(plan, NoParams())

    result = _read_delta(delta_root, "staging.ledger")
    assert len(result) == 3  # 1 existing + 2 appended


# ---------------------------------------------------------------------------
# Observer lifecycle tests
# ---------------------------------------------------------------------------


def test_run_step_emits_start_and_end_events(
    seed_table,
    polars_reader: MinimalPolarsDeltaReader,
    polars_writer: MinimalPolarsDeltaWriter,
) -> None:
    """Observer receives step_start then step_end(success) on a clean run."""
    seed_table("raw.orders", pl.DataFrame({"id": [1], "amount": [1.0]}))
    observer = StubRunObserver()

    plan = ETLCompiler().compile_step(DoubleAmountStep)
    ETLExecutor(polars_reader, polars_writer, observers=[observer]).run_step(plan, NoParams())

    assert observer.event_names == [EventName.STEP_START, EventName.STEP_END]
    assert observer.step_statuses == ["success"]


def test_run_step_emits_error_event_on_failure(
    delta_root,
    polars_writer: MinimalPolarsDeltaWriter,
) -> None:
    """Observer receives step_error + step_end(failed) when a step raises."""

    # Reader that always raises
    class FailingReader:
        def read(self, spec, params_instance):  # type: ignore[override]
            raise RuntimeError("read failure")

    observer = StubRunObserver()
    plan = ETLCompiler().compile_step(DoubleAmountStep)

    with pytest.raises(RuntimeError, match="read failure"):
        ETLExecutor(FailingReader(), polars_writer, observers=[observer]).run_step(plan, NoParams())

    assert EventName.STEP_ERROR in observer.event_names
    assert observer.step_statuses == [RunStatus.FAILED]


# ---------------------------------------------------------------------------
# Schema derivation test
# ---------------------------------------------------------------------------


def test_seed_table_registers_schema_in_catalog(
    seed_table,
    delta_catalog,
) -> None:
    """seed_table() registers the correct LoomDtype schema in the catalog."""
    seed_table("raw.orders", pl.DataFrame({"id": [1], "amount": [1.0]}))
    schema = delta_catalog.schema(TableRef("raw.orders"))

    assert schema is not None
    assert schema[0].name == "id"
    assert schema[0].dtype is LoomDtype.INT64
    assert schema[1].name == "amount"
    assert schema[1].dtype is LoomDtype.FLOAT64
