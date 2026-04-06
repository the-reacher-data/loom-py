"""Tests for ETLScenario/StepRunnerProto and in-memory stubs."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from loom.etl.io.source import TableSourceSpec
from loom.etl.io.target._table import ReplaceSpec
from loom.etl.observability.records import EventName, RunContext, RunStatus
from loom.etl.schema._schema import ColumnSchema, LoomDtype
from loom.etl.schema._table import TableRef
from loom.etl.testing._scenario import ETLScenario, StepRunnerProto
from loom.etl.testing._stubs import StubCatalog, StubRunObserver, StubSourceReader, StubTargetWriter


class _Runner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[tuple[Any, ...]], list[str]]] = []

    def seed(self, ref: str, data: list[tuple[Any, ...]], columns: list[str]) -> Any:
        self.calls.append((ref, data, columns))
        return None


def test_scenario_apply_seeds_all_tables_in_order_and_returns_runner() -> None:
    runner = _Runner()
    scenario = (
        ETLScenario()
        .with_table("raw.orders", [(1, 10.0)], ["id", "amount"])
        .with_table("raw.customers", [(1, "alice")], ["id", "name"])
    )

    out = scenario.apply(runner)

    assert out is runner
    assert runner.calls == [
        ("raw.orders", [(1, 10.0)], ["id", "amount"]),
        ("raw.customers", [(1, "alice")], ["id", "name"]),
    ]
    assert "raw.orders" in repr(scenario)
    assert isinstance(runner, StepRunnerProto)


def test_stub_catalog_prefers_explicit_schema_over_table_columns() -> None:
    schemas = {"raw.orders": (ColumnSchema("id", LoomDtype.INT64),)}
    catalog = StubCatalog(tables={"raw.orders": ("legacy",)}, schemas=schemas)

    assert catalog.exists(TableRef("raw.orders")) is True
    assert catalog.columns(TableRef("raw.orders")) == ("id",)
    assert catalog.schema(TableRef("raw.orders")) == schemas["raw.orders"]


def test_stub_reader_and_writer_capture_data_flow() -> None:
    frame = {"id": [1]}
    reader = StubSourceReader({"orders": frame})
    writer = StubTargetWriter()

    src_spec = TableSourceSpec(alias="orders", table_ref=TableRef("raw.orders"))
    target_spec = ReplaceSpec(table_ref=TableRef("staging.out"))

    assert reader.read(src_spec, _params_instance=None) is frame
    writer.write(frame, target_spec, _params_instance=None)
    assert writer.written == [(frame, target_spec)]


def test_stub_run_observer_records_full_lifecycle_and_status_helpers() -> None:
    observer = StubRunObserver()
    ctx = RunContext(
        run_id="run-1",
        correlation_id="corr-1",
        attempt=2,
        last_attempt=False,
    )

    step_plan = type("StepPlan", (), {"step_type": type("MyStep", (), {})})()
    observer.on_pipeline_start(type("Pipe", (), {})(), object(), ctx)
    observer.on_process_start(type("Proc", (), {})(), ctx, "proc-1")
    observer.on_step_start(step_plan, ctx, "step-1")
    observer.on_step_error("step-1", RuntimeError("boom"))
    observer.on_step_end("step-1", RunStatus.FAILED, 5)
    observer.on_process_end("proc-1", RunStatus.FAILED, 6)
    observer.on_pipeline_end(ctx, RunStatus.FAILED, 7)

    assert observer.event_names == [
        EventName.PIPELINE_START,
        EventName.PROCESS_START,
        EventName.STEP_START,
        EventName.STEP_ERROR,
        EventName.STEP_END,
        EventName.PROCESS_END,
        EventName.PIPELINE_END,
    ]
    assert observer.step_statuses == [RunStatus.FAILED]
    assert observer.pipeline_statuses == [RunStatus.FAILED]


def test_run_context_defaults_are_stable() -> None:
    ctx = RunContext(run_id="run-2")
    assert ctx.correlation_id is None
    assert ctx.attempt == 1
    assert ctx.last_attempt is True
    assert isinstance(datetime.now(tz=UTC), datetime)
