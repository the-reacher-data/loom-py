"""Internal observer behavior tests (composite/sink/structlog/delta sink)."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from loom.etl.observability.observers.composite import CompositeObserver
from loom.etl.observability.observers.execution_records import ExecutionRecordsObserver
from loom.etl.observability.observers.structlog import (
    StructlogRunObserver,
    _source_label,
    _target_label,
)
from loom.etl.observability.records import (
    EventName,
    PipelineRunRecord,
    ProcessRunRecord,
    RunContext,
    RunStatus,
    StepRunRecord,
)
from loom.etl.observability.stores.table import TableExecutionRecordStore
from loom.etl.schema._table import TableRef


class _CaptureObserver:
    def __init__(self) -> None:
        self.step_end_calls = 0

    def on_pipeline_start(self, _plan: object, _params: object, _ctx: RunContext) -> None:
        return None

    def on_pipeline_end(self, _ctx: RunContext, _status: RunStatus, _duration_ms: int) -> None:
        return None

    def on_process_start(self, _plan: object, _ctx: RunContext, _process_run_id: str) -> None:
        return None

    def on_process_end(self, _process_run_id: str, _status: RunStatus, _duration_ms: int) -> None:
        return None

    def on_step_start(self, _plan: object, _ctx: RunContext, _step_run_id: str) -> None:
        return None

    def on_step_end(self, _step_run_id: str, _status: RunStatus, _duration_ms: int) -> None:
        self.step_end_calls += 1

    def on_step_error(self, _step_run_id: str, _exc: Exception) -> None:
        return None


class _FailingObserver(_CaptureObserver):
    def on_step_end(self, _step_run_id: str, _status: RunStatus, _duration_ms: int) -> None:
        raise RuntimeError("observer boom")


class _Sink:
    def __init__(self) -> None:
        self.records: list[object] = []

    def write_record(self, record: object) -> None:
        self.records.append(record)


class _CaptureAppendWriter:
    def __init__(self) -> None:
        self.calls: list[tuple[object, TableRef]] = []

    def write_record(self, record: object, table_ref: TableRef, /) -> None:
        self.calls.append((record, table_ref))


def _ctx() -> RunContext:
    return RunContext(run_id="run-1", correlation_id="corr-1", attempt=3, last_attempt=False)


def test_composite_observer_isolates_failures_and_calls_remaining_observers() -> None:
    capture = _CaptureObserver()
    composite = CompositeObserver([_FailingObserver(), capture])

    with patch("loom.etl.observability.observers.composite._log") as log:
        composite.on_step_end("step-1", RunStatus.SUCCESS, 10)

    assert capture.step_end_calls == 1
    log.error.assert_called_once()


@pytest.mark.parametrize(
    "spec,expected",
    [
        (
            type(
                "Spec",
                (),
                {
                    "table_ref": type("T", (), {"ref": "raw.orders"})(),
                    "temp_name": None,
                    "path": None,
                },
            )(),
            "raw.orders",
        ),
        (type("Spec", (), {"table_ref": None, "temp_name": "norm", "path": None})(), "temp:norm"),
        (
            type("Spec", (), {"table_ref": None, "temp_name": None, "path": "s3://bucket/file"})(),
            "s3://bucket/file",
        ),
    ],
)
def test_structlog_source_and_target_labels(spec: object, expected: str) -> None:
    assert _source_label(spec) == expected
    assert _target_label(spec) == expected


def test_structlog_observer_emits_slow_step_warning_when_threshold_crossed() -> None:
    observer = StructlogRunObserver(slow_step_threshold_ms=100)

    with patch("loom.etl.observability.observers.structlog._log") as log:
        observer.on_step_end("step-1", RunStatus.SUCCESS, 150)

    log.info.assert_called_once_with(
        EventName.STEP_END,
        step_run_id="step-1",
        status=RunStatus.SUCCESS,
        duration_ms=150,
    )
    log.warning.assert_called_once_with(
        "slow_step",
        step_run_id="step-1",
        duration_ms=150,
        threshold_ms=100,
    )


def test_execution_records_observer_writes_process_and_pipeline_records_with_ctx() -> None:
    sink = _Sink()
    observer = ExecutionRecordsObserver(sink)

    process_plan = type("ProcPlan", (), {"process_type": type("MyProcess", (), {})})()
    pipeline_plan = type("PipePlan", (), {"pipeline_type": type("MyPipeline", (), {})})()

    ctx = _ctx()
    observer.on_pipeline_start(pipeline_plan, object(), ctx)
    observer.on_process_start(process_plan, ctx, "proc-1")
    observer.on_process_end("proc-1", RunStatus.SUCCESS, 25)
    observer.on_pipeline_end(ctx, RunStatus.SUCCESS, 80)

    assert len(sink.records) == 2
    process_record = sink.records[0]
    pipeline_record = sink.records[1]

    assert isinstance(process_record, ProcessRunRecord)
    assert process_record.process == "MyProcess"
    assert process_record.attempt == 3
    assert process_record.correlation_id == "corr-1"

    assert isinstance(pipeline_record, PipelineRunRecord)
    assert pipeline_record.pipeline == "MyPipeline"
    assert pipeline_record.attempt == 3
    assert pipeline_record.correlation_id == "corr-1"


def test_table_execution_record_store_writes_to_expected_table_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = monkeypatch
    writer = _CaptureAppendWriter()
    sink = TableExecutionRecordStore(writer)

    record = StepRunRecord(
        event=EventName.STEP_END,
        run_id="run-1",
        correlation_id=None,
        attempt=1,
        step_run_id="step-1",
        step="MyStep",
        started_at=datetime.now(tz=UTC),
        status=RunStatus.SUCCESS,
        duration_ms=12,
        error=None,
    )
    sink.write_record(record)

    assert len(writer.calls) == 1
    saved_record, table_ref = writer.calls[0]
    assert saved_record is record
    assert table_ref == TableRef("step_runs")


def test_table_execution_record_store_rejects_unknown_record_type() -> None:
    sink = TableExecutionRecordStore(_CaptureAppendWriter())
    with pytest.raises(TypeError, match="unrecognised record type"):
        sink.write_record(object())  # type: ignore[arg-type]


def test_table_execution_record_store_applies_database_prefix() -> None:
    writer = _CaptureAppendWriter()
    sink = TableExecutionRecordStore(writer, database="ops")
    record = PipelineRunRecord(
        event=EventName.PIPELINE_END,
        run_id="run-1",
        correlation_id=None,
        attempt=1,
        pipeline="P",
        started_at=datetime.now(tz=UTC),
        status=RunStatus.SUCCESS,
        duration_ms=1,
        error=None,
    )
    sink.write_record(record)

    assert writer.calls[0][1] == TableRef("ops.pipeline_runs")
