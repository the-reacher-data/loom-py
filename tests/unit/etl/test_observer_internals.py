"""Internal observer behavior tests (composite/sink/structlog/delta sink)."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.observability.observers._labels import source_label, target_label
from loom.etl.observability.observers.composite import CompositeObserver
from loom.etl.observability.observers.structlog import StructlogRunObserver
from loom.etl.observability.recording import ExecutionRecordsObserver
from loom.etl.observability.records import (
    EventName,
    PipelineRunRecord,
    ProcessRunRecord,
    RunContext,
    RunStatus,
    StepRunRecord,
)
from loom.etl.observability.sinks import TableExecutionRecordStore, TargetExecutionRecordWriter


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


class _CaptureTargetWriter:
    def __init__(self) -> None:
        self.to_frame_calls: list[list[object]] = []
        self.append_calls: list[tuple[object, TableRef, object]] = []

    def to_frame(self, records: Sequence[object], /) -> object:
        self.to_frame_calls.append(list(records))
        return {"rows": records}

    def append(
        self,
        frame: object,
        table_ref: TableRef,
        params_instance: object,
        /,
        *,
        streaming: bool = False,
    ) -> None:
        _ = streaming
        self.append_calls.append((frame, table_ref, params_instance))


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
    assert source_label(spec) == expected
    assert target_label(spec) == expected


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
    saved_record, table_ref = next(iter(writer.calls))
    assert saved_record is record
    assert table_ref == TableRef("step_runs")


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

    assert len(writer.calls) == 1
    _, table_ref = next(iter(writer.calls))
    assert table_ref == TableRef("ops.pipeline_runs")


# ---------------------------------------------------------------------------
# TargetExecutionRecordWriter
# ---------------------------------------------------------------------------


def test_target_execution_record_writer_uses_to_frame_then_append() -> None:
    target_writer = _CaptureTargetWriter()
    writer = TargetExecutionRecordWriter(target_writer)
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

    writer.write_record(record, TableRef("step_runs"))

    assert len(target_writer.to_frame_calls) == 1
    frame_records = next(iter(target_writer.to_frame_calls))
    assert len(frame_records) == 1
    assert frame_records[0] is record
    assert len(target_writer.append_calls) == 1
    _, table_ref, params_instance = next(iter(target_writer.append_calls))
    assert table_ref == TableRef("step_runs")
    assert params_instance is None


# ---------------------------------------------------------------------------
# OtelRunObserver
# ---------------------------------------------------------------------------


def _make_span_mock() -> object:
    from unittest.mock import MagicMock

    return MagicMock()


def _make_tracer_mock(span: object) -> object:
    from unittest.mock import MagicMock

    tracer = MagicMock()
    tracer.start_span.return_value = span
    return tracer


def _pipeline_plan() -> object:
    return type("PipePlan", (), {"pipeline_type": type("MyPipeline", (), {})})()


def _process_plan() -> object:
    return type("ProcPlan", (), {"process_type": type("MyProcess", (), {})})()


def _step_plan() -> object:
    target_spec = type(
        "Spec",
        (),
        {"table_ref": type("T", (), {"ref": "staging.orders"})(), "temp_name": None, "path": None},
    )()
    target_binding = type("TB", (), {"spec": target_spec})()
    return type(
        "StepPlan",
        (),
        {"step_type": type("MyStep", (), {}), "target_binding": target_binding},
    )()


def test_otel_observer_starts_and_ends_pipeline_span() -> None:
    from unittest.mock import patch

    from opentelemetry.trace import StatusCode

    from loom.etl.observability.observers.otel import OtelRunObserver

    span = _make_span_mock()
    tracer = _make_tracer_mock(span)

    with patch("loom.etl.observability.observers.otel._tracer", tracer):
        observer = OtelRunObserver()
        ctx = RunContext(run_id="run-1", attempt=2)
        observer.on_pipeline_start(_pipeline_plan(), None, ctx)
        observer.on_pipeline_end(ctx, RunStatus.SUCCESS, 500)

    tracer.start_span.assert_called_once()
    call_name = tracer.start_span.call_args[0][0]
    assert call_name == "loom.etl.pipeline"
    span.set_status.assert_called_once_with(StatusCode.OK)
    span.end.assert_called_once()


def test_otel_observer_sets_error_status_on_pipeline_failure() -> None:
    from unittest.mock import patch

    from opentelemetry.trace import StatusCode

    from loom.etl.observability.observers.otel import OtelRunObserver

    span = _make_span_mock()
    tracer = _make_tracer_mock(span)

    with patch("loom.etl.observability.observers.otel._tracer", tracer):
        observer = OtelRunObserver()
        ctx = RunContext(run_id="run-1")
        observer.on_pipeline_start(_pipeline_plan(), None, ctx)
        observer.on_pipeline_end(ctx, RunStatus.FAILED, 200)

    span.set_status.assert_called_once_with(StatusCode.ERROR)


def test_otel_observer_starts_process_span_as_pipeline_child() -> None:
    from unittest.mock import MagicMock, patch

    from loom.etl.observability.observers.otel import OtelRunObserver

    pipeline_span = MagicMock()
    process_span = MagicMock()
    tracer = MagicMock()
    tracer.start_span.side_effect = [pipeline_span, process_span]

    with patch("loom.etl.observability.observers.otel._tracer", tracer):
        observer = OtelRunObserver()
        ctx = RunContext(run_id="run-1")
        observer.on_pipeline_start(_pipeline_plan(), None, ctx)
        observer.on_process_start(_process_plan(), ctx, "proc-1")
        observer.on_process_end("proc-1", RunStatus.SUCCESS, 100)

    assert tracer.start_span.call_count == 2
    # Second call should have a context derived from the pipeline span
    _, kwargs = tracer.start_span.call_args_list[1]
    assert kwargs.get("context") is not None
    process_span.end.assert_called_once()


def test_otel_observer_starts_step_span_with_target_attributes() -> None:
    from unittest.mock import MagicMock, patch

    from loom.etl.observability.observers.otel import OtelRunObserver

    pipeline_span = MagicMock()
    step_span = MagicMock()
    tracer = MagicMock()
    tracer.start_span.side_effect = [pipeline_span, step_span]

    with patch("loom.etl.observability.observers.otel._tracer", tracer):
        observer = OtelRunObserver()
        ctx = RunContext(run_id="run-1")
        observer.on_pipeline_start(_pipeline_plan(), None, ctx)
        observer.on_step_start(_step_plan(), ctx, "step-1")
        observer.on_step_end("step-1", RunStatus.SUCCESS, 50)

    _, kwargs = tracer.start_span.call_args_list[1]
    attrs = kwargs.get("attributes", {})
    assert attrs.get("loom.step") == "MyStep"
    assert attrs.get("loom.target") == "staging.orders"
    step_span.end.assert_called_once()


def test_otel_observer_records_exception_on_step_error() -> None:
    from unittest.mock import MagicMock, patch

    from loom.etl.observability.observers.otel import OtelRunObserver

    pipeline_span = MagicMock()
    step_span = MagicMock()
    tracer = MagicMock()
    tracer.start_span.side_effect = [pipeline_span, step_span]

    exc = ValueError("boom")

    with patch("loom.etl.observability.observers.otel._tracer", tracer):
        observer = OtelRunObserver()
        ctx = RunContext(run_id="run-1")
        observer.on_pipeline_start(_pipeline_plan(), None, ctx)
        observer.on_step_start(_step_plan(), ctx, "step-1")
        observer.on_step_error("step-1", exc)
        observer.on_step_end("step-1", RunStatus.FAILED, 10)

    step_span.record_exception.assert_called_once_with(exc)


def test_otel_observer_tolerates_end_without_matching_start() -> None:
    """on_*_end for unknown IDs must not raise."""
    from unittest.mock import patch

    from opentelemetry.trace import NonRecordingSpan

    from loom.etl.observability.observers.otel import OtelRunObserver

    with patch("loom.etl.observability.observers.otel._tracer") as tracer:
        tracer.start_span.return_value = NonRecordingSpan(
            type(
                "SC",
                (),
                {
                    "is_valid": False,
                    "is_remote": False,
                    "trace_flags": 0,
                    "trace_id": 0,
                    "span_id": 0,
                },
            )()
        )
        observer = OtelRunObserver()
        ctx = RunContext(run_id="run-x")
        # No start called — should not raise
        observer.on_pipeline_end(ctx, RunStatus.SUCCESS, 0)
        observer.on_process_end("unknown-proc", RunStatus.SUCCESS, 0)
        observer.on_step_end("unknown-step", RunStatus.SUCCESS, 0)
