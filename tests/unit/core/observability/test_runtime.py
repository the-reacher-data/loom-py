"""Unit tests for ObservabilityRuntime — emit, span, error isolation, noop."""

from __future__ import annotations

import pytest

from loom.core.observability.event import EventKind, LifecycleEvent
from loom.core.observability.runtime import ObservabilityRuntime


class _RecordingObserver:
    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []

    def on_event(self, event: LifecycleEvent) -> None:
        self.events.append(event)


class _BrokenObserver:
    def on_event(self, event: LifecycleEvent) -> None:
        raise RuntimeError("observer exploded")


class TestEmit:
    def test_fans_out_to_all_observers(self) -> None:
        a, b = _RecordingObserver(), _RecordingObserver()
        runtime = ObservabilityRuntime([a, b])
        event = LifecycleEvent(scope="node", name="transform", kind=EventKind.START)

        runtime.emit(event)

        assert len(a.events) == 1
        assert len(b.events) == 1

    def test_broken_observer_does_not_stop_others(self) -> None:
        broken = _BrokenObserver()
        good = _RecordingObserver()
        runtime = ObservabilityRuntime([broken, good])
        event = LifecycleEvent(scope="node", name="x", kind=EventKind.START)

        runtime.emit(event)

        assert len(good.events) == 1

    def test_empty_observers_is_safe(self) -> None:
        runtime = ObservabilityRuntime([])
        runtime.emit(LifecycleEvent(scope="node", name="x", kind=EventKind.START))


class TestSpan:
    def test_emits_start_and_end_on_success(self) -> None:
        obs = _RecordingObserver()
        runtime = ObservabilityRuntime([obs])

        with runtime.span("use_case", "CreateOrder"):
            pass

        assert obs.events[0].kind is EventKind.START
        assert obs.events[1].kind is EventKind.END
        assert obs.events[1].status == "ok"

    def test_emits_start_and_error_on_exception(self) -> None:
        obs = _RecordingObserver()
        runtime = ObservabilityRuntime([obs])

        with pytest.raises(ValueError), runtime.span("use_case", "CreateOrder"):
            raise ValueError("bad input")

        assert obs.events[0].kind is EventKind.START
        assert obs.events[1].kind is EventKind.ERROR
        assert obs.events[1].error == "bad input"

    def test_error_event_reraises_exception(self) -> None:
        runtime = ObservabilityRuntime([_RecordingObserver()])

        with pytest.raises(RuntimeError, match="boom"), runtime.span("node", "step"):
            raise RuntimeError("boom")

    def test_end_event_carries_duration(self) -> None:
        obs = _RecordingObserver()
        runtime = ObservabilityRuntime([obs])

        with runtime.span("job", "ingest"):
            pass

        end = obs.events[1]
        assert end.duration_ms is not None
        assert end.duration_ms >= 0

    def test_propagates_trace_and_correlation_id(self) -> None:
        obs = _RecordingObserver()
        runtime = ObservabilityRuntime([obs])

        with runtime.span("node", "x", trace_id="t-1", correlation_id="c-1"):
            pass

        for event in obs.events:
            assert event.trace_id == "t-1"
            assert event.correlation_id == "c-1"

    def test_meta_forwarded_to_both_events(self) -> None:
        obs = _RecordingObserver()
        runtime = ObservabilityRuntime([obs])

        with runtime.span("node", "x", flow="my_flow"):
            pass

        for event in obs.events:
            assert event.meta == {"flow": "my_flow"}


class TestNoop:
    def test_noop_runtime_is_callable(self) -> None:
        runtime = ObservabilityRuntime.noop()

        with runtime.span("use_case", "GetOrder"):
            pass

    def test_noop_emit_is_safe(self) -> None:
        runtime = ObservabilityRuntime.noop()
        runtime.emit(LifecycleEvent(scope="node", name="x", kind=EventKind.END))
