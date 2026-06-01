from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import uvloop
from bytewax.dataflow import Dataflow
from pytest import MonkeyPatch

from loom.core.async_bridge import build_backend_options as _build_backend_options
from loom.core.config import ConfigContext
from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.core.observability.observer.otel import OtelLifecycleObserver
from loom.core.observability.runtime import ObservabilityRuntime
from loom.streaming import Drain, FromMongoCDC, Process, StreamFlow
from loom.streaming.bytewax.runner import (
    BytewaxRuntimeConfig,
    StreamingRunner,
)
from loom.streaming.mongo import MongoCDCEvent
from tests.unit.streaming.bytewax.cases import Order, Result

pytestmark = pytest.mark.bytewax


class TestStreamingRunner:
    def test_run_generates_a_poll_cycle_id(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
        monkeypatch: MonkeyPatch,
    ) -> None:
        events: list[LifecycleEvent] = []

        class _RecordingObserver:
            def on_event(self, event: LifecycleEvent) -> None:
                events.append(event)

        runtime = ObservabilityRuntime([_RecordingObserver()])
        runner = StreamingRunner.from_dict(
            bytewax_stream_flow,
            bytewax_runtime_config_dict,
            observability_runtime=runtime,
        )
        dataflow = Dataflow("test")

        def _fake_prepare() -> object:
            def shutdown() -> None:
                return None

            return SimpleNamespace(
                dataflow=dataflow,
                shutdown=shutdown,
            )

        monkeypatch.setattr(runner, "prepare_run", _fake_prepare)
        monkeypatch.setattr(
            "loom.streaming.bytewax.runner.cli_main", lambda *_args, **_kwargs: None
        )
        monkeypatch.setattr("loom.streaming.bytewax.runner.generate_trace_id", lambda: "poll-trace")

        runner.run()

        poll_cycle = [e for e in events if e.scope == Scope.POLL_CYCLE]
        assert [e.kind for e in poll_cycle] == [EventKind.START, EventKind.END]
        assert poll_cycle[0].id == "poll-trace"
        assert poll_cycle[1].id == "poll-trace"
        assert poll_cycle[0].trace_id is None
        assert poll_cycle[1].trace_id is None

    def test_run_uses_bytewax_cli_main_with_runtime_config(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
        monkeypatch: MonkeyPatch,
    ) -> None:
        runner = StreamingRunner.from_dict(bytewax_stream_flow, bytewax_runtime_config_dict)
        dataflow = Dataflow("test")
        shutdown_calls: list[str] = []
        cli_calls: dict[str, object] = {}

        def _fake_prepare() -> object:
            def shutdown() -> None:
                shutdown_calls.append("done")

            runner._shutdown = shutdown
            return SimpleNamespace(
                dataflow=dataflow,
                shutdown=shutdown,
            )

        def _fake_cli_main(flow: Dataflow, **kwargs: object) -> None:
            cli_calls["flow"] = flow
            cli_calls["kwargs"] = kwargs

        monkeypatch.setattr(runner, "prepare_run", _fake_prepare)
        monkeypatch.setattr("loom.streaming.bytewax.runner.cli_main", _fake_cli_main)

        runner.run()

        assert cli_calls["flow"] is dataflow
        kwargs = cli_calls["kwargs"]
        assert isinstance(kwargs, dict)
        assert kwargs["workers_per_process"] == 2
        assert kwargs["process_id"] == 1
        assert kwargs["addresses"] == ["127.0.0.1:2101", "127.0.0.1:2102"]
        assert shutdown_calls == ["done"]

    def test_run_calls_shutdown_when_cli_main_raises(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
        monkeypatch: MonkeyPatch,
    ) -> None:
        runner = StreamingRunner.from_dict(bytewax_stream_flow, bytewax_runtime_config_dict)
        dataflow = Dataflow("test")
        shutdown_calls: list[str] = []

        def _fake_prepare() -> object:
            def shutdown() -> None:
                shutdown_calls.append("done")

            runner._shutdown = shutdown
            return SimpleNamespace(dataflow=dataflow, shutdown=shutdown)

        def _fake_cli_main(flow: Dataflow, **kwargs: object) -> None:
            raise RuntimeError("boom")

        monkeypatch.setattr(runner, "prepare_run", _fake_prepare)
        monkeypatch.setattr("loom.streaming.bytewax.runner.cli_main", _fake_cli_main)

        with pytest.raises(RuntimeError, match="boom"):
            runner.run()

        assert shutdown_calls == ["done"]

    def test_from_context_loads_runtime_section(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
    ) -> None:
        runner = StreamingRunner.from_context(
            bytewax_stream_flow,
            config=ConfigContext.from_dict(bytewax_runtime_config_dict),
        )

        assert runner._runtime.workers_per_process == 2
        assert runner._runtime.process_id == 1
        assert runner._runtime.addresses == ("127.0.0.1:2101", "127.0.0.1:2102")
        assert runner._runtime.epoch_interval_ms == 5000
        assert runner._runtime.recovery is not None
        assert runner._runtime.recovery.db_dir == "/var/lib/loom/tests/bytewax-recovery"
        assert runner._runtime.recovery.backup_interval_ms == 30000

    def test_from_dict_loads_streaming_observability_section(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
    ) -> None:
        runner = StreamingRunner.from_dict(bytewax_stream_flow, bytewax_runtime_config_dict)

        assert any(
            isinstance(obs, OtelLifecycleObserver)
            for obs in runner._observability_runtime.observers
        )

    def test_from_yaml_loads_runtime_section(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        tmp_path: Path,
    ) -> None:
        config_path = tmp_path / "streaming.yaml"
        config_path.write_text(
            """
kafka:
  consumer:
    brokers: ["localhost:9092"]
    group_id: "test"
    topics: ["orders.in"]
  producer:
    brokers: ["localhost:9092"]
    client_id: "test-producer"
    topic: "orders.out"
streaming:
  runtime:
    workers_per_process: 3
    epoch_interval_ms: 7000
    process_id: 2
    addresses: ["127.0.0.1:2201", "127.0.0.1:2202"]
    recovery:
      db_dir: "/var/lib/loom/tests/bytewax-runtime"
      backup_interval_ms: 45000
""".strip()
        )

        runner = StreamingRunner.from_yaml(bytewax_stream_flow, str(config_path))

        assert runner._runtime.workers_per_process == 3
        assert runner._runtime.process_id == 2
        assert runner._runtime.addresses == ("127.0.0.1:2201", "127.0.0.1:2202")
        assert runner._runtime.epoch_interval_ms == 7000
        assert runner._runtime.recovery is not None
        assert runner._runtime.recovery.db_dir == "/var/lib/loom/tests/bytewax-runtime"
        assert runner._runtime.recovery.backup_interval_ms == 45000


class TestPrepareRun:
    def test_build_dataflow_supports_mongo_cdc_source_without_runtime_import(
        self,
    ) -> None:
        flow: StreamFlow[MongoCDCEvent, MongoCDCEvent] = StreamFlow(
            name="mongo_runner_build",
            source=FromMongoCDC("domain_events", collections=("orders",)),
            process=Process(Drain()),
        )
        runner = StreamingRunner.from_dict(flow, _mongo_runner_config())

        dataflow = runner.build_dataflow()

        assert isinstance(dataflow, Dataflow)
        runner.shutdown()

    def test_prepare_run_releases_previous_shutdown_before_rebuilding(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
        monkeypatch: MonkeyPatch,
    ) -> None:
        runner = StreamingRunner.from_dict(bytewax_stream_flow, bytewax_runtime_config_dict)
        shutdown_calls: list[str] = []
        call_count = 0

        def _fake_prepare_run(
            plan: object,
            *,
            observability_runtime: object = None,
            runtime: object = None,
            **kwargs: object,
        ) -> object:
            nonlocal call_count
            call_count += 1
            n = call_count

            def shutdown() -> None:
                shutdown_calls.append(f"run-{n}")

            return SimpleNamespace(dataflow=Dataflow("test"), shutdown=shutdown)

        monkeypatch.setattr("loom.streaming.bytewax.runner._prepare_run", _fake_prepare_run)

        runner.prepare_run()
        assert shutdown_calls == []

        runner.prepare_run()
        assert shutdown_calls == ["run-1"]


class TestBytewaxRuntimeConfig:
    def test_defaults_match_runtime_contract(self) -> None:
        cfg = BytewaxRuntimeConfig()

        assert cfg.async_backend == "asyncio"
        assert cfg.use_uvloop is False
        assert cfg.force_shutdown_timeout_ms is None

    def test_custom_values_are_preserved(self) -> None:
        cfg = BytewaxRuntimeConfig(
            async_backend="trio",
            use_uvloop=True,
            force_shutdown_timeout_ms=5000,
        )

        assert cfg.async_backend == "trio"
        assert cfg.use_uvloop is True
        assert cfg.force_shutdown_timeout_ms == 5000


class TestBuildBackendOptions:
    def test_asyncio_without_uvloop_returns_empty(self) -> None:
        assert _build_backend_options("asyncio", use_uvloop=False) == {}

    def test_trio_with_uvloop_flag_returns_empty(self) -> None:
        assert _build_backend_options("trio", use_uvloop=True) == {}

    def test_asyncio_with_uvloop_adds_loop_factory(self) -> None:
        if sys.platform == "win32":
            pytest.skip("uvloop not available on Windows")

        opts = _build_backend_options("asyncio", use_uvloop=True)

        assert opts.get("loop_factory") is uvloop.new_event_loop


class TestRunFlowSpan:
    def test_run_emits_flow_start_and_end(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
        monkeypatch: MonkeyPatch,
    ) -> None:
        events: list[LifecycleEvent] = []

        class _RecordingObserver:
            def on_event(self, event: LifecycleEvent) -> None:
                events.append(event)

        runtime = ObservabilityRuntime([_RecordingObserver()])
        runner = StreamingRunner.from_dict(
            bytewax_stream_flow,
            bytewax_runtime_config_dict,
            observability_runtime=runtime,
        )
        dataflow = Dataflow("test")

        def _fake_prepare() -> object:
            return SimpleNamespace(dataflow=dataflow, shutdown=lambda: None)

        monkeypatch.setattr(runner, "prepare_run", _fake_prepare)
        monkeypatch.setattr("loom.streaming.bytewax.runner.cli_main", lambda *_a, **_kw: None)

        runner.run()

        flow_events = [e for e in events if e.scope == Scope.FLOW]
        assert [e.kind for e in flow_events] == [EventKind.START, EventKind.END]
        assert flow_events[1].duration_ms is not None
        assert "node_count" in flow_events[0].meta
        assert flow_events[0].meta["node_count"] == flow_events[1].meta["node_count"]

    def test_run_emits_flow_error_when_cli_main_raises(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
        monkeypatch: MonkeyPatch,
    ) -> None:
        events: list[LifecycleEvent] = []

        class _RecordingObserver:
            def on_event(self, event: LifecycleEvent) -> None:
                events.append(event)

        runtime = ObservabilityRuntime([_RecordingObserver()])
        runner = StreamingRunner.from_dict(
            bytewax_stream_flow,
            bytewax_runtime_config_dict,
            observability_runtime=runtime,
        )
        dataflow = Dataflow("test")

        def _fake_prepare() -> object:
            return SimpleNamespace(dataflow=dataflow, shutdown=lambda: None)

        monkeypatch.setattr(runner, "prepare_run", _fake_prepare)
        monkeypatch.setattr(
            "loom.streaming.bytewax.runner.cli_main",
            lambda *_a, **_kw: (_ for _ in ()).throw(RuntimeError("stream failure")),
        )

        with pytest.raises(RuntimeError, match="stream failure"):
            runner.run()

        flow_events = [e for e in events if e.scope == Scope.FLOW]
        assert flow_events[0].kind is EventKind.START
        assert flow_events[1].kind is EventKind.ERROR

    def test_run_emits_poll_cycle_end_when_prepare_run_fails(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
        monkeypatch: MonkeyPatch,
    ) -> None:
        events: list[LifecycleEvent] = []

        class _RecordingObserver:
            def on_event(self, event: LifecycleEvent) -> None:
                events.append(event)

        runtime = ObservabilityRuntime([_RecordingObserver()])
        runner = StreamingRunner.from_dict(
            bytewax_stream_flow,
            bytewax_runtime_config_dict,
            observability_runtime=runtime,
        )

        monkeypatch.setattr(
            runner, "prepare_run", lambda: (_ for _ in ()).throw(RuntimeError("prep failed"))
        )

        with pytest.raises(RuntimeError, match="prep failed"):
            runner.run()

        poll_cycle = [e for e in events if e.scope == Scope.POLL_CYCLE]
        assert poll_cycle[0].kind is EventKind.START
        assert poll_cycle[1].kind is EventKind.END


class TestPrepareRunErrorSinks:
    def test_prepare_run_accepts_error_sinks_and_passes_them_through(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
        monkeypatch: MonkeyPatch,
    ) -> None:
        from loom.streaming.core._errors import ErrorKind

        runner = StreamingRunner.from_dict(bytewax_stream_flow, bytewax_runtime_config_dict)
        received_kwargs: dict[str, object] = {}
        error_sink_value = object()

        def _fake_prepare_run(
            plan: object,
            *,
            observability_runtime: object = None,
            runtime: object = None,
            **kwargs: object,
        ) -> object:
            received_kwargs.update(kwargs)
            return SimpleNamespace(dataflow=Dataflow("test"), shutdown=lambda: None)

        monkeypatch.setattr("loom.streaming.bytewax.runner._prepare_run", _fake_prepare_run)

        runner.prepare_run(error_sinks={ErrorKind.TASK: error_sink_value})

        assert "error_sinks" in received_kwargs
        assert received_kwargs["error_sinks"] == {ErrorKind.TASK: error_sink_value}


def _mongo_runner_config() -> dict[str, object]:
    return {
        "mongo": {
            "sources": {
                "domain_events": {
                    "uri": "mongodb://localhost:27017",
                    "database": "app",
                }
            }
        }
    }
