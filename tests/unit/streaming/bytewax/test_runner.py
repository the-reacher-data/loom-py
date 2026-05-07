from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import uvloop
from bytewax.dataflow import Dataflow
from pytest import MonkeyPatch

from loom.core.observability.event import EventKind, LifecycleEvent
from loom.core.observability.observer.otel import OtelLifecycleObserver
from loom.core.observability.runtime import ObservabilityRuntime
from loom.streaming import StreamFlow
from loom.streaming.bytewax.runner import (
    BytewaxRuntimeConfig,
    StreamingRunner,
    _build_backend_options,
)
from tests.unit.streaming.bytewax.cases import Order, Result

pytestmark = pytest.mark.bytewax


class TestStreamingRunner:
    def test_run_generates_a_poll_cycle_trace_id(
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

        assert [event.kind for event in events[:2]] == [EventKind.START, EventKind.END]
        assert events[0].trace_id == "poll-trace"
        assert events[1].trace_id == "poll-trace"

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

    def test_from_dict_loads_runtime_section(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
    ) -> None:
        runner = StreamingRunner.from_dict(bytewax_stream_flow, bytewax_runtime_config_dict)

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
        config = dict(bytewax_runtime_config_dict)
        streaming = config["streaming"]
        assert isinstance(streaming, dict)
        runtime = dict(streaming["runtime"])
        runtime["observability"] = {
            "log": {"enabled": False},
            "otel": {
                "enabled": True,
                "config": {
                    "service_name": "loom-streaming",
                    "tracer_name": "loom.streaming",
                    "endpoint": "",
                },
            },
        }
        config["streaming"] = {"runtime": runtime}

        runner = StreamingRunner.from_dict(bytewax_stream_flow, config)

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
