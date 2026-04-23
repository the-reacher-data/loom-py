from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from pytest import MonkeyPatch

pytest.importorskip("bytewax")

from bytewax.dataflow import Dataflow

from loom.core.model import LoomFrozenStruct
from loom.streaming import FromTopic, IntoTopic, Message, Process, StreamFlow, Task
from loom.streaming.bytewax.runner import StreamingRunner


class _Order(LoomFrozenStruct, frozen=True):
    order_id: str


class _Result(LoomFrozenStruct, frozen=True):
    value: str


class _DoubleTask(Task[_Order, _Result]):
    def execute(self, message: Message[_Order], **kwargs: object) -> _Result:
        return _Result(value=message.payload.order_id * 2)


def _flow() -> StreamFlow[_Order, _Result]:
    return StreamFlow(
        name="runner_flow",
        source=FromTopic("orders.in", payload=_Order),
        process=Process(_DoubleTask()),
        output=IntoTopic("orders.out", payload=_Result),
    )


def _config_dict() -> dict[str, object]:
    return {
        "kafka": {
            "consumer": {
                "brokers": ["localhost:9092"],
                "group_id": "test",
                "topics": ["orders.in"],
            },
            "producer": {
                "brokers": ["localhost:9092"],
                "client_id": "test-producer",
                "topic": "orders.out",
            },
        },
        "streaming": {
            "runtime": {
                "workers_per_process": 2,
                "epoch_interval_ms": 5000,
                "addresses": ["127.0.0.1:2101", "127.0.0.1:2102"],
                "process_id": 1,
                "recovery": {
                    "db_dir": "/tmp/loom-bytewax-recovery",
                    "backup_interval_ms": 30000,
                },
            }
        },
    }


class TestStreamingRunner:
    def test_run_uses_bytewax_cli_main_with_runtime_config(
        self,
        monkeypatch: MonkeyPatch,
    ) -> None:
        runner = StreamingRunner.from_dict(_flow(), _config_dict())
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

    def test_from_dict_loads_runtime_section(self) -> None:
        runner = StreamingRunner.from_dict(_flow(), _config_dict())

        assert runner._runtime.workers_per_process == 2
        assert runner._runtime.process_id == 1
        assert runner._runtime.addresses == ("127.0.0.1:2101", "127.0.0.1:2102")
        assert runner._runtime.epoch_interval_ms == 5000
        assert runner._runtime.recovery is not None
        assert runner._runtime.recovery.db_dir == "/tmp/loom-bytewax-recovery"
        assert runner._runtime.recovery.backup_interval_ms == 30000

    def test_from_yaml_loads_runtime_section(self, tmp_path: Path) -> None:
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
      db_dir: "/tmp/loom-bytewax-runtime"
      backup_interval_ms: 45000
""".strip()
        )

        runner = StreamingRunner.from_yaml(_flow(), str(config_path))

        assert runner._runtime.workers_per_process == 3
        assert runner._runtime.process_id == 2
        assert runner._runtime.addresses == ("127.0.0.1:2201", "127.0.0.1:2202")
        assert runner._runtime.epoch_interval_ms == 7000
        assert runner._runtime.recovery is not None
        assert runner._runtime.recovery.db_dir == "/tmp/loom-bytewax-runtime"
        assert runner._runtime.recovery.backup_interval_ms == 45000
