"""TDD for Bytewax runtime adapter — end-to-end execution."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from loom.streaming.compiler._plan import CompiledPlan, CompiledSink
from loom.streaming.core._errors import ErrorKind
from loom.streaming.core._message import Message
from loom.streaming.kafka import KafkaRecord, MessageDescriptor, MsgspecCodec, build_message
from loom.streaming.kafka._config import ProducerSettings
from loom.streaming.nodes._boundary import IntoTopic
from loom.streaming.nodes._shape import Drain
from loom.streaming.testing import StreamingTestRunner
from tests.unit.streaming.bytewax.cases import DoubleStep, Order, Result, SuffixStep

pytestmark = pytest.mark.bytewax

PlanFactory = Callable[..., CompiledPlan]


class TestStepNodeExecution:
    """A simple Step node must transform records end-to-end."""

    def test_single_task_produces_transformed_output(
        self,
        bytewax_double_step: DoubleStep,
        bytewax_message: Message[Order],
        bytewax_plan_factory: PlanFactory,
    ) -> None:
        plan = bytewax_plan_factory(bytewax_double_step)
        runner = StreamingTestRunner(plan).with_messages([bytewax_message])
        runner.run()
        results = runner.output

        assert len(results) == 1
        assert isinstance(results[0], Message)
        assert results[0].payload == Result(value="AA")
        assert results[0].meta.message_id == "msg-1"

    def test_task_chain_passes_message_to_next_task(
        self,
        bytewax_double_step: DoubleStep,
        bytewax_suffix_step: SuffixStep,
        bytewax_message: Message[Order],
        bytewax_plan_factory: PlanFactory,
    ) -> None:
        plan = bytewax_plan_factory(bytewax_double_step, bytewax_suffix_step)
        runner = StreamingTestRunner(plan).with_messages([bytewax_message])
        runner.run()
        results = runner.output

        assert [message.payload.value for message in results] == ["AA:ok"]


class TestSourceDecode:
    """Raw Kafka records must be decoded before reaching DSL nodes."""

    def test_kafka_record_source_decodes_envelope_before_task_execution(
        self,
        bytewax_double_step: DoubleStep,
        bytewax_order: Order,
        bytewax_plan_factory: PlanFactory,
    ) -> None:
        plan = bytewax_plan_factory(bytewax_double_step)
        codec = MsgspecCodec[Order]()
        envelope = build_message(
            bytewax_order,
            MessageDescriptor(message_type="order.created", message_version=1),
            correlation_id="corr-1",
            trace_id="trace-1",
            produced_at_ms=10,
        )
        source_record = KafkaRecord(
            topic="orders.in",
            key=b"tenant-a",
            value=codec.encode(envelope),
            headers={"h": b"1"},
            partition=0,
            offset=3,
            timestamp_ms=11,
        )
        runner = StreamingTestRunner(plan).with_messages([source_record])
        runner.run()
        results = runner.output

        assert len(results) == 1
        assert results[0].payload == Result(value="AA")
        assert results[0].meta.message_id == "orders.in:0:3"
        assert results[0].meta.message_type == "order.created"
        assert results[0].meta.message_version == 1
        assert results[0].meta.correlation_id == "corr-1"
        assert results[0].meta.trace_id == "trace-1"
        assert results[0].meta.produced_at_ms == 10
        assert results[0].meta.key == b"tenant-a"
        assert results[0].meta.headers == {"h": b"1"}

    def test_wire_decode_error_routes_to_testing_error_sink(
        self,
        bytewax_double_step: DoubleStep,
        bytewax_plan_factory: PlanFactory,
    ) -> None:
        error_sink = CompiledSink(
            settings=ProducerSettings(
                brokers=("localhost:9092",),
                client_id="test-producer",
                topic="orders.dlq",
            ),
            topic="orders.dlq",
            partition_policy=None,
        )
        plan = bytewax_plan_factory(bytewax_double_step, error_routes={ErrorKind.WIRE: error_sink})
        source_record = KafkaRecord(
            topic="orders.in",
            key=b"tenant-a",
            value=b"not-msgpack",
            headers={"h": b"1"},
            partition=0,
            offset=4,
            timestamp_ms=12,
        )
        runner = (
            StreamingTestRunner(plan).with_messages([source_record]).capture_errors(ErrorKind.WIRE)
        )
        runner.run()
        results = runner.output
        errors = runner.errors[ErrorKind.WIRE]

        assert results == []
        assert len(errors) == 1
        assert errors[0].error.kind is ErrorKind.WIRE
        assert errors[0].error.original_message is None
        assert errors[0].raw == b"not-msgpack"
        assert errors[0].topic == "orders.in"
        assert errors[0].partition == 0
        assert errors[0].offset == 4
        assert errors[0].key == b"tenant-a"
        assert errors[0].headers == {"h": b"1"}


class TestTerminalNodes:
    """Terminal DSL nodes must be executable with Bytewax testing sinks."""

    def test_into_topic_node_wires_terminal_sink(
        self,
        bytewax_double_step: DoubleStep,
        bytewax_message: Message[Order],
        bytewax_plan_factory: PlanFactory,
    ) -> None:
        target = IntoTopic("out", payload=Result)
        plan = bytewax_plan_factory(bytewax_double_step, output=target)
        runner = StreamingTestRunner(plan).with_messages([bytewax_message])
        runner.run()
        results = runner.output

        assert [message.payload.value for message in results] == ["AA"]

    def test_drain_node_swallows_stream(
        self,
        bytewax_double_step: DoubleStep,
        bytewax_message: Message[Order],
        bytewax_plan_factory: PlanFactory,
    ) -> None:
        plan = bytewax_plan_factory(bytewax_double_step, Drain())
        runner = StreamingTestRunner(plan).with_messages([bytewax_message])
        runner.run()
        results = runner.output

        assert results == []

    def test_payload_helper_builds_default_test_metadata(
        self,
        bytewax_double_step: DoubleStep,
        bytewax_order: Order,
        bytewax_plan_factory: PlanFactory,
    ) -> None:
        plan = bytewax_plan_factory(bytewax_double_step)
        runner = StreamingTestRunner(plan).with_payloads([bytewax_order])

        runner.run()

        results = runner.output
        assert len(results) == 1
        assert results[0].meta.message_id == "test-0"
        assert results[0].meta.topic == "in"

    def test_into_topic_node_does_not_duplicate_flow_output(
        self,
        bytewax_double_step: DoubleStep,
        bytewax_message: Message[Order],
        bytewax_plan_factory: PlanFactory,
    ) -> None:
        target = IntoTopic("out", payload=Result)
        plan = bytewax_plan_factory(bytewax_double_step, output=target)
        runner = StreamingTestRunner(plan).with_messages([bytewax_message])

        runner.run()

        results = runner.output
        assert [message.payload.value for message in results] == ["AA"]


class TestOutputAndErrorWiring:
    """Output and error routes must coexist without interfering."""

    def test_valid_output_and_wire_error_can_be_captured_together(
        self,
        bytewax_double_step: DoubleStep,
        bytewax_plan_factory: PlanFactory,
    ) -> None:
        error_sink = CompiledSink(
            settings=ProducerSettings(
                brokers=("localhost:9092",),
                client_id="test-producer",
                topic="orders.dlq",
            ),
            topic="orders.dlq",
            partition_policy=None,
        )
        target = IntoTopic("out", payload=Result)
        codec = MsgspecCodec[Order]()
        envelope = build_message(
            Order(order_id="A"),
            MessageDescriptor(message_type="order.created", message_version=1),
        )
        valid_record = KafkaRecord(
            topic="orders.in",
            key=b"tenant-a",
            value=codec.encode(envelope),
            headers={},
            partition=0,
            offset=1,
            timestamp_ms=10,
        )
        invalid_record = KafkaRecord(
            topic="orders.in",
            key=b"tenant-a",
            value=b"bad-wire",
            headers={},
            partition=0,
            offset=2,
            timestamp_ms=11,
        )
        plan = bytewax_plan_factory(
            bytewax_double_step,
            output=target,
            error_routes={ErrorKind.WIRE: error_sink},
        )
        runner = (
            StreamingTestRunner(plan)
            .with_messages([valid_record, invalid_record])
            .capture_errors(ErrorKind.WIRE)
        )

        runner.run()

        assert [message.payload.value for message in runner.output] == ["AA"]
        errors = runner.errors[ErrorKind.WIRE]
        assert len(errors) == 1
        assert errors[0].raw == b"bad-wire"
        assert errors[0].offset == 2
