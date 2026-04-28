"""Step and flow contract tests for streaming."""

from __future__ import annotations

import pytest

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming import (
    ErrorKind,
    FromTopic,
    IntoTopic,
    Message,
    MessageMeta,
    Process,
    ResourceFactory,
    StepContext,
    StreamFlow,
)
from tests.unit.streaming.contracts.cases import (
    Client,
    ClientFactory,
    Context,
    NamedValidateOrder,
    Order,
    ValidatedOrder,
    ValidateOrder,
)


class TestSteps:
    def test_resource_contracts_are_protocols(self) -> None:
        client = Client()
        factory = ClientFactory()
        context = Context(client)

        assert isinstance(factory, ResourceFactory)
        assert isinstance(context, StepContext)
        assert context.resource is client

    def test_step_names_resolve_from_class_or_explicit_name(self) -> None:
        assert ValidateOrder.step_name() == "ValidateOrder"
        assert NamedValidateOrder.step_name() == "custom"

    def test_step_receives_injected_resources(self) -> None:
        task = ValidateOrder()
        message = Message(payload=Order(order_id="o-1"), meta=MessageMeta(message_id="msg-1"))

        result = task.execute(message, client="mock-client")

        assert result == ValidatedOrder(order_id="o-1")

    def test_process_requires_nodes_and_preserves_order(self) -> None:
        target = IntoTopic("orders.out", payload=ValidatedOrder)
        task = ValidateOrder()
        process = Process[Order, ValidatedOrder](task, target)

        assert process.nodes == (task, target)
        assert list(process) == [task, target]
        assert len(process) == 2

    def test_process_rejects_empty_graph(self) -> None:
        with pytest.raises(ValueError, match="at least one node"):
            Process[Order, ValidatedOrder]()

    def test_stream_flow_captures_source_process_output_and_error_routes(self) -> None:
        source = FromTopic("orders.in", payload=Order)
        output = IntoTopic("orders.out", payload=ValidatedOrder)
        dlq: IntoTopic[LoomStruct | LoomFrozenStruct] = IntoTopic(
            "orders.dlq",
            payload=ValidatedOrder,
        )
        process = Process[Order, ValidatedOrder](ValidateOrder())
        flow = StreamFlow(
            name="orders",
            source=source,
            process=process,
            output=output,
            errors={ErrorKind.TASK: dlq},
        )

        assert flow.name == "orders"
        assert flow.source is source
        assert flow.process is process
        assert flow.output is output
        assert flow.errors[ErrorKind.TASK] is dlq

    def test_stream_flow_rejects_empty_name(self) -> None:
        source = FromTopic("orders.in", payload=Order)
        process = Process[Order, ValidatedOrder](ValidateOrder())

        with pytest.raises(ValueError, match="name"):
            StreamFlow(name="", source=source, process=process)
