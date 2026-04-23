from __future__ import annotations

import pytest

from loom.core.config import ConfigBinding
from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.core.routing import LogicalRef
from loom.streaming import (
    BatchTask,
    CollectBatch,
    Drain,
    ErrorEnvelope,
    ErrorKind,
    ForEach,
    FromTopic,
    IntoTopic,
    Message,
    MessageMeta,
    PartitionGuarantee,
    PartitionPolicy,
    PartitionStrategy,
    Process,
    ResourceFactory,
    StreamFlow,
    StreamShape,
    Task,
    TaskContext,
)


class _Order(LoomStruct):
    order_id: str


class _ValidatedOrder(LoomStruct):
    order_id: str


class _Client:
    pass


class _ClientFactory:
    def create(self) -> _Client:
        return _Client()

    def close(self, resource: _Client) -> None:
        self.closed = resource


class _Context:
    def __init__(self, resource: _Client) -> None:
        self._resource = resource

    @property
    def resource(self) -> _Client:
        return self._resource


class _OrderPartitionStrategy:
    def partition_key(self, message: Message[_Order]) -> bytes | str | None:
        return message.payload.order_id


class _ValidateOrder(Task[_Order, _ValidatedOrder]):
    def execute(self, message: Message[_Order], **kwargs: object) -> _ValidatedOrder:
        return _ValidatedOrder(order_id=message.payload.order_id)


class _NamedValidateOrder(Task[_Order, _ValidatedOrder]):
    name = "custom"

    def execute(self, message: Message[_Order], **kwargs: object) -> _ValidatedOrder:
        return _ValidatedOrder(order_id=message.payload.order_id)


class _BulkValidateOrder(BatchTask[_Order, _ValidatedOrder]):
    resource = _ClientFactory

    def execute(self, messages: list[Message[_Order]], **kwargs: object) -> list[_ValidatedOrder]:
        return [_ValidatedOrder(order_id=message.payload.order_id) for message in messages]


def test_topic_boundaries_hold_payload_classes_and_explicit_shapes() -> None:
    source = FromTopic("orders.in", payload=_Order, shape=StreamShape.BATCH)
    target = IntoTopic("orders.out", payload=_ValidatedOrder, shape=StreamShape.MANY)

    assert source.name == "orders.in"
    assert source.payload is _Order
    assert source.shape is StreamShape.BATCH
    assert target.payload is _ValidatedOrder
    assert target.shape is StreamShape.MANY


def test_topic_boundaries_use_logical_refs_only() -> None:
    source = FromTopic("orders-input", payload=_Order)
    target = IntoTopic("validated-orders", payload=_ValidatedOrder)

    assert source.logical_ref == LogicalRef("orders-input")
    assert target.logical_ref == LogicalRef("validated-orders")


def test_into_topic_can_be_used_without_payload_for_error_routes() -> None:
    target = IntoTopic("orders.dlq")

    assert target.payload is None
    assert target.shape is StreamShape.RECORD


def test_into_topic_can_declare_partitioning_policy() -> None:
    strategy: PartitionStrategy[_Order] = _OrderPartitionStrategy()
    policy = PartitionPolicy(
        strategy=strategy,
        guarantee=PartitionGuarantee.ENTITY_STABLE,
        allow_repartition=True,
    )
    target = IntoTopic("orders.out", payload=_Order, partitioning=policy)

    assert target.partitioning is policy
    assert policy.guarantee is PartitionGuarantee.ENTITY_STABLE
    assert policy.allow_repartition is True


def test_message_and_metadata_are_loom_structs() -> None:
    meta = MessageMeta(
        message_id="msg-1",
        correlation_id="corr-1",
        trace_id="trace-1",
        causation_id="cause-1",
        produced_at_ms=42,
        message_type="order.created",
        message_version=2,
        topic="orders.in",
        partition=2,
        offset=9,
        key=b"tenant-a",
        headers={"x": b"1"},
    )
    message = Message(payload=_Order(order_id="o-1"), meta=meta)

    assert isinstance(meta, LoomFrozenStruct)
    assert isinstance(message, LoomFrozenStruct)
    assert message.payload.order_id == "o-1"
    assert message.meta.causation_id == "cause-1"
    assert message.meta.produced_at_ms == 42
    assert message.meta.message_type == "order.created"
    assert message.meta.message_version == 2
    assert message.meta.topic == "orders.in"
    assert message.meta.headers == {"x": b"1"}


def test_value_contracts_are_immutable() -> None:
    batch = CollectBatch(max_records=500, timeout_ms=250)

    with pytest.raises(AttributeError):
        batch.max_records = 100


def test_shape_adapters_are_explicit_contracts() -> None:
    batch = CollectBatch(max_records=500, timeout_ms=250)

    assert isinstance(ForEach(), ForEach)
    assert batch.max_records == 500
    assert batch.timeout_ms == 250
    assert isinstance(Drain(), Drain)


@pytest.mark.parametrize(
    ("max_records", "timeout_ms"),
    [(0, 1), (1, 0)],
)
def test_collect_batch_rejects_non_positive_limits(max_records: int, timeout_ms: int) -> None:
    with pytest.raises(ValueError):
        CollectBatch(max_records=max_records, timeout_ms=timeout_ms)


def test_error_envelope_carries_error_kind_and_original_message() -> None:
    message = Message(payload=_Order(order_id="o-1"), meta=MessageMeta(message_id="msg-1"))
    envelope = ErrorEnvelope(
        kind=ErrorKind.TASK,
        reason="task failed",
        original_message=message,
    )

    assert envelope.kind is ErrorKind.TASK
    assert envelope.reason == "task failed"
    assert envelope.original_message is message


def test_resource_contracts_are_protocols() -> None:
    client = _Client()
    factory = _ClientFactory()
    context = _Context(client)

    assert isinstance(factory, ResourceFactory)
    assert isinstance(context, TaskContext)
    assert context.resource is client


def test_task_names_resolve_from_class_or_explicit_name() -> None:
    assert _ValidateOrder.task_name() == "_ValidateOrder"
    assert _NamedValidateOrder.task_name() == "custom"


def test_task_subclasses_execute_with_messages() -> None:
    task = _ValidateOrder()
    message = Message(payload=_Order(order_id="o-1"), meta=MessageMeta(message_id="msg-1"))

    assert task.execute(message) == _ValidatedOrder(order_id="o-1")


def test_batch_task_declaration_holds_resource_and_executes_batch() -> None:
    batch = _BulkValidateOrder()
    message = Message(payload=_Order(order_id="o-1"), meta=MessageMeta(message_id="msg-1"))

    assert _BulkValidateOrder.resource is _ClientFactory
    assert batch.execute([message]) == [_ValidatedOrder(order_id="o-1")]


def test_process_requires_nodes_and_preserves_order() -> None:
    target = IntoTopic("orders.out", payload=_ValidatedOrder)
    process = Process[_Order, _ValidatedOrder](_ValidateOrder, target)

    assert process.nodes == (_ValidateOrder, target)
    assert list(process) == [_ValidateOrder, target]
    assert len(process) == 2


def test_process_rejects_empty_graph() -> None:
    with pytest.raises(ValueError, match="at least one node"):
        Process[_Order, _ValidatedOrder]()


def test_stream_flow_captures_source_process_output_and_error_routes() -> None:
    source = FromTopic("orders.in", payload=_Order)
    output = IntoTopic("orders.out", payload=_ValidatedOrder)
    dlq = IntoTopic("orders.dlq")
    process = Process[_Order, _ValidatedOrder](_ValidateOrder)
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


def test_stream_flow_rejects_empty_name() -> None:
    source = FromTopic("orders.in", payload=_Order)
    process = Process[_Order, _ValidatedOrder](_ValidateOrder)

    with pytest.raises(ValueError, match="name"):
        StreamFlow(name="", source=source, process=process)


class _ResourceInjectedTask(Task[_Order, _ValidatedOrder]):
    def execute(self, message: Message[_Order], **kwargs: object) -> _ValidatedOrder:
        client = kwargs.get("client")
        return _ValidatedOrder(order_id=f"{message.payload.order_id}:{client}")


def test_task_receives_injected_resources() -> None:
    task = _ResourceInjectedTask()
    message = Message(payload=_Order(order_id="o-1"), meta=MessageMeta(message_id="msg-1"))

    result = task.execute(message, client="mock-client")

    assert result == _ValidatedOrder(order_id="o-1:mock-client")


def test_task_ignores_unused_resources() -> None:
    task = _ValidateOrder()
    message = Message(payload=_Order(order_id="o-1"), meta=MessageMeta(message_id="msg-1"))

    result = task.execute(message, client="mock-client", db="mock-db")

    assert result == _ValidatedOrder(order_id="o-1")


def test_task_from_config_returns_config_binding() -> None:
    binding = _ValidateOrder.from_config("streaming.tasks.validate")

    assert isinstance(binding, ConfigBinding)
    assert binding.target is _ValidateOrder
    assert binding.config_path == "streaming.tasks.validate"
    assert binding.overrides == {}


def test_task_from_config_with_overrides() -> None:
    binding = _ValidateOrder.from_config("streaming.tasks.validate", timeout_ms=20_000)

    assert binding.overrides == {"timeout_ms": 20_000}


def test_batch_task_from_config_returns_config_binding() -> None:
    binding = _BulkValidateOrder.from_config("streaming.tasks.bulk")

    assert isinstance(binding, ConfigBinding)
    assert binding.target is _BulkValidateOrder
    assert binding.config_path == "streaming.tasks.bulk"
