"""Direct tests for Bytewax node handler helpers."""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from typing import cast

import pytest

from loom.core.errors.errors import RuleViolation
from loom.core.model import LoomStruct
from loom.streaming.bytewax import _node_handlers
from loom.streaming.core._message import Message, MessageMeta
from loom.streaming.core._typing import StreamPayload
from loom.streaming.nodes._boundary import IntoTopic
from loom.streaming.nodes._shape import CollectBatch, Drain
from loom.streaming.nodes._step import BatchExpandStep, BatchStep, ExpandStep, RecordStep

pytestmark = pytest.mark.bytewax


class _Payload(LoomStruct):
    value: str


class _FakeNode:
    pass


def _message(value: str = "v") -> Message[_Payload]:
    return Message(payload=_Payload(value=value), meta=MessageMeta(message_id="m-1"))


class _RecordingObserver:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, int, str | None]] = []

    def on_flow_start(self, flow_name: str, *, node_count: int) -> None:
        del flow_name, node_count

    def on_flow_end(self, flow_name: str, *, status: str, duration_ms: int) -> None:
        del flow_name, status, duration_ms

    def on_node_start(self, flow_name: str, node_idx: int, *, node_type: str) -> None:
        self.events.append(("start", flow_name, node_idx, node_type))

    def on_node_end(
        self,
        flow_name: str,
        node_idx: int,
        *,
        node_type: str,
        status: str,
        duration_ms: int,
    ) -> None:
        del duration_ms
        self.events.append(("end", flow_name, node_idx, f"{node_type}:{status}"))

    def on_node_error(
        self,
        flow_name: str,
        node_idx: int,
        *,
        node_type: str,
        exc: Exception,
    ) -> None:
        self.events.append(("error", flow_name, node_idx, f"{node_type}:{type(exc).__name__}"))


class _UpperRecordStep(RecordStep[_Payload, _Payload]):
    def execute(self, message: Message[StreamPayload], **kwargs: object) -> _Payload:
        del kwargs
        payload = _message_payload(message)
        return _Payload(value=payload.value.upper())


class _BoomRecordStep(RecordStep[_Payload, _Payload]):
    def execute(self, message: Message[StreamPayload], **kwargs: object) -> _Payload:
        del message, kwargs
        raise RuleViolation("value", "boom")


class _UpperBatchStep(BatchStep[_Payload, _Payload]):
    def execute(
        self,
        messages: list[Message[StreamPayload]],
        **kwargs: object,
    ) -> list[StreamPayload]:
        del kwargs
        return [
            cast(StreamPayload, _Payload(value=_message_payload(message).value.upper()))
            for message in messages
        ]


class _UpperExpandStep(ExpandStep[_Payload, _Payload]):
    def execute(
        self,
        message: Message[StreamPayload],
        **kwargs: object,
    ) -> list[StreamPayload]:
        del kwargs
        payload = _message_payload(message)
        return [
            cast(StreamPayload, _Payload(value=payload.value.upper())),
            cast(StreamPayload, _Payload(value=payload.value.lower())),
        ]


class _UpperBatchExpandStep(BatchExpandStep[_Payload, _Payload]):
    def execute(
        self,
        messages: list[Message[StreamPayload]],
        **kwargs: object,
    ) -> list[StreamPayload]:
        del kwargs
        return [
            cast(StreamPayload, _Payload(value=_message_payload(message).value.upper()))
            for message in messages
        ]


def test_require_message_rejects_non_message() -> None:
    with pytest.raises(TypeError, match="Expected Message"):
        _node_handlers._require_message(object())


def test_replace_payloads_rejects_mismatched_lengths() -> None:
    with pytest.raises(RuntimeError, match="must match input length"):
        _node_handlers._replace_payloads([_message("a")], [])


def test_wire_node_dispatches_registered_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[tuple[object, object, int, object]] = []
    node = _FakeNode()
    ctx = SimpleNamespace()

    def _handler(stream: object, node: object, idx: int, ctx: object) -> object:
        seen.append((stream, node, idx, ctx))
        return "wired"

    monkeypatch.setattr(_node_handlers, "_NODE_HANDLERS", {_FakeNode: _handler})

    result = _node_handlers._wire_node("input-stream", node, 7, ctx)

    assert result == "wired"
    assert seen == [("input-stream", node, 7, ctx)]


def test_apply_collect_batch_default_wires_timeout_and_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, tuple[object, ...]] = {}
    node = CollectBatch(max_records=3, timeout_ms=250)

    def _key_on(step_id: str, stream: object, fn: object) -> object:
        calls["key_on"] = (step_id, stream, fn)
        return "keyed-stream"

    def _collect(
        step_id: str,
        keyed: object,
        *,
        timeout: timedelta,
        max_size: int,
    ) -> object:
        calls["collect"] = (step_id, keyed, timeout, max_size)
        return "collected-stream"

    def _key_rm(step_id: str, collected: object) -> object:
        calls["key_rm"] = (step_id, collected)
        return "result-stream"

    monkeypatch.setattr(_node_handlers, "key_on", _key_on)
    monkeypatch.setattr(_node_handlers, "collect", _collect)
    monkeypatch.setattr(_node_handlers, "key_rm", _key_rm)

    result = _node_handlers._apply_collect_batch_default("input-stream", node, "step-1")

    assert result == "result-stream"
    assert calls["key_on"] == ("collect_key_step-1", "input-stream", _node_handlers._batch_key)
    assert calls["collect"] == (
        "collect_step-1",
        "keyed-stream",
        timedelta(milliseconds=250),
        3,
    )
    assert calls["key_rm"] == ("collect_unkey_step-1", "collected-stream")


def test_execute_record_step_replaces_payload_and_observes() -> None:
    observer = _RecordingObserver()
    result = _node_handlers._execute_record_step(
        observer,
        "orders",
        0,
        "Upper",
        _UpperRecordStep(),
        _message("abc"),
    )

    payload = _message_payload(result)
    assert payload.value == "ABC"
    assert observer.events == [
        ("start", "orders", 0, "Upper"),
        ("end", "orders", 0, "Upper:success"),
    ]


def test_execute_record_step_propagates_business_error_and_observes() -> None:
    observer = _RecordingObserver()
    with pytest.raises(RuleViolation, match="value: boom"):
        _node_handlers._execute_record_step(
            observer,
            "orders",
            1,
            "Boom",
            _BoomRecordStep(),
            _message("abc"),
        )

    assert observer.events == [
        ("start", "orders", 1, "Boom"),
        ("error", "orders", 1, "Boom:RuleViolation"),
    ]


def test_execute_batch_step_replaces_payloads_and_observes() -> None:
    observer = _RecordingObserver()
    result = _node_handlers._execute_batch_step(
        observer,
        "orders",
        2,
        "UpperBatch",
        _UpperBatchStep(),
        [_message("a"), _message("b")],
    )

    assert [_message_payload(item).value for item in result] == ["A", "B"]
    assert observer.events == [
        ("start", "orders", 2, "UpperBatch"),
        ("end", "orders", 2, "UpperBatch:success"),
    ]


def test_execute_expand_step_replaces_payloads_and_observes() -> None:
    observer = _RecordingObserver()
    result = _node_handlers._execute_expand_step(
        observer,
        "orders",
        3,
        "UpperExpand",
        _UpperExpandStep(),
        _message("ab"),
    )

    assert [_message_payload(item).value for item in result] == ["AB", "ab"]
    assert observer.events == [
        ("start", "orders", 3, "UpperExpand"),
        ("end", "orders", 3, "UpperExpand:success"),
    ]


def test_execute_batch_expand_step_replaces_payloads_and_observes() -> None:
    observer = _RecordingObserver()
    result = _node_handlers._execute_batch_expand_step(
        observer,
        "orders",
        4,
        "UpperBatchExpand",
        _UpperBatchExpandStep(),
        [_message("a"), _message("b")],
    )

    assert [_message_payload(item).value for item in result] == ["A", "B"]
    assert observer.events == [
        ("start", "orders", 4, "UpperBatchExpand"),
        ("end", "orders", 4, "UpperBatchExpand:success"),
    ]


@pytest.mark.parametrize(
    ("node", "expected_value"),
    [
        (_UpperRecordStep(), "ABC"),
        (_UpperBatchStep(), "ABC"),
        (IntoTopic("orders.out", payload=_Payload), "abc"),
        (Drain(), "abc"),
    ],
)
def test_execute_router_node_supports_allowed_branch_nodes(
    node: object,
    expected_value: str,
) -> None:
    result = _node_handlers._execute_router_node(node, _message("abc"))

    assert _message_payload(result).value == expected_value


def _message_payload(message: Message[StreamPayload]) -> _Payload:
    return cast(_Payload, message.payload)
