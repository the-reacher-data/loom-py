"""Tests for Bytewax node error boundary helpers."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loom.core.errors.errors import RuleViolation
from loom.core.model import LoomStruct
from loom.streaming.bytewax import _error_boundary
from loom.streaming.core._errors import ErrorEnvelope, ErrorKind
from loom.streaming.core._message import Message, MessageMeta

pytestmark = pytest.mark.bytewax


class _Payload(LoomStruct):
    value: str


class _OutputWire:
    def __init__(self) -> None:
        self.calls: list[tuple[ErrorKind, str, object]] = []

    def wire_node_error(self, kind: ErrorKind, step_id: str, stream: object) -> None:
        self.calls.append((kind, step_id, stream))


class _Context:
    def __init__(self) -> None:
        self.outputs = _OutputWire()


def _message(value: str = "v") -> Message[_Payload]:
    return Message(payload=_Payload(value=value), meta=MessageMeta(message_id="m-1"))


def test_execute_in_boundary_returns_message_on_success() -> None:
    result = _error_boundary._execute_in_boundary(
        _error_boundary._classify_task,
        _message(),
        lambda: _message("ok"),
    )

    assert isinstance(result, Message)
    payload = cast(_Payload, result.payload)
    assert payload.value == "ok"


def test_execute_in_boundary_maps_domain_error_to_business() -> None:
    result = _error_boundary._execute_in_boundary(
        _error_boundary._classify_task,
        _message(),
        lambda: (_ for _ in ()).throw(RuleViolation("field", "boom")),
    )

    assert isinstance(result, ErrorEnvelope)
    assert result.kind is ErrorKind.BUSINESS
    assert result.reason == "field: boom"
    original = result.original_message
    assert original is not None
    payload = cast(_Payload, original.payload)
    assert payload.value == "v"


def test_execute_batch_in_boundary_returns_per_message_envelopes_on_failure() -> None:
    originals = [_message("a"), _message("b")]

    result = _error_boundary._execute_batch_in_boundary(
        _error_boundary._classify_routing,
        originals,
        lambda: (_ for _ in ()).throw(RuntimeError("routing-boom")),
    )

    envelopes = [item for item in result if isinstance(item, ErrorEnvelope)]

    assert len(result) == 2
    assert len(envelopes) == 2
    assert [item.original_message for item in envelopes] == originals
    assert all(item.kind is ErrorKind.ROUTING for item in envelopes)


def test_split_node_result_wires_error_branch_and_returns_success_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _Context()
    split = SimpleNamespace(trues="ok-stream", falses="error-stream")

    def _branch(step_id: str, stream: object, predicate: object) -> object:
        assert step_id == "node_1_split"
        assert stream == "input-stream"
        assert predicate is _error_boundary._is_message
        return split

    monkeypatch.setattr(_error_boundary, "branch", _branch)

    result = _error_boundary._split_node_result(
        "input-stream",
        "node_1",
        ctx,
        ErrorKind.TASK,
    )

    assert result == "ok-stream"
    assert ctx.outputs.calls == [(ErrorKind.TASK, "node_1", "error-stream")]


def test_split_batch_node_result_flattens_error_branch_and_wires_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _Context()
    split = SimpleNamespace(trues="ok-stream", falses=[["error-a"], ["error-b"]])

    def _branch(step_id: str, stream: object, predicate: object) -> object:
        assert step_id == "node_2_split"
        assert stream == "batch-stream"
        assert predicate is _error_boundary._is_message_batch
        return split

    def _flat_map(step_id: str, stream: object, fn: object) -> object:
        assert step_id == "node_2_errors"
        assert stream == split.falses
        return "error-stream"

    monkeypatch.setattr(_error_boundary, "branch", _branch)
    monkeypatch.setattr(_error_boundary, "flat_map", _flat_map)

    result = _error_boundary._split_batch_node_result(
        "batch-stream",
        "node_2",
        ctx,
        ErrorKind.ROUTING,
    )

    assert result == "ok-stream"
    assert ctx.outputs.calls == [(ErrorKind.ROUTING, "node_2", "error-stream")]
