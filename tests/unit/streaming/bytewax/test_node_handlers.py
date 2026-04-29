"""Direct tests for Bytewax node handler helpers."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from loom.core.model import LoomStruct
from loom.streaming.bytewax import _node_handlers
from loom.streaming.core._message import Message, MessageMeta

pytestmark = pytest.mark.bytewax


class _Payload(LoomStruct):
    value: str


class _FakeNode:
    pass


def _message(value: str = "v") -> Message[_Payload]:
    return Message(payload=_Payload(value=value), meta=MessageMeta(message_id="m-1"))


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
