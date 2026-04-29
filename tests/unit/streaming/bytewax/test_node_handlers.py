"""Direct tests for Bytewax node handler helpers."""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

import pytest

from loom.core.model import LoomStruct
from loom.streaming.bytewax import _node_handlers
from loom.streaming.core._message import Message, MessageMeta
from loom.streaming.nodes._shape import CollectBatch

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
