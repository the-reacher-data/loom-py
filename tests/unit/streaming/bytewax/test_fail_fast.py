"""Regression tests for fail-fast async batch execution."""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any, cast

import anyio
import pytest

pytest.importorskip("bytewax")

from loom.core.model import LoomStruct
from loom.streaming import Message, MessageMeta, WithAsync
from loom.streaming.bytewax import _node_handlers
from loom.streaming.bytewax._node_handlers import _build_fail_fast_step


class _Order(LoomStruct):
    order_id: str


class _ValidatedOrder(LoomStruct):
    order_id: str


class _EchoAsyncStep:
    async def execute(self, message: Message[_Order], **kwargs: object) -> _ValidatedOrder:
        del kwargs
        return _ValidatedOrder(order_id=message.payload.order_id)


class _FakeManager:
    def open_batch(self) -> dict[str, object]:
        return {}

    def close_batch(self) -> None:
        return None


class _FakeBridge:
    def run(
        self,
        batch_result: Coroutine[Any, Any, list[Message[_ValidatedOrder]]],
    ) -> list[Message[_ValidatedOrder]]:
        return asyncio.run(batch_result)


def _message(order_id: str) -> Message[_Order]:
    return Message(payload=_Order(order_id=order_id), meta=MessageMeta(message_id=order_id))


def test_fail_fast_parses_batch_once(monkeypatch: pytest.MonkeyPatch) -> None:
    node = WithAsync(step=cast(Any, _EchoAsyncStep()), max_concurrency=2, error_mode="fail_fast")
    manager = cast(Any, _FakeManager())
    bridge = cast(Any, _FakeBridge())
    sem = anyio.Semaphore(2)
    calls = 0
    original = _node_handlers._messages_from_batch

    def counting(batch: list[Any]) -> list[Message[Any]]:
        nonlocal calls
        calls += 1
        return original(batch)

    monkeypatch.setattr(_node_handlers, "_messages_from_batch", counting)

    step = _build_fail_fast_step(
        node=node,
        manager=manager,
        worker_resources={},
        bridge=bridge,
        observer=None,
        flow_name="flow",
        idx=0,
        node_type="WithAsync",
        sem=sem,
    )

    result = step([_message("o-1"), _message("o-2")])

    assert calls == 1
    assert [
        cast(Message[_ValidatedOrder], item).payload.order_id
        for item in result
        if isinstance(item, Message)
    ] == ["o-1", "o-2"]
