"""Integration tests for With / WithAsync context-manager adapters."""

from __future__ import annotations

import asyncio

import pytest

from loom.core.model import LoomStruct
from loom.streaming import (
    ContextFactory,
    Message,
    MessageMeta,
    RecordStep,
    With,
    WithAsync,
)


class _Payload(LoomStruct):
    value: str


class _Result(LoomStruct):
    value: str


class _FakeAsyncClient:
    """Async context manager that tracks open/close lifecycle."""

    def __init__(self) -> None:
        self.opened = False
        self.closed = False

    async def __aenter__(self) -> _FakeAsyncClient:
        self.opened = True
        return self

    async def __aexit__(self, *args: object) -> None:
        self.closed = True

    async def process(self, value: str) -> str:
        await asyncio.sleep(0)  # yield to event loop
        return f"async:{value}"


class _FakeSyncClient:
    """Sync context manager that tracks open/close lifecycle."""

    def __init__(self) -> None:
        self.opened = False
        self.closed = False

    def __enter__(self) -> _FakeSyncClient:
        self.opened = True
        return self

    def __exit__(self, *args: object) -> None:
        self.closed = True

    def process(self, value: str) -> str:
        return f"sync:{value}"


class _AsyncStep(RecordStep[_Payload, _Result]):
    async def execute(self, message: Message[_Payload], *, client: _FakeAsyncClient) -> _Result:
        processed = await client.process(message.payload.value)
        return _Result(value=processed)


class _SyncStep(RecordStep[_Payload, _Result]):
    def execute(self, message: Message[_Payload], *, client: _FakeSyncClient) -> _Result:
        return _Result(value=client.process(message.payload.value))


@pytest.mark.asyncio
async def test_with_async_executes_batch_under_open_context_manager() -> None:
    """WithAsync detects the CM, opens it, and the task receives the injected client."""
    client = _FakeAsyncClient()
    step = _AsyncStep()
    adapter = WithAsync(step=step, client=client, max_concurrency=5)

    # Verify detection
    assert adapter.async_contexts == {"client": client}
    assert adapter.plain_deps == {}
    assert adapter.max_concurrency == 5

    # Simulate what the runtime adapter does: open CM + gather
    async with client:
        results = await asyncio.gather(
            *[
                step.execute(msg, client=client)
                for msg in [
                    Message(payload=_Payload(value="a"), meta=MessageMeta(message_id="m1")),
                    Message(payload=_Payload(value="b"), meta=MessageMeta(message_id="m2")),
                ]
            ]
        )

    assert client.opened is True
    assert client.closed is True
    assert results == [_Result(value="async:a"), _Result(value="async:b")]


def test_with_executes_batch_under_open_context_manager() -> None:
    """With detects the sync CM, opens it, and the task receives the injected client."""
    client = _FakeSyncClient()
    step = _SyncStep()
    adapter = With(step=step, client=client)

    # Verify detection
    assert adapter.sync_contexts == {"client": client}
    assert adapter.plain_deps == {}

    # Simulate what the runtime adapter does: open CM + sequential execution
    with client:
        results = [
            step.execute(msg, client=client)
            for msg in [
                Message(payload=_Payload(value="a"), meta=MessageMeta(message_id="m1")),
                Message(payload=_Payload(value="b"), meta=MessageMeta(message_id="m2")),
            ]
        ]

    assert client.opened is True
    assert client.closed is True
    assert results == [_Result(value="sync:a"), _Result(value="sync:b")]


def test_with_async_detects_mixed_dependencies() -> None:
    """WithAsync separates async CMs from plain deps."""
    async_cm = _FakeAsyncClient()
    adapter = WithAsync(
        step=_AsyncStep(),
        client=async_cm,
        validator="plain",
        max_concurrency=3,
    )

    assert adapter.async_contexts == {"client": async_cm}
    assert adapter.plain_deps == {"validator": "plain"}
    assert adapter.max_concurrency == 3


def test_with_async_rejects_non_positive_max_concurrency() -> None:
    step = _AsyncStep()

    with pytest.raises(ValueError, match="max_concurrency"):
        WithAsync(step=step, max_concurrency=0)


def test_with_rejects_async_context_manager() -> None:
    step = _SyncStep()

    with pytest.raises(TypeError, match="sync context managers"):
        With(step=step, client=_FakeAsyncClient())


def test_with_async_rejects_sync_context_manager() -> None:
    step = _AsyncStep()

    with pytest.raises(TypeError, match="async context managers"):
        WithAsync(step=step, client=_FakeSyncClient())


def test_with_keeps_plain_dependencies() -> None:
    step = _SyncStep()
    adapter = With(step=step, validator="plain", retries=3)

    assert adapter.sync_contexts == {}
    assert adapter.plain_deps == {"validator": "plain", "retries": 3}


def test_context_factory_detected_as_factory() -> None:
    """ContextFactory is stored in context_factories, not contexts or plain_deps."""
    factory = ContextFactory(lambda: _FakeSyncClient())
    adapter = With(step=_SyncStep(), db=factory)

    assert adapter.context_factories == {"db": factory}
    assert adapter.sync_contexts == {}
    assert adapter.plain_deps == {}


def test_context_factory_creates_fresh_instance() -> None:
    """Each call to create() returns a new instance."""
    factory = ContextFactory(lambda: _FakeSyncClient())
    a = factory.create()
    b = factory.create()

    assert a is not b
    assert isinstance(a, _FakeSyncClient)
    assert isinstance(b, _FakeSyncClient)
