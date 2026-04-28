"""Integration tests for With / WithAsync context-manager adapters."""

from __future__ import annotations

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
from loom.streaming.graph._flow import Process
from loom.streaming.nodes._boundary import IntoTopic


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


class _SyncStep(RecordStep[_Payload, _Result]):
    def execute(self, message: Message[_Payload], *, client: _FakeSyncClient) -> _Result:
        return _Result(value=client.process(message.payload.value))


def _make_process() -> Process[_Payload, _Result]:
    return Process(IntoTopic("results", payload=_Result))


def test_with_async_detects_async_context_managers() -> None:
    """WithAsync classifies async CMs into async_contexts."""
    client = _FakeAsyncClient()
    adapter = WithAsync(process=_make_process(), client=client, max_concurrency=5)

    assert adapter.async_contexts == {"client": client}
    assert adapter.plain_deps == {}
    assert adapter.max_concurrency == 5


def test_with_executes_batch_under_open_context_manager() -> None:
    """With detects the sync CM, opens it, and the task receives the injected client."""
    client = _FakeSyncClient()
    step = _SyncStep()
    adapter = With(step=step, client=client)

    assert adapter.sync_contexts == {"client": client}
    assert adapter.plain_deps == {}

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
        process=_make_process(),
        client=async_cm,
        validator="plain",
        max_concurrency=3,
    )

    assert adapter.async_contexts == {"client": async_cm}
    assert adapter.plain_deps == {"validator": "plain"}
    assert adapter.max_concurrency == 3


def test_with_async_rejects_non_positive_max_concurrency() -> None:
    with pytest.raises(ValueError, match="max_concurrency"):
        WithAsync(process=_make_process(), max_concurrency=0)


def test_with_async_defaults_task_timeout_to_none() -> None:
    adapter = WithAsync(process=_make_process())

    assert adapter.task_timeout_ms is None


def test_with_async_accepts_positive_task_timeout() -> None:
    adapter = WithAsync(process=_make_process(), task_timeout_ms=500)

    assert adapter.task_timeout_ms == 500


def test_with_async_rejects_zero_task_timeout() -> None:
    with pytest.raises(ValueError, match="task_timeout_ms must be greater than zero"):
        WithAsync(process=_make_process(), task_timeout_ms=0)


def test_with_async_rejects_negative_task_timeout() -> None:
    with pytest.raises(ValueError, match="task_timeout_ms must be greater than zero"):
        WithAsync(process=_make_process(), task_timeout_ms=-100)


def test_with_rejects_async_context_manager() -> None:
    step = _SyncStep()

    with pytest.raises(TypeError, match="sync context managers"):
        With(step=step, client=_FakeAsyncClient())


def test_with_async_rejects_sync_context_manager() -> None:
    with pytest.raises(TypeError, match="async context managers"):
        WithAsync(process=_make_process(), client=_FakeSyncClient())


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
