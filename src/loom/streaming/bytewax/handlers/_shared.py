"""Shared primitives for Bytewax handler families."""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Awaitable, Iterable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from typing import Any, Protocol, TypeAlias, TypeGuard, TypeVar, cast, runtime_checkable

from structlog.contextvars import bind_contextvars, reset_contextvars

from loom.core.async_bridge import AsyncBridge
from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.core.observability.event import EventKind, LifecycleEvent, LifecycleStatus, Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.streaming.bytewax._commit_tracker import CommitCompletionPort
from loom.streaming.bytewax._operators import ResourceLifecycle
from loom.streaming.compiler._plan import CompiledPlan
from loom.streaming.core._errors import ErrorEnvelope, ErrorKind
from loom.streaming.core._message import Message
from loom.streaming.core._typing import StreamPayload
from loom.streaming.nodes._table.common import SqlAlchemyDatabaseConfig

Stream: TypeAlias = Any
AwaitT = TypeVar("AwaitT")


@runtime_checkable
class _ExecutableRecordStep(Protocol):
    """Runtime-executable record-shaped step."""

    def execute(
        self,
        message: Message[StreamPayload],
        **kwargs: object,
    ) -> StreamPayload | Message[StreamPayload] | Awaitable[StreamPayload | Message[StreamPayload]]:
        """Execute one record-shaped message or replacement message."""
        ...


@runtime_checkable
class _ExecutableBatchStep(Protocol):
    """Runtime-executable batch-shaped step."""

    def execute(
        self,
        messages: list[Message[StreamPayload]],
        **kwargs: object,
    ) -> (
        list[StreamPayload | Message[StreamPayload]]
        | Awaitable[list[StreamPayload | Message[StreamPayload]]]
    ):
        """Execute one batch-shaped message group or replacement messages."""
        ...


@runtime_checkable
class _ExecutableExpandStep(Protocol):
    """Runtime-executable expanding step."""

    def execute(
        self,
        message: Message[StreamPayload],
        **kwargs: object,
    ) -> (
        Iterable[StreamPayload | Message[StreamPayload]]
        | Awaitable[Iterable[StreamPayload | Message[StreamPayload]]]
    ):
        """Expand one message into many payloads or replacement messages."""
        ...


@runtime_checkable
class _ExecutableBatchExpandStep(Protocol):
    """Runtime-executable batch-expanding step."""

    def execute(
        self,
        messages: list[Message[StreamPayload]],
        **kwargs: object,
    ) -> (
        Iterable[StreamPayload | Message[StreamPayload]]
        | Awaitable[Iterable[StreamPayload | Message[StreamPayload]]]
    ):
        """Expand one batch into many payloads or replacement messages."""
        ...


class _WithProcessNode(Protocol):
    """Node that carries an inner process."""

    process: Any


class _BuildContextProtocol(Protocol):
    """Adapter build context required by node handlers."""

    plan: CompiledPlan
    bridge: AsyncBridge | None
    commit_tracker: CommitCompletionPort | None
    flow_runtime: ObservabilityRuntime

    def wire_terminal(self, step_id: str, stream: Stream) -> None:
        """Wire one terminal output branch."""

    def wire_branch_terminal(self, step_id: str, stream: Stream, path: tuple[int, ...]) -> None:
        """Wire one branch terminal output branch."""

    def wire_node_error(self, kind: ErrorKind, step_id: str, stream: Stream) -> None:
        """Wire one node error branch."""

    def wire_flow_output(self, stream: Stream, plan: CompiledPlan) -> None:
        """Wire flow-level outputs after the process completes."""

    def wire_decode_error(self, stream: Stream, plan: CompiledPlan) -> None:
        """Wire source decode errors."""

    @property
    def current_path(self) -> tuple[int, ...]:
        """Return the current wiring path inside the process tree."""
        ...

    def inline_sink_partition_for(
        self,
        path: tuple[int, ...],
    ) -> Any:
        """Return a ready-to-write sink partition for an inline (non-graph) write."""
        ...

    def session_manager_for(
        self,
        config: SqlAlchemyDatabaseConfig | Mapping[str, Any],
    ) -> SessionManager:
        """Return a shared SQLAlchemy session manager for one sink config."""
        ...

    def manager_for(
        self,
        idx: int,
        node: Any,
    ) -> ResourceLifecycle:
        """Return the resource manager for one scoped node."""
        ...

    def enter_path(self, path: tuple[int, ...]) -> AbstractContextManager[None]:
        """Temporarily set the current compilation path."""
        ...

    def wire_process(
        self,
        stream: Stream,
        nodes: tuple[object, ...],
        *,
        path_prefix: tuple[int, ...] = (),
    ) -> Stream:
        """Wire one nested process subtree."""


def _step_id(base: str, ctx: _BuildContextProtocol) -> str:
    """Build a Bytewax step ID qualified with the current wiring path."""
    path = ctx.current_path
    if not path:
        return base
    return "_".join(map(str, path)) + "_" + base


def _resolve_node_name(raw: object) -> str:
    """Resolve a human-readable name for a DSL node."""
    step_name = getattr(type(raw), "step_name", None)
    if callable(step_name):
        try:
            return cast(str, step_name())
        except TypeError:
            pass
    return type(raw).__name__


@contextmanager
def _observe_node(
    observer: ObservabilityRuntime,
    flow_name: str,
    idx: int,
    node_type: str,
    trace_id: str | None = None,
    correlation_id: str | None = None,
) -> Iterator[None]:
    """Emit observability events around one node execution."""
    observer.emit(
        LifecycleEvent(
            scope=Scope.NODE,
            name=f"{flow_name}:{idx}",
            kind=EventKind.START,
            trace_id=trace_id,
            correlation_id=correlation_id,
            meta={"flow": flow_name, "node_idx": idx, "node_type": node_type},
        )
    )
    t0 = time.monotonic()
    success = False
    context_tokens = bind_contextvars(
        flow_name=flow_name,
        node_idx=idx,
        node_type=node_type,
        method="execute",
    )
    try:
        yield
        success = True
    except Exception as exc:
        observer.emit(
            LifecycleEvent(
                scope=Scope.NODE,
                name=f"{flow_name}:{idx}",
                kind=EventKind.ERROR,
                trace_id=trace_id,
                correlation_id=correlation_id,
                error=repr(exc),
                meta={"flow": flow_name, "node_idx": idx, "node_type": node_type},
            )
        )
        raise
    finally:
        reset_contextvars(**context_tokens)
        if success:
            elapsed = int((time.monotonic() - t0) * 1000)
            observer.emit(
                LifecycleEvent(
                    scope=Scope.NODE,
                    name=f"{flow_name}:{idx}",
                    kind=EventKind.END,
                    trace_id=trace_id,
                    correlation_id=correlation_id,
                    duration_ms=elapsed,
                    status=LifecycleStatus.SUCCESS,
                    meta={"flow": flow_name, "node_idx": idx, "node_type": node_type},
                )
            )


def _resolve_record_result(
    result: StreamPayload
    | Message[StreamPayload]
    | Awaitable[StreamPayload | Message[StreamPayload]],
    node_type: str,
) -> StreamPayload | Message[StreamPayload]:
    """Resolve a synchronous record-shaped result and reject awaitables."""
    if isinstance(result, Awaitable):
        raise TypeError(f"{node_type} returned an awaitable outside WithAsync.")
    return result


def _resolve_batch_result(
    result: list[StreamPayload | Message[StreamPayload]]
    | Awaitable[list[StreamPayload | Message[StreamPayload]]],
    node_type: str,
) -> list[StreamPayload | Message[StreamPayload]]:
    """Resolve a synchronous batch-shaped result and reject awaitables."""
    if isinstance(result, Awaitable):
        raise TypeError(f"{node_type} returned an awaitable outside WithAsync.")
    return result


def _resolve_expand_result(
    result: Iterable[StreamPayload | Message[StreamPayload]]
    | Awaitable[Iterable[StreamPayload | Message[StreamPayload]]],
    node_type: str,
) -> Iterable[StreamPayload | Message[StreamPayload]]:
    """Resolve a synchronous expanding result and reject awaitables."""
    if isinstance(result, Awaitable):
        raise TypeError(f"{node_type} returned an awaitable outside WithAsync.")
    return result


async def _resolve_async_result(
    result: StreamPayload | Awaitable[StreamPayload],
    timeout_ms: int | None,
) -> StreamPayload:
    """Resolve a step result for async execution."""
    if isinstance(result, Awaitable):
        return await _await_with_optional_timeout(result, timeout_ms)
    return result


def _messages_from_batch(batch: list[Any]) -> list[Message[StreamPayload]]:
    """Coerce one batch of runtime values into DSL messages."""
    return [_require_message(item) for item in batch]


def _require_message(value: Any) -> Message[StreamPayload]:
    """Validate that the runtime stream carries Loom messages."""
    if not _is_message(value):
        raise TypeError(f"Expected Message, got {type(value).__name__}.")
    return value


def _is_message(value: object) -> TypeGuard[Message[StreamPayload]]:
    """Return whether one runtime item is a Loom message."""
    return isinstance(value, Message)


def _replace_payload(message: Message[StreamPayload], payload: Any) -> Message[StreamPayload]:
    """Preserve metadata while replacing the logical payload."""
    if isinstance(payload, Message):
        return cast(Message[StreamPayload], payload)
    if not isinstance(payload, (LoomStruct, LoomFrozenStruct)):
        raise TypeError(f"Expected StreamPayload, got {type(payload).__name__}.")
    return Message(payload=payload, meta=message.meta)


def _replace_payloads(
    messages: list[Message[StreamPayload]],
    payloads: list[Any],
) -> list[Message[StreamPayload]]:
    """Preserve per-record metadata for batch task outputs."""
    if len(messages) != len(payloads):
        raise RuntimeError("Batch task output length must match input length.")
    return [
        _replace_payload(message, payload)
        for message, payload in zip(messages, payloads, strict=True)
    ]


def _empty(_item: Any) -> tuple[()]:
    """Drop one item from a stream."""
    return ()


def _commit_key(item: Any) -> tuple[str, int, int] | None:
    """Resolve the source offset one runtime item is accountable for.

    Both a produced ``Message`` and an ``ErrorEnvelope`` carry the offset of
    the record they came from — the envelope through its original snapshot —
    because both eventually reach a terminal that completes that offset.
    """
    if _is_message(item):
        return _offset_triple(item.meta.topic, item.meta.partition, item.meta.offset)
    if isinstance(item, ErrorEnvelope):
        original = item.original_message
        if original is None:
            return None
        return _offset_triple(original.meta.topic, original.meta.partition, original.meta.offset)
    return None


def _offset_triple(
    topic: str | None, partition: int | None, offset: int | None
) -> tuple[str, int, int] | None:
    """Return a complete offset triple, or ``None`` when any part is missing."""
    if topic is None or partition is None or offset is None:
        return None
    return topic, partition, offset


def _reconcile_fanout(
    originals: Sequence[Any],
    results: Sequence[Any],
    tracker: CommitCompletionPort | None,
) -> None:
    """Align commit accounting with the fan-out a node actually produced.

    An expanding node turns one input record into N outputs that all carry the
    same source offset, and every one of them completes that offset when it
    reaches a terminal. The tracker starts each record expecting a single
    completion, so accounting has to be corrected here — while the outputs are
    still a plain list and none of them can have reached a terminal yet.

    Two failures this prevents, both silent:

    - ``N > 1`` without a fork: the first completion releases the offset while
      N-1 outputs are still in flight, so a crash loses them for good — the
      consumer group has already moved past them.
    - ``N == 0`` without a completion: nothing ever completes that offset, so
      the partition's watermark never advances again.

    Args:
        originals: Input records handed to the node.
        results: Everything the node produced, successes and error envelopes.
        tracker: Completion port, or ``None`` under at-most-once delivery.
    """
    if tracker is None:
        return
    produced: Counter[tuple[str, int, int]] = Counter()
    for item in results:
        key = _commit_key(item)
        if key is not None:
            produced[key] += 1
    for key, count in produced.items():
        if count > 1:
            tracker.fork(key[0], key[1], key[2], count - 1)
    for original in originals:
        key = _commit_key(original)
        if key is not None and key not in produced:
            tracker.complete(key[0], key[1], key[2])


def _drop_and_commit(item: Any, tracker: CommitCompletionPort) -> tuple[()]:
    """Drop one item and mark it complete for commit tracking."""
    if not _is_message(item):
        return ()
    t, p, o = item.meta.topic, item.meta.partition, item.meta.offset
    if t is not None and p is not None and o is not None:
        tracker.complete(t, p, o)
    return ()


def _identity(items: Any) -> Any:
    """Pass through one item unchanged for flat_map."""
    return items


def _register_row_fanout(
    item: Any,
    tracker: CommitCompletionPort,
    declared_types: frozenset[type],
    has_default: bool,
) -> Any:
    """Account for the rows an ``ExpandRoutes`` node actually produced.

    The expanded payload maps each output type to its rows. Every row becomes
    one message on its route and completes the source offset at its terminal,
    so the expected completions must match the total row count — which is
    unrelated to how many routes were declared. Zero rows completes the offset
    here, since nothing downstream ever will.
    """
    message = _require_message(item)
    expanded = cast(dict[type, list[Any]], message.payload)
    total = sum(len(expanded.get(output_type) or []) for output_type in declared_types)
    if has_default:
        total += sum(
            len(rows) for output_type, rows in expanded.items() if output_type not in declared_types
        )
    key = _commit_key(message)
    if key is None:
        return message
    if total == 0:
        tracker.complete(key[0], key[1], key[2])
    elif total > 1:
        tracker.fork(key[0], key[1], key[2], total - 1)
    return message


def _register_broadcast_fanout(item: Any, tracker: CommitCompletionPort, route_count: int) -> Any:
    """Increase pending completions for a broadcast fan-out item."""
    if route_count <= 1:
        return item
    message = _require_message(item)
    t, p, o = message.meta.topic, message.meta.partition, message.meta.offset
    if t is not None and p is not None and o is not None:
        tracker.fork(t, p, o, route_count - 1)
    return message


@contextmanager
def _batch_dependencies(
    manager: ResourceLifecycle,
    worker_resources: Mapping[str, object],
) -> Iterator[dict[str, object]]:
    """Open and close one batch-scoped dependency set."""
    batch_resources = manager.open_batch()
    try:
        yield {**worker_resources, **batch_resources}
    finally:
        manager.close_batch()


async def _await_with_optional_timeout(
    awaitable: Awaitable[AwaitT],
    timeout_ms: int | None,
) -> AwaitT:
    """Await *awaitable*, optionally bounded by *timeout_ms* milliseconds."""
    if timeout_ms is None:
        return await awaitable
    import anyio

    with anyio.fail_after(timeout_ms / 1000):
        return await awaitable
