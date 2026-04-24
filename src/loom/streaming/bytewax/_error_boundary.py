"""Error boundary helpers for Bytewax node execution."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol, TypeAlias, cast

from bytewax.operators import branch, flat_map

from loom.streaming.core._errors import ErrorEnvelope, ErrorKind
from loom.streaming.core._message import Message
from loom.streaming.core._typing import StreamPayload

Stream: TypeAlias = Any
NodeResult: TypeAlias = Message[StreamPayload] | ErrorEnvelope[StreamPayload]


class _ErrorWireOutputs(Protocol):
    """Wire node error branches to configured sinks."""

    def wire_node_error(self, kind: ErrorKind, step_id: str, stream: Stream) -> None:
        """Wire one error branch to an output sink."""


class _ErrorSplitContext(Protocol):
    """Context carrying error-output wiring for node boundaries."""

    outputs: Any


def _classify_task(exc: Exception) -> ErrorKind:
    """Classify one execution failure as a task error."""
    del exc
    return ErrorKind.TASK


def _classify_routing(exc: Exception) -> ErrorKind:
    """Classify one execution failure as a routing error."""
    del exc
    return ErrorKind.ROUTING


def _execute_in_boundary(
    classify: Callable[[Exception], ErrorKind],
    original: Message[StreamPayload],
    fn: Callable[[], Message[StreamPayload]],
) -> NodeResult:
    """Execute one message node and capture failures as error envelopes."""
    try:
        return fn()
    except Exception as exc:
        return ErrorEnvelope(
            kind=classify(exc),
            reason=str(exc),
            original_message=original,
        )


def _execute_batch_in_boundary(
    classify: Callable[[Exception], ErrorKind],
    originals: list[Message[StreamPayload]],
    fn: Callable[[], list[Message[StreamPayload]]],
) -> list[NodeResult]:
    """Execute one batch node and capture failures as per-message envelopes."""
    try:
        return cast(list[NodeResult], fn())
    except Exception as exc:
        kind = classify(exc)
        return [
            ErrorEnvelope(kind=kind, reason=str(exc), original_message=message)
            for message in originals
        ]


def _split_node_result(
    stream: Stream,
    step_id: str,
    ctx: _ErrorSplitContext,
    kind: ErrorKind,
) -> Stream:
    """Split node results into success and error branches and wire errors."""
    split = branch(f"{step_id}_split", stream, _is_message)
    wire_outputs = cast(_ErrorWireOutputs, ctx.outputs)
    wire_outputs.wire_node_error(kind, step_id, split.falses)
    return split.trues


def _split_batch_node_result(
    stream: Stream,
    step_id: str,
    ctx: _ErrorSplitContext,
    kind: ErrorKind,
) -> Stream:
    """Split batch-shaped node results into success and error branches."""
    split = branch(f"{step_id}_split", stream, _is_message_batch)
    errors = flat_map(f"{step_id}_errors", split.falses, _identity)
    wire_outputs = cast(_ErrorWireOutputs, ctx.outputs)
    wire_outputs.wire_node_error(kind, step_id, errors)
    return split.trues


def _is_message(item: Any) -> bool:
    """Return whether one item is a normal message result."""
    return isinstance(item, Message)


def _is_message_batch(item: Any) -> bool:
    """Return whether one batch-shaped result contains normal messages."""
    if not isinstance(item, list):
        return False
    if not item:
        return True
    return isinstance(item[0], Message)


def _identity(items: Any) -> Any:
    """Pass through an item unchanged for flat_map."""
    return items
