"""Error boundary helpers for Bytewax node execution."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, Protocol, TypeAlias

from bytewax.operators import branch, flat_map

from loom.core.errors.errors import DomainError
from loom.core.logger import get_logger
from loom.streaming.core._errors import ErrorEnvelope, ErrorKind, snapshot_message
from loom.streaming.core._message import Message
from loom.streaming.core._typing import StreamPayload

Stream: TypeAlias = Any
NodeResult: TypeAlias = Message[StreamPayload] | ErrorEnvelope[StreamPayload]
logger = get_logger(__name__)


class _ErrorWireOutputs(Protocol):
    """Wire node error branches to configured sinks."""

    def wire_node_error(self, kind: ErrorKind, step_id: str, stream: Stream) -> None:
        """Wire one error branch to an output sink."""


class _ErrorSplitContext(Protocol):
    """Context carrying error-output wiring for node boundaries."""

    outputs: Any


def _classify_task(exc: Exception) -> ErrorKind:
    """Classify one execution failure as a task error."""
    if isinstance(exc, DomainError):
        return ErrorKind.BUSINESS
    return ErrorKind.TASK


def _classify_routing(exc: Exception) -> ErrorKind:
    """Classify one execution failure as a routing error."""
    del exc
    return ErrorKind.ROUTING


def _build_error_envelope(
    kind: ErrorKind,
    reason: str,
    original: Message[StreamPayload],
) -> ErrorEnvelope[StreamPayload]:
    snapshot = snapshot_message(original)
    payload_type = original.payload.__class__.loom_message_type()
    return ErrorEnvelope(
        kind=kind,
        reason=reason,
        payload_type=payload_type,
        original_message=snapshot,
    )


def _execute_in_boundary(
    classify: Callable[[Exception], ErrorKind],
    original: Message[StreamPayload],
    fn: Callable[[], Message[StreamPayload]],
) -> NodeResult:
    """Execute one message node and capture failures as error envelopes."""
    try:
        return fn()
    except Exception as exc:
        kind = classify(exc)
        _log_boundary_error(kind, exc, original)
        return _build_error_envelope(kind, str(exc), original)


def _execute_batch_in_boundary(
    classify: Callable[[Exception], ErrorKind],
    originals: Sequence[Message[StreamPayload]],
    fn: Callable[[], Sequence[NodeResult]],
) -> list[NodeResult]:
    """Execute one batch node and capture failures as per-message envelopes."""
    try:
        return list(fn())
    except Exception as exc:
        kind = classify(exc)
        for message in originals:
            _log_boundary_error(kind, exc, message)
        return [_build_error_envelope(kind, str(exc), message) for message in originals]


def _split_node_result(
    stream: Stream,
    step_id: str,
    ctx: _ErrorSplitContext,
    kind: ErrorKind,
) -> Stream:
    """Split node results into success and error branches and wire errors."""
    split = branch(f"{step_id}_split", stream, _is_message)
    ctx.outputs.wire_node_error(kind, step_id, split.falses)
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
    ctx.outputs.wire_node_error(kind, step_id, errors)
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


def _log_boundary_error(kind: ErrorKind, exc: Exception, original: Message[StreamPayload]) -> None:
    payload_type = original.payload.__class__.loom_message_type()
    context = {
        "kind": kind.value,
        "payload_type": payload_type,
        "message_id": original.meta.message_id,
        "topic": original.meta.topic,
        "partition": original.meta.partition,
        "offset": original.meta.offset,
    }
    if isinstance(exc, DomainError):
        logger.warning("managed_boundary_error", **context, reason=str(exc))
        return
    logger.exception("unhandled_boundary_error", **context, reason=str(exc))
