"""Server-sent-events encoding of the agent event stream.

Implements the wire contract in ``specs/001-ai-agent-layer/contracts/http-sse.md``:
five event names, fixed payload field names, exactly one terminal frame,
``usage`` only on ``final``, and comment frames during long silences.

Two properties are deliberate and load-bearing:

* Encoding is a dispatch map keyed by the event class, resolved once per event
  with a single mapping lookup. A chain of type tests, or reading a type's name
  at run time, would be reflection on the most frequent event of the whole
  pillar.
* Heartbeats race a timeout against the next event inside this generator. A
  background task feeding a queue would survive the disconnect cancellation and
  keep a run burning tokens with nobody listening.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Mapping
from typing import Any, Final

from loom.ai.abc import (
    AgentEvent,
    ErrorEvent,
    FinalEvent,
    TextDeltaEvent,
    ToolCallEvent,
    ToolResultEvent,
)
from loom.ai.errors import AgentRunErrorCode
from loom.ai.fastapi.response import ENCODER
from loom.ai.runtime import AgentRunError

_logger = logging.getLogger("loom.ai.fastapi.streaming")

HEARTBEAT_FRAME: Final[bytes] = b": ping\n\n"
"""SSE comment frame keeping a silent stream alive; clients ignore it."""

_EVENT_PREFIX: Final[bytes] = b"event: "
_DATA_PREFIX: Final[bytes] = b"\ndata: "
_FRAME_SUFFIX: Final[bytes] = b"\n\n"

_UNEXPECTED_FAILURE = "the agent run failed unexpectedly"


def _text_delta_payload(event: TextDeltaEvent) -> Mapping[str, object]:
    return {"text": event.text}


def _tool_call_payload(event: ToolCallEvent) -> Mapping[str, object]:
    return {"tool": event.tool, "call_id": event.call_id, "arguments": event.arguments}


def _tool_result_payload(event: ToolResultEvent) -> Mapping[str, object]:
    return {"call_id": event.call_id, "ok": event.ok, "summary": event.summary}


def _error_payload(event: ErrorEvent) -> Mapping[str, object]:
    return {"code": event.code, "message": event.message}


def _final_payload(event: FinalEvent) -> Mapping[str, object]:
    return {"output": event.output, "usage": event.usage}


# The event class is the key: one mapping lookup per event, no reflection.
# ``Any`` in the callable signature is the price of a heterogeneous table; each
# entry pairs a class with the builder written for exactly that class.
_DISPATCH: Mapping[type[Any], tuple[bytes, Callable[[Any], Mapping[str, object]]]] = {
    TextDeltaEvent: (b"text_delta", _text_delta_payload),
    ToolCallEvent: (b"tool_call", _tool_call_payload),
    ToolResultEvent: (b"tool_result", _tool_result_payload),
    ErrorEvent: (b"error", _error_payload),
    FinalEvent: (b"final", _final_payload),
}

_TERMINAL_TYPES: frozenset[type[Any]] = frozenset({ErrorEvent, FinalEvent})


def encode_sse_event(event: AgentEvent) -> bytes:
    """Encode one agent event as a single SSE frame.

    The event's name travels on the ``event:`` line, so the union's ``type``
    tag never appears inside the data payload.

    Args:
        event: Event to encode.

    Returns:
        The frame ``event: <name>\\ndata: <json>\\n\\n``.

    Raises:
        KeyError: When the event is not one of the five contract members —
            widening the union without widening this map is a contract break,
            not a silent pass-through.

    Example::

        frame = encode_sse_event(TextDeltaEvent(text="Demand rose "))
    """
    name, payload = _DISPATCH[event.__class__]
    return _EVENT_PREFIX + name + _DATA_PREFIX + ENCODER.encode(payload(event)) + _FRAME_SUFFIX


def _failure_event(exc: Exception) -> ErrorEvent:
    """Turn a post-first-byte failure into the stream's terminal error event."""
    if type(exc) is AgentRunError:
        return ErrorEvent(code=exc.code, message=str(exc))
    return ErrorEvent(code=AgentRunErrorCode.PROVIDER_UNAVAILABLE, message=_UNEXPECTED_FAILURE)


async def _next_or_none(iterator: AsyncIterator[AgentEvent]) -> AgentEvent | None:
    """Await the next event, reporting exhaustion as ``None``."""
    try:
        return await anext(iterator)
    except StopAsyncIteration:
        return None


async def _drain(pending: asyncio.Future[AgentEvent | None] | None) -> None:
    """Cancel the awaited event and wait for it, so nothing outlives the stream."""
    if pending is None:
        return
    pending.cancel()
    try:
        await pending
    except (Exception, asyncio.CancelledError):
        # The consumer is already gone: the cancellation just requested, and
        # any late failure, have nobody left to be reported to.
        return


async def stream_sse(
    events: AsyncIterator[AgentEvent], *, heartbeat_ms: int
) -> AsyncIterator[bytes]:
    """Encode an agent event stream as SSE frames, with heartbeats.

    Every silence longer than *heartbeat_ms* produces one comment frame and the
    race starts again. The awaited event is shielded from the heartbeat — a
    timeout must not tear down the run it is keeping alive — and is cancelled
    and drained when the consumer goes away, so no work outlives it (FR-033).

    Args:
        events: Agent events to encode, terminal event last.
        heartbeat_ms: Silence after which a comment frame is emitted.

    Yields:
        Encoded SSE frames, ending at the single terminal frame.

    Example::

        async for frame in stream_sse(events, heartbeat_ms=15000):
            ...
    """
    iterator = events.__aiter__()
    beat = heartbeat_ms / 1000
    pending: asyncio.Future[AgentEvent | None] | None = None
    try:
        while True:
            if pending is None:
                pending = asyncio.ensure_future(_next_or_none(iterator))
            try:
                async with asyncio.timeout(beat):
                    event = await asyncio.shield(pending)
            except TimeoutError:
                yield HEARTBEAT_FRAME
                continue
            pending = None
            if event is None:
                return
            yield encode_sse_event(event)
            if event.__class__ in _TERMINAL_TYPES:
                return
    except Exception as exc:
        # The status line is long gone, so the only place this failure can
        # travel is in-band, as the stream's one terminal frame (FR-032).
        _logger.warning("agent stream failed after the first byte", exc_info=exc)
        yield encode_sse_event(_failure_event(exc))
    finally:
        await _drain(pending)
