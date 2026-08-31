"""Engine stream events → loom's closed :data:`~loom.ai.abc.AgentEvent` union.

The union has five members and gains a sixth only when two independent
consumers need it (FR-035), so this translation **projects onto what exists
and invents nothing**. pydantic-ai publishes twenty-odd event kinds; four
carry information a loom consumer can act on, and the rest are structural
(part boundaries, availability deltas, realtime-session signals).

Structural kinds are dropped rather than turned into an ``error``: an
``error`` terminates the stream (SC-011), so reporting one for a
``part_end`` would fail a healthy run. Drift is caught instead by a coverage
test that fails when pydantic-ai adds a kind this module has not been taught
— explicit, and at test time rather than in production.
"""

from __future__ import annotations

from typing import Any

from pydantic_ai.messages import (
    FunctionToolCallEvent,
    FunctionToolResultEvent,
    PartDeltaEvent,
    PartStartEvent,
    RetryPromptPart,
    TextPart,
    TextPartDelta,
)

from loom.ai.abc import AgentEvent, TextDeltaEvent, ToolCallEvent, ToolResultEvent

MAPPED_EVENT_KINDS: frozenset[str] = frozenset(
    {"part_start", "part_delta", "function_tool_call", "function_tool_result", "agent_run_result"}
)
"""Engine event kinds this module projects onto the loom union."""

IGNORED_EVENT_KINDS: frozenset[str] = frozenset(
    {
        "part_end",
        "final_result",
        "enqueued_messages",
        "tool_availability_delta",
        "output_tool_call",
        "output_tool_result",
        "deferred_tool_requests",
        "deferred_tool_results",
        "realtime_turn_complete",
        "realtime_input_speech_start",
        "realtime_input_speech_end",
        "realtime_output_speech_start",
        "realtime_output_speech_end",
        "realtime_response_interrupted",
        "realtime_input_transcription_error",
        "realtime_session_reconnect",
        "realtime_session_error",
    }
)
"""Engine event kinds with no loom counterpart; carried by no consumer."""

_TOOL_OK = "completed"
_TOOL_RETRY = "the model was asked to retry the call"


def _text_from_start(event: PartStartEvent) -> AgentEvent | None:
    part = event.part
    if isinstance(part, TextPart) and part.content:
        return TextDeltaEvent(text=part.content)
    return None


def _text_from_delta(event: PartDeltaEvent) -> AgentEvent | None:
    delta = event.delta
    if isinstance(delta, TextPartDelta) and delta.content_delta:
        return TextDeltaEvent(text=delta.content_delta)
    return None


def _tool_call(event: FunctionToolCallEvent) -> AgentEvent:
    part = event.part
    arguments: dict[str, Any] = part.args_as_dict() if part.args is not None else {}
    return ToolCallEvent(tool=part.tool_name, call_id=part.tool_call_id, arguments=arguments)


def _tool_result(event: FunctionToolResultEvent) -> AgentEvent:
    part = event.part
    ok = not isinstance(part, RetryPromptPart)
    # The summary never carries a byte of the payload (FR-030b).
    return ToolResultEvent(
        call_id=part.tool_call_id, ok=ok, summary=_TOOL_OK if ok else _TOOL_RETRY
    )


def translate(event: object) -> AgentEvent | None:
    """Project one engine event onto the loom union.

    ``agent_run_result`` is deliberately absent: the terminal ``final`` event
    carries the *validated* answer and the run's usage, so it is built by the
    engine adapter, which owns the decode.

    Args:
        event: Event yielded by the engine's stream.

    Returns:
        The loom event, or ``None`` when the kind carries nothing a loom
        consumer can act on.
    """
    if isinstance(event, PartStartEvent):
        return _text_from_start(event)
    if isinstance(event, PartDeltaEvent):
        return _text_from_delta(event)
    if isinstance(event, FunctionToolCallEvent):
        return _tool_call(event)
    if isinstance(event, FunctionToolResultEvent):
        return _tool_result(event)
    return None
