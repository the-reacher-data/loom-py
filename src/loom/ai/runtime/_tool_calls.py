"""Tool-call summary of one run, accumulated on the event path it already crosses.

Composed into the stream only when the plan's ``on_output`` hook declares the
``tool_calls`` name (:data:`~loom.ai.compiler._plan.HOOK_TOOL_CALLS_FIELD`), so
a run whose hook stays silent has no wrapper on its events at all.

Nothing is serialised here: the arguments are already decoded exactly once by
the engine adapter, and the outcome is the two values loom already built for
the stream, copied into a :class:`~loom.ai.abc.ToolCallOutcome` of its own so
no stream vocabulary reaches an application command.  The accumulator observes
and forwards; it never rewrites, drops or reorders an event.

The records are owned by one accumulator instance, created per run, and leave
it only as an immutable snapshot: the hook stage receives a tuple, never the
mutable list.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator

import msgspec

from loom.ai.abc import (
    AgentEvent,
    ToolCallEvent,
    ToolCallOutcome,
    ToolCallRecord,
    ToolResultEvent,
)


class ToolCallAccumulator:
    """Owner of one run's tool-call records, filled from its event stream.

    Calls and results are correlated by ``call_id`` and kept in call order.  A
    result whose ``call_id`` names no observed call is ignored rather than
    recorded: a record with no arguments would claim the model made a call it
    never made.
    """

    __slots__ = ("_positions", "_records")

    def __init__(self) -> None:
        self._records: list[ToolCallRecord] = []
        self._positions: dict[str, int] = {}

    async def track(self, events: AsyncIterator[AgentEvent]) -> AsyncGenerator[AgentEvent, None]:
        """Forward every event of the run unchanged, recording the tool traffic.

        Args:
            events: The run's event stream.

        Yields:
            The same events, in the same order, untouched.
        """
        async for event in events:
            self._observe(event)
            yield event

    def records(self) -> tuple[ToolCallRecord, ...]:
        """Return the calls observed so far, in call order.

        Returns:
            An immutable snapshot; a call still awaiting its result carries
            ``result=None``.
        """
        return tuple(self._records)

    def _observe(self, event: AgentEvent) -> None:
        """Record a call or attach a result; every other event kind is not ours."""
        if type(event) is ToolCallEvent:
            self._open(event)
        elif type(event) is ToolResultEvent:
            self._close(event)

    def _open(self, event: ToolCallEvent) -> None:
        self._positions[event.call_id] = len(self._records)
        self._records.append(
            ToolCallRecord(tool=event.tool, call_id=event.call_id, arguments=event.arguments)
        )

    def _close(self, event: ToolResultEvent) -> None:
        position = self._positions.get(event.call_id)
        if position is None:
            return
        outcome = ToolCallOutcome(ok=event.ok, summary=event.summary)
        self._records[position] = msgspec.structs.replace(self._records[position], result=outcome)
