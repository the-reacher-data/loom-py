"""Rules both agent transports share (:mod:`loom.ai._transport`).

The HTTP surface and the A2A one used to carry two copies of the terminal-error
mapping and of the heartbeat race. The copies drifted: one tested the failure
with ``type(exc) is`` and the other with ``isinstance``, so a subclass of
:class:`~loom.ai.errors.AgentRunError` kept its code over A2A and lost it over
HTTP. These tests fix the unified behaviour and the single ownership.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path

import loom.ai.a2a._handlers as a2a_handlers
import loom.ai.a2a.server as a2a_server
import loom.ai.fastapi.endpoints as endpoints_module
import loom.ai.fastapi.streaming as streaming_module
from loom.ai._transport import failure_event, with_heartbeats
from loom.ai.abc import AgentEvent, TextDeltaEvent
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.fastapi.streaming import stream_sse


class TimeoutWithContext(AgentRunError):
    """Deployment subclass carrying extra context beside the stable code.

    Subclassing is the documented way to add context to a coded failure, so a
    subclass must keep answering its own code.
    """

    def __init__(self, tool: str) -> None:
        super().__init__(AgentRunErrorCode.TOOL_TIMEOUT, f"tool {tool!r} timed out")
        self.tool = tool


def _payload(frame: bytes) -> dict[str, object]:
    """Decode the ``data:`` payload of one SSE frame."""
    _, _, data = frame.decode("utf-8").partition("\ndata: ")
    return dict(json.loads(data.strip()))


async def _collect(frames: AsyncIterator[bytes]) -> list[bytes]:
    return [frame async for frame in frames]


class TestAgentRunErrorSubclass:
    """A subclass owns a stable code; the terminal frame must not invent one."""

    def test_keeps_the_code_when_the_failure_is_a_subclass(self) -> None:
        """``isinstance``, not an exact class match (the divergence this fixes)."""
        event = failure_event(TimeoutWithContext("sql_analytics"))

        assert event.code is AgentRunErrorCode.TOOL_TIMEOUT

    def test_degrades_to_provider_unavailable_when_the_failure_is_not_the_agents(self) -> None:
        """Anything else is the catch-all, with a fixed message."""
        event = failure_event(RuntimeError("the DSN is postgres://user:pw@host/db"))

        assert event.code is AgentRunErrorCode.PROVIDER_UNAVAILABLE

    def test_leaks_no_text_when_the_failure_is_not_the_agents(self) -> None:
        """An unexpected failure's text never reaches the caller."""
        event = failure_event(RuntimeError("the DSN is postgres://user:pw@host/db"))

        assert "postgres://" not in event.message

    async def test_the_sse_frame_carries_the_subclasses_code_when_the_stream_fails(
        self,
    ) -> None:
        """End to end over the HTTP encoding: the code survives to the wire."""

        async def failing() -> AsyncIterator[AgentEvent]:
            yield TextDeltaEvent(text="Demand rose ")
            raise TimeoutWithContext("sql_analytics")

        frames = await _collect(stream_sse(failing(), heartbeat_ms=1000))

        assert _payload(frames[-1])["code"] == "TOOL_TIMEOUT"


class TestSharedHeartbeats:
    """The heartbeat race is relayed over already-encoded frames."""

    async def test_relays_the_frames_in_order_when_there_is_no_silence(self) -> None:
        """Nothing is added to a stream that never goes quiet."""

        async def frames() -> AsyncIterator[bytes]:
            yield b"one"
            yield b"two"

        assert await _collect(with_heartbeats(frames(), heartbeat_ms=1000)) == [b"one", b"two"]


def _a2a_transport_source() -> str:
    """Return every module of the A2A transport package, concatenated.

    The transport is split across ``server``, ``_rpc``, ``_handlers`` and
    ``_binding``; scanning only the entry module would let a copy of a shared
    rule reappear in a sibling and pass unnoticed.
    """
    package = Path(str(a2a_server.__file__)).parent
    return "\n".join(module.read_text(encoding="utf-8") for module in sorted(package.glob("*.py")))


class TestSingleOwnership:
    """Neither surface owns the shared rules, and neither reaches into the other."""

    def test_the_a2a_server_does_not_import_the_http_surfaces_private_names(self) -> None:
        """A leading underscore is a boundary; the A2A module must not cross it."""
        assert "from loom.ai.fastapi.endpoints import" not in _a2a_transport_source()

    def test_both_transports_use_the_same_race_when_they_beat(self) -> None:
        """One heartbeat generator, not a copy per surface."""
        assert a2a_handlers.with_heartbeats is with_heartbeats

    def test_the_http_surface_uses_the_same_race_when_it_beats(self) -> None:
        """``stream_sse`` is the encoder composed with that same generator."""
        assert streaming_module.with_heartbeats is with_heartbeats

    def test_both_transports_use_the_same_period_when_they_beat(self) -> None:
        """One silence budget, so the two surfaces cannot drift apart again."""
        assert a2a_handlers.HEARTBEAT_MS is endpoints_module.HEARTBEAT_MS

    def test_the_http_surface_defines_no_terminal_failure_mapping_of_its_own(self) -> None:
        """The encoder module keeps no private copy of the failure mapping."""
        source = Path(str(streaming_module.__file__)).read_text(encoding="utf-8")

        assert "def _failure_event(" not in source

    def test_the_a2a_server_defines_no_terminal_failure_mapping_of_its_own(self) -> None:
        """Nor does the A2A one: a single definition, imported by both."""
        assert "def _failure_event(" not in _a2a_transport_source()
