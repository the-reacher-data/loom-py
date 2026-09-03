"""Neutral runtime contracts of the AI pillar.

Everything the compiler, the runtime and the HTTP layer share with an engine
lives here, and nothing here imports an engine: the bootstrap resolves the
provider through :mod:`loom.ai.registry` and hands the compiler plain values.

These contracts are experimental and may change within a major line; the
artifact format they serve is not.  See :mod:`loom.ai` for the distinction.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextlib import AbstractAsyncContextManager
from typing import Any, ClassVar, Final, Literal, Protocol

from loom.ai.errors import AgentRunErrorCode
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from loom.core.model import LoomFrozenStruct

CONVERSATION_ID_MAX_LENGTH: Final[int] = 128
"""Longest ``conversation_id`` a run accepts; the value itself is opaque."""


class AgentUsage(LoomFrozenStruct, frozen=True, kw_only=True):
    """Resource accounting of one agent run.

    Attributes:
        input_tokens: Tokens sent to the model across the run.
        output_tokens: Tokens produced by the model across the run.
        requests: Model requests issued during the run.
        duration_ms: Wall-clock duration of the run in milliseconds.
    """

    input_tokens: int
    output_tokens: int
    requests: int
    duration_ms: int


class AgentResult(LoomFrozenStruct, frozen=True, kw_only=True):
    """Outcome of a non-streaming agent run.

    Attributes:
        output: Answer already decoded and validated against the declared
            output shape.
        usage: Resource accounting of the run.
        interaction_id: Identifier the runtime minted for this run.
        hook_result: Return value of the ``on_output`` use case, when the plan
            declares one.
    """

    output: object
    usage: AgentUsage
    interaction_id: str | None = None
    hook_result: object | None = None


class TextDeltaEvent(
    LoomFrozenStruct, frozen=True, kw_only=True, tag="text_delta", tag_field="type"
):
    """Incremental model text.

    Attributes:
        text: Text fragment, passed through unmodified.
    """

    text: str


class ToolCallEvent(LoomFrozenStruct, frozen=True, kw_only=True, tag="tool_call", tag_field="type"):
    """The model invoked a tool.

    Attributes:
        tool: Tool name as the engine exposes it.
        call_id: Correlation id matching the eventual ``tool_result``.
        arguments: Arguments the model supplied.
    """

    tool: str
    call_id: str
    arguments: Mapping[str, Any]


class ToolResultEvent(
    LoomFrozenStruct, frozen=True, kw_only=True, tag="tool_result", tag_field="type"
):
    """A tool invocation completed.

    Attributes:
        call_id: Correlation id of the originating ``tool_call``.
        ok: Whether the tool succeeded.
        summary: Short human-readable outcome; never the full payload.
    """

    call_id: str
    ok: bool
    summary: str


class ErrorEvent(LoomFrozenStruct, frozen=True, kw_only=True, tag="error", tag_field="type"):
    """The run failed mid-stream (FR-032).

    Attributes:
        code: Stable run-time failure code; the retry policy reads its class.
        message: Human-readable description.
        interaction_id: Identifier of the admitted run this failure belongs
            to; ``None`` before admission.
    """

    code: AgentRunErrorCode
    message: str
    interaction_id: str | None = None


class FinalEvent(LoomFrozenStruct, frozen=True, kw_only=True, tag="final", tag_field="type"):
    """The run completed; the only variant carrying usage.

    Attributes:
        output: Answer already decoded and validated against the declared
            output shape.
        usage: Resource accounting of the whole run.
        interaction_id: Identifier the runtime minted for this run.
        hook_result: Return value of the ``on_output`` use case, when the plan
            declares one.
    """

    output: object
    usage: AgentUsage
    interaction_id: str | None = None
    hook_result: object | None = None


AgentEvent = TextDeltaEvent | ToolCallEvent | ToolResultEvent | ErrorEvent | FinalEvent
"""Closed five-member tagged union of streaming events (FR-030).

Exactly one of ``final`` or ``error`` terminates every stream (SC-011), and
``final`` is the only variant carrying usage.

Adding a variant requires two independent real consumers (FR-035): a single
engine wanting a richer event is not grounds to widen a union every SSE
client, test fake and contract suite must understand.
"""


class AgentEngine(Protocol):
    """One compiled agent, ready to run.

    Engines take a single prompt and never a message history: multi-turn is
    out of scope by design (FR-034).
    """

    async def run(self, prompt: str, *, identity: Identity) -> AgentResult:
        """Run the agent to completion.

        Args:
            prompt: Caller prompt.
            identity: Verified caller; every capability call runs as them.

        Returns:
            The validated output and the run's usage.
        """
        ...

    def run_stream(
        self, prompt: str, *, identity: Identity
    ) -> AbstractAsyncContextManager[AsyncIterator[AgentEvent]]:
        """Run the agent, streaming events.

        Returns an async context manager rather than a bare iterator so that
        closing the stream — and the provider connection behind it — is
        deterministic on exit instead of being left to the garbage collector.

        Args:
            prompt: Caller prompt.
            identity: Verified caller; every capability call runs as them.

        Returns:
            An async context manager yielding the event stream.
        """
        ...

    async def health(self) -> HealthStatus:
        """Report the engine's current health without per-call network I/O.

        Returns:
            The engine's state, derived from outcomes it has already observed
            rather than from a probe issued on this call.
        """
        ...


HealthState = Literal["ok", "degraded", "unavailable"]
"""The three health states. Defined beside the struct that carries it so the alias
and the field cannot drift apart."""


class HealthStatus(LoomFrozenStruct, frozen=True, kw_only=True):
    """Health of one agent engine, shared by every engine (FR-048).

    Attributes:
        status: ``"ok"``, ``"degraded"`` or ``"unavailable"``.
        detail: Optional human-readable explanation.
    """

    status: HealthState
    detail: str | None = None


class ToolsetFactory(Protocol):
    """Target of a ``kind: python`` capability, validated at compile time."""

    def __call__(self, container: LoomContainer) -> object:
        """Build the toolset from application-scope services.

        Args:
            container: Application container the factory may resolve from.

        Returns:
            The engine-facing toolset object.
        """
        ...


class DepsFactory(Protocol):
    """Builds per-invocation dependencies for capability calls.

    Singleton services are captured once at build; :class:`Identity` is
    supplied per invocation so every capability call runs as the caller
    (FR-043).
    """

    def build(self, identity: Identity, container: LoomContainer) -> object:
        """Build the dependency bundle for one invocation.

        Args:
            identity: Verified caller of this invocation.
            container: Application container holding the singleton services.

        Returns:
            The engine-facing dependency bundle.
        """
        ...


class AgentEngineProvider(Protocol):
    """Entry-point target in group ``loom.ai.engines``.

    Attributes:
        LOOM_AI_ENGINE_API: Handshake version, checked with ``getattr`` on
            load — never with ``isinstance``.
    """

    LOOM_AI_ENGINE_API: ClassVar[int]

    def create_engine(
        self, plan: object, *, deps: DepsFactory, container: LoomContainer
    ) -> AgentEngine:
        """Build one engine for one compiled plan.

        Called exactly once per plan by the runtime, never per request.

        Args:
            plan: The compiled ``AgentPlan``.  Typed as ``object`` here
                because the plan struct is built in phase 4; the parameter
                narrows to ``AgentPlan`` then (recorded decision).
            deps: Per-invocation dependency factory.
            container: Application container.

        Returns:
            The engine serving this plan.
        """
        ...

    def supported_capability_kinds(self) -> frozenset[str]:
        """Capability kinds this engine can serve.

        The compiler receives the result as a plain value resolved by the
        bootstrap — nothing in ``loom.ai`` imports an engine to obtain it.

        Returns:
            The supported ``kind`` identifiers.
        """
        ...
