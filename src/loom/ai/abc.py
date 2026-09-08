"""Neutral runtime contracts of the AI pillar.

Everything the compiler, the runtime and the HTTP layer share with an engine
lives here, and nothing here imports an engine: the bootstrap resolves the
provider through :mod:`loom.ai.registry` and hands the compiler plain values.

A run may continue a conversation the application loaded (FR-034): the prior
history crosses this boundary as opaque, engine-native bytes inside
:class:`Conversation`, and the run's new messages come back the same way on
:attr:`AgentResult.messages` and :attr:`FinalEvent.messages`.  Loom defines no
message model and stores no history.

These contracts are experimental and may change within a major line; the
artifact format they serve is not.  See :mod:`loom.ai` for the distinction.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import AbstractAsyncContextManager
from decimal import Decimal
from typing import Any, ClassVar, Final, Literal, Protocol, TypeAlias

from loom.ai.errors import AgentRunErrorCode
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from loom.core.model import LoomFrozenStruct

CONVERSATION_ID_MAX_LENGTH: Final[int] = 128
"""Longest ``conversation_id`` a run accepts; the value itself is opaque."""


class AgentUsage(LoomFrozenStruct, frozen=True, kw_only=True):
    """Resource accounting of one agent run.

    Nothing the engine reported is dropped. The counters any engine would
    plausibly report are named fields; every other field it returned — the
    audio counters, a provider's own extras, a field a future engine release
    adds — rides verbatim in ``details``, so a new counter reaches the caller
    without a change here. The engine's own usage type never crosses this
    boundary: a second engine fills this struct.

    Attributes:
        input_tokens: Tokens sent to the model across the run, cached ones
            included.
        output_tokens: Tokens produced by the model across the run.
        requests: Model requests issued during the run.
        duration_ms: Wall-clock duration of the run in milliseconds.
        cache_read_tokens: Input tokens served from the provider's prompt
            cache, already counted in ``input_tokens``. A cached token costs a
            fraction of a fresh one, so comparing models on ``input_tokens``
            alone can invert the ranking.
        cache_write_tokens: Input tokens written to the prompt cache, already
            counted in ``input_tokens``.
        tool_calls: Tool invocations the model completed during the run. A count, not
            a listing: the per-call detail an output hook may declare is
            :class:`ToolCallRecord`, which shares this name deliberately.
        cost: Run cost in the engine's currency, or ``None`` when the engine
            could not price the model. Absent rather than zero: a zero would
            silently win a cost comparison.
        details: Every field the engine reported that has no named field
            here, under the engine's own names. Not disjoint from the named
            counters: a provider that reports its own ``cached_tokens``
            alongside the normalised ``cache_read_tokens`` has both, so
            summing ``details`` double-counts.
    """

    input_tokens: int
    output_tokens: int
    requests: int
    duration_ms: int
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    tool_calls: int = 0
    cost: Decimal | None = None
    details: Mapping[str, int | float] = {}

    @property
    def total_tokens(self) -> int:
        """Return the input plus output tokens of the run."""
        return self.input_tokens + self.output_tokens

    @property
    def cache_hit_ratio(self) -> float:
        """Return the fraction of input tokens served from the prompt cache.

        Zero when the run reported no input tokens.
        """
        if self.input_tokens == 0:
            return 0.0
        return self.cache_read_tokens / self.input_tokens


class Conversation(LoomFrozenStruct, frozen=True, kw_only=True):
    """The conversation a run continues.

    Attributes:
        conversation_id: The application's identifier of the conversation;
            opaque to loom.
        history: Prior turns in the engine's own serialised form, opaque to
            loom; ``None`` on the first turn.
    """

    conversation_id: str
    history: bytes | None = None


class AgentResult(LoomFrozenStruct, frozen=True, kw_only=True):
    """Outcome of a non-streaming agent run.

    Attributes:
        output: Answer already decoded and validated against the declared
            output shape.
        usage: Resource accounting of the run.
        interaction_id: Identifier the runtime minted for this run.
        hook_result: Return value of the ``on_output`` use case, when the plan
            declares one.
        messages: New messages of this run in the engine's own serialised
            form; ``None`` unless the run carried a conversation.
    """

    output: object
    usage: AgentUsage
    interaction_id: str | None = None
    hook_result: object | None = None
    messages: bytes | None = None


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
        usage: What the failed run had already spent, when the engine knew it.
            A run that made three model round trips and then failed its output
            schema still cost money, and a model that fails more must not rank
            better on cost for it. ``None`` when nothing was spent or nothing
            was measurable — a refusal before admission, a run a declared
            limit killed from outside the engine. Not on the wire: the stream
            contract puts ``usage`` on ``final`` only.
    """

    code: AgentRunErrorCode
    message: str
    interaction_id: str | None = None
    usage: AgentUsage | None = None


class FinalEvent(LoomFrozenStruct, frozen=True, kw_only=True, tag="final", tag_field="type"):
    """The run completed; the only variant carrying usage.

    Attributes:
        output: Answer already decoded and validated against the declared
            output shape.
        usage: Resource accounting of the whole run.
        interaction_id: Identifier the runtime minted for this run.
        hook_result: Return value of the ``on_output`` use case, when the plan
            declares one.
        messages: New messages of this run in the engine's own serialised
            form; ``None`` unless the run carried a conversation.
    """

    output: object
    usage: AgentUsage
    interaction_id: str | None = None
    hook_result: object | None = None
    messages: bytes | None = None


AgentEvent = TextDeltaEvent | ToolCallEvent | ToolResultEvent | ErrorEvent | FinalEvent
"""Closed five-member tagged union of streaming events (FR-030).

Exactly one of ``final`` or ``error`` terminates every stream (SC-011), and
``final`` is the only variant carrying usage.

Adding a variant requires two independent real consumers (FR-035): a single
engine wanting a richer event is not grounds to widen a union every SSE
client, test fake and contract suite must understand.
"""


class ToolCallOutcome(LoomFrozenStruct, frozen=True, kw_only=True):
    """How one tool call of a run ended, as the ``on_output`` hook reads it.

    Loom's own closed outcome vocabulary, and never the tool's payload: a flag
    and a short summary such as ``"3 rows"`` or ``"refused"``.  A hook that
    needs the data re-reads it through its own repositories.

    Deliberately not :class:`ToolResultEvent`, which says the same two things
    on the stream.  That one is a tagged member of :data:`AgentEvent`, so it
    carries a wire tag and a ``call_id`` whose only meaning is correlating a
    stream still in flight.  A completed run has nothing left to correlate —
    :class:`ToolCallRecord` already pairs the call with its outcome — so
    reusing it would put stream vocabulary inside an application command and
    tie that command to a struct the stream contract is free to grow.

    Attributes:
        ok: Whether the tool succeeded.
        summary: Short human-readable outcome; never the full payload.
    """

    ok: bool
    summary: str


class ToolCallRecord(LoomFrozenStruct, frozen=True, kw_only=True):
    """One tool invocation of a completed run, as the ``on_output`` hook sees it.

    Offered to the hook's use case only when its Command declares a
    ``tool_calls`` field: declaring the field is the whole opt-in, so a run
    whose hook does not name it accumulates nothing.  The records arrive in
    call order, which is what makes "the agent ran 3 of the 11 mandatory
    queries" answerable from the hook.

    ``result`` is a :class:`ToolCallOutcome`, so it is loom's own closed
    vocabulary and **never** what the tool returned: the payload does not
    cross this boundary.  ``None`` means the run ended with no result for this
    call, which is deliberately distinguishable from a call that failed
    (``result.ok`` is ``False``): an absent result is an interrupted run, a
    failed one is an answered call.

    A ``kind: native`` tool is executed by the provider and emits no function
    events, so it never appears here.

    Not to be confused with :attr:`AgentUsage.tool_calls`, which is the
    integer count of invocations the model completed during the run.  The two
    names collide on purpose: the counter is what a run cost, these records
    are what it consulted.  The collision has one consequence worth stating,
    because it makes a start-up failure into a run-time one: a Command that
    declares ``tool_calls`` as an ``int`` — the natural spelling next to that
    counter — used to fail compilation with ``ON_OUTPUT_INPUT_UNSATISFIED``
    and now compiles, because the hook is validated by name and never by
    type, and then fails to decode on every run.  Declare the field as
    ``tuple[ToolCallRecord, ...]``.

    Attributes:
        tool: Tool name as the engine exposes it.
        call_id: Correlation id the engine minted for this call, carried so a
            hook can match a record against its own trace of the same run.
        arguments: Arguments the model supplied, decoded once on the event
            path.  A model-authored JSON object has no static shape, hence
            the ``Any`` value type.
        result: How the call ended; ``None`` when the run ended before the
            tool answered.
    """

    tool: str
    call_id: str
    arguments: Mapping[str, Any]
    result: ToolCallOutcome | None = None


class AgentEngine(Protocol):
    """One compiled agent, ready to run.

    Engines take a single prompt and, optionally, the conversation the run
    continues: an opaque, engine-native history the application loaded
    (FR-034).  Loom defines no message model and stores no history; an engine
    that receives ``conversation=None`` runs single-shot.
    """

    async def run(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
    ) -> AgentResult:
        """Run the agent to completion.

        Args:
            prompt: Caller prompt.
            identity: Verified caller; every capability call runs as them.
            conversation: The conversation this run continues; ``None`` runs
                single-shot.

        Returns:
            The validated output and the run's usage, plus ``messages`` when a
            conversation was passed.
        """
        ...

    def run_stream(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
    ) -> AbstractAsyncContextManager[AsyncIterator[AgentEvent]]:
        """Run the agent, streaming events.

        Returns an async context manager rather than a bare iterator so that
        closing the stream — and the provider connection behind it — is
        deterministic on exit instead of being left to the garbage collector.

        Args:
            prompt: Caller prompt.
            identity: Verified caller; every capability call runs as them.
            conversation: The conversation this run continues; ``None`` runs
                single-shot.

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


class McpSession(Protocol):
    """Minimal MCP session the runtime needs from any client library."""

    async def list_tools(self) -> tuple[str, ...]:
        """Return the tool names the server exposes.

        Returns:
            Every tool name the server advertises, before any declared filter
            is applied.
        """
        ...

    async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> object:
        """Invoke one tool and return its result.

        Args:
            name: Tool name as the server exposes it.
            arguments: Arguments to pass to the tool.

        Returns:
            The tool's result, as the client library decoded it.
        """
        ...


class ToolsetContext(Protocol):
    """What a build-time factory of an artifact may reach while it builds.

    Two declarations receive this object, both called once at start-up as
    ``factory(context, **params)``: a ``kind: python`` capability, which
    returns the engine-facing toolset, and ``dynamic_instructions``, which
    returns an :data:`InstructionsProvider`. One name serves both because it
    is one contract; the consumers differ only in what they return.

    A build-time object: the engine hands it to the factory once, at start-up,
    and nothing keeps it alive afterwards. A factory resolves the remotes it
    needs in its body and keeps the session on the toolset it returns; it must
    not call :meth:`remote` lazily from a tool at run time.

    :meth:`remote` is bounded to the ``mcp`` grants of the same agent: it
    returns the worker's shared session for a server the agent's own artifact
    declared, and nothing else. Calls made through that session go to the
    shared connection directly, so they bypass the ``include``/``exclude``
    filter of the ``mcp`` grant.

    **An instructions factory has no legitimate use for** :meth:`remote`. The
    provider it returns is synchronous and performs no I/O, so a session it
    captured could only be called from the prompt path, where nothing bounds
    it: the tool timeout covers tool calls, not prompt building. An
    instructions factory that needs remote material fetches it elsewhere and
    closes over the result.
    """

    @property
    def agent(self) -> str:
        """Name of the plan being built."""
        ...

    @property
    def container(self) -> LoomContainer:
        """Application container the factory may resolve services from."""
        ...

    def remote(self, server: str) -> McpSession:
        """Return the agent's shared session for one of its ``mcp`` servers.

        Args:
            server: Server name as the agent's ``mcp`` grant declares it.

        Returns:
            The session the agent's own ``mcp`` toolset runs over.

        Raises:
            AgentCompilationError: When the agent has no ``mcp`` grant on that
                server.
        """
        ...


ToolsetFactory: TypeAlias = Callable[..., object]
"""Target of a ``kind: python`` capability, validated at compile time.

Called exactly once at build as ``factory(context, **params)``: the first
positional is a :class:`ToolsetContext`, and the artifact's ``params`` arrive
as keyword arguments. A factory declares its own named parameters, with
defaults, and returns the engine-facing toolset. The parameter names are
checked against the signature at compile time. It is a plain ``Callable``
alias rather than a Protocol because a Protocol fixing ``**params`` would
reject every factory that names them.
"""


OutputCheck: TypeAlias = Callable[[Mapping[str, Any]], str | None]
"""Target of an artifact's ``output_check``: a rule its schema cannot express.

A schema states the *shape* of an answer; a check states a rule over its
*values* — "a report that claims resolution must name a root cause". It is
resolved at compile time and registered as the engine's output validator, so a
rejection reaches the model as another attempt at the same run instead of as a
failure the caller has to handle after the fact.

**The return contract is inverted, and that is why this alias exists.**
Returning ``None`` *accepts* the answer; returning a string *rejects* it, and
the string is the text the model reads to correct itself. It is the opposite of
the usual predicate convention, so it is named and documented here rather than
left to each author to rediscover.

**It receives the mapping the engine parsed, not loom's decoded object.** Loom
decodes the answer once, from the run's raw messages, after the engine has
finished; handing the check that object would mean decoding the same payload a
second time. So the argument is the plain mapping the engine already holds, and
mutating it in place changes nothing downstream: the decode does not read it.
It is the one public name here typed ``Mapping[str, Any]``, because the payload
shape is the artifact's own JSON schema, which loom cannot name statically.

**It must be pure.** It runs inside the engine's output-retry loop, so any
effect it performed would repeat once per attempt. It is synchronous, takes no
dependencies and reads no data. Work that needs to read data belongs in the
``on_output`` hook, which runs once, outside the engine.

Example::

    def report_is_complete(answer: Mapping[str, Any]) -> str | None:
        if answer.get("resolved") and answer.get("root_cause_id") is None:
            return "resolved reports must name a root cause; query it and answer again"
        return None
"""


class InstructionsRequest(LoomFrozenStruct, frozen=True, kw_only=True):
    """What an :data:`InstructionsProvider` is told about the request it serves.

    A struct of its own, built once per model request — not once per run — and
    never a view over the run's dependency bundle. The bundle also carries the
    application container and the caller-bound invoker; neither crosses to
    prompt-building code, and building a separate value is what makes that a
    property of the type rather than a promise in this docstring.

    Every request of one run is told the same four things: the prompt is the
    run's, not the last model turn's. So a provider that reads nothing else
    returns the same text every time it is called within a run.

    **The prompt is here to choose instructions, never to authorise
    anything.** It is caller-supplied text, so a provider may branch on it to
    decide *which* material to compose, and must not read it as evidence of
    who the caller is or of what the agent may do. Authorisation is the
    caller's identity, enforced at the capability boundary; a provider returns
    text and cannot widen what the agent is granted.

    The conversation and interaction identifiers are deliberately absent: the
    interaction identifier is minted by the runtime and never handed to an
    engine, and the conversation identifier only exists when the artifact
    declares a ``conversation`` block. Supplying either would change the
    dependency-factory contract or the versioned engine run protocol, so both
    are deferred rather than approximated.

    Attributes:
        agent: Name of the agent serving the request, as the artifact declares
            it.
        prompt: Prompt this run was called with — the caller's, unchanged for
            every model request the run makes.
        subject: Verified subject of the caller, as the run's identity carries
            it; empty for an anonymous caller.
        mechanism: Label of the authentication mechanism that produced
            ``subject``.
    """

    agent: str
    prompt: str
    subject: str
    mechanism: str


InstructionsProvider: TypeAlias = Callable[[InstructionsRequest], str | None]
"""Per-request half of ``dynamic_instructions``: text for one request, or nothing.

Composed after the artifact's literal ``instructions`` and never instead of
them: **the literal composes first and the returned text is appended to it**,
so a provider adds to the agent's role and tone rather than replacing them.
Returning ``None`` contributes nothing at all, which is how a provider says
"this request needs no extra material".

**Called once per model request, not once per run.** The engine rebuilds the
instructions for every request it makes to the model, and a run makes as many
as it needs: a single tool call already costs two, and a run that reaches the
artifact's ``max_iterations`` costs that many. Only a run that answers without
calling anything costs exactly one. This is the normal case for any agent with
capabilities, so a provider is written to be called repeatedly within one run.

**Synchronous, and free of I/O**, which this multiplier is what makes
non-negotiable rather than merely tidy: whatever a provider costs is paid per
request, not per run. It runs on the prompt path, which no deadline of loom's
covers — the ``tool_timeout_ms`` of the artifact bounds tool calls — so an
await here would spend the whole run budget without a name of its own. A
provider that needs data reads it in its factory, at start-up, and closes over
the result. A coroutine function is refused at build time rather than awaited.

**A non-deterministic provider is answerable for its own drift.** Loom neither
caches nor deduplicates the result, so a provider that returns different text
for two requests of one run genuinely sends the model different instructions
mid-run, with the earlier text still in the message history. That is the
author's decision to make and the author's to defend: a provider that must be
stable within a run derives its text from
:class:`InstructionsRequest` alone, which is identical across the requests of
one run.

Example::

    def provide(request: InstructionsRequest) -> str | None:
        return catalog.render(request.prompt)
"""


InstructionsFactory: TypeAlias = Callable[..., InstructionsProvider]
"""Target of an artifact's ``dynamic_instructions``, validated at compile time.

Called exactly once at build as ``factory(context, **params)``: the first
positional is a :class:`ToolsetContext` — the same build-time object a
``kind: python`` capability factory receives — and the artifact's ``params``
arrive as keyword arguments, their names checked against the signature at
compile time. It returns the :data:`InstructionsProvider` the run path calls.

It is a plain ``Callable`` alias, for the reason :data:`ToolsetFactory` gives:
a Protocol fixing ``**params`` would reject every factory that names them.

Example::

    def build_checklist(
        context: ToolsetContext, *, locale: str = "en"
    ) -> InstructionsProvider:
        catalog = context.container.resolve(ChecklistCatalog)

        def provide(request: InstructionsRequest) -> str | None:
            return catalog.render(request.prompt, locale=locale)

        return provide
"""


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


NativeToolSupport = Callable[[InferenceTarget], frozenset[str]]
"""Answers which provider tools a model binding admits, by loom tool name.

Supplied by an engine as an optional ``native_tool_support`` attribute and read
with ``getattr``, so the compiler learns what a binding admits without importing
an engine. May raise :class:`~loom.ai.errors.AgentCompilationError` when the
provider SDK is missing.
"""


class AgentEngineProvider(Protocol):
    """Entry-point target in group ``loom.ai.engines``.

    Attributes:
        LOOM_AI_ENGINE_API: Handshake version, checked with ``getattr`` on
            load — never with ``isinstance``.
        native_tool_support: Optional :data:`NativeToolSupport`, read with
            ``getattr``; an engine that serves no ``native`` grant omits it.
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
