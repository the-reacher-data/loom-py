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

from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from decimal import Decimal
from typing import Any, ClassVar, Final, Generic, Literal, Protocol, TypeAlias, TypeVar, overload

from loom.ai.errors import AgentRunErrorCode
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from loom.core.model import LoomFrozenStruct

CONVERSATION_ID_MAX_LENGTH: Final[int] = 128
"""Longest ``conversation_id`` a run accepts; the value itself is opaque."""

AnswerT = TypeVar("AnswerT")
"""Output type of an :class:`AgentHandle`, carried by the annotation on the
use-case parameter it fills — never by the marker that fills it."""

ExpectedT = TypeVar("ExpectedT")
"""Per-run output type passed to :meth:`AgentHandle.run` through ``expect``,
independent of the handle's own :data:`AnswerT`."""

ToolResultT = TypeVar("ToolResultT")
"""Decoded type of a single :meth:`McpHandle.call`."""


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
        tool_calls: Tool invocations the model completed during the run.
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


class AgentAnswer(LoomFrozenStruct, Generic[AnswerT], frozen=True, kw_only=True):
    """Outcome of one run reached through an :class:`AgentHandle`.

    Carries this run's answer and this run's own accounting, and nothing
    else: ``usage`` is scoped to the single call that produced this answer
    and is never merged with the usage of another call the same handle made,
    or of the parent run that reached this agent in the first place. A use
    case that runs the same handle three times gets three independent
    ``AgentAnswer`` values with three independent ``usage`` fields; summing
    them, if a caller wants a total, is the caller's own arithmetic.

    Attributes:
        output: Decoded answer of this run — the artefact's declared output
            shape by default, or the type passed as ``expect`` when the run
            overrode it for this call only.
        usage: Resource accounting of this run only.
        interaction_id: Identifier the runtime minted for this run.
    """

    output: AnswerT
    usage: AgentUsage
    interaction_id: str | None = None


class McpHandle(Protocol):
    """One artefact's own filtered view of one of its ``mcp`` grants.

    Not a second, independently configured filter: this is the very same
    composed view — over the same shared session, built by the same
    include/exclude predicate — that the model's own toolset runs over.
    There is no wider filter this handle could reach, because no second
    filter exists to diverge towards. Authentication, timeout, span and
    expiry are the same guard with the same numbers the model's own calls
    use, because they come from the same plan.
    """

    def tools(self) -> tuple[str, ...]:
        """Return the tool names visible through this grant's own filter.

        Returns:
            Tool names already narrowed by the artefact's declared
            include/exclude filter for this server.
        """
        ...

    async def call(
        self,
        tool: str,
        arguments: Mapping[str, Any],
        *,
        expect: type[ToolResultT],
    ) -> ToolResultT:
        """Call *tool* and decode its structured result into *expect*.

        Two methods exist here instead of one method with an optional
        ``expect`` — this one and :meth:`call_untyped` — because an optional
        argument would govern two different behaviours from one parameter,
        which this framework's rules on multi-behaviour flags forbid, and it
        would turn the unshaped path into an omission instead of a decision.
        Typed is what this method commits to; the unshaped path is the
        separately named, deliberate exception.

        Loom does not compare ``expect`` against the tool's published output
        schema — that comparison is JSON Schema subsumption, whose verdict
        would only be approximate. Decoding the structured result into
        ``expect`` is the check, and it is exact: a mismatch names the field
        and the type that did not fit.

        Args:
            tool: Tool name, as returned by :meth:`tools`.
            arguments: Arguments passed to the tool call.
            expect: Type the tool's structured result is decoded into. The
                tool must publish an output schema; one that does not is
                refused before any network call.

        Returns:
            The decoded result.

        Raises:
            AgentRunError: With a code naming why the call did not produce a
                decoded ``expect`` — the tool is outside this grant's filter,
                the tool publishes no output schema, the tool reported a
                failure, the server returned no structured content despite
                publishing a schema, or the structured content did not
                decode into ``expect``.
        """
        ...

    async def call_untyped(self, tool: str, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        """Call *tool* and return the server's own result, undecoded.

        The deliberate exception to :meth:`call`'s typed default — for a
        tool that never publishes an output schema, or for a caller who
        genuinely wants the server's own shape. ``Mapping[str, Any]`` is
        admissible here specifically because this signature has nowhere to
        put a type parameter; it is not a general-purpose escape hatch.

        Args:
            tool: Tool name, as returned by :meth:`tools`.
            arguments: Arguments passed to the tool call.

        Returns:
            The server's own structured result, unvalidated and undecoded.

        Raises:
            AgentRunError: With a code naming why the call failed — the tool
                is outside this grant's filter, the tool reported a failure,
                or the server returned structured content that is not a
                mapping (a list, a scalar) — a contradiction of the protocol
                this method returns, distinct from returning no structured
                content at all, which comes back as ``{}``.
        """
        ...


class SqlGrantHandle(Protocol):
    """One artefact's own bounded view of one of its ``sql`` grants.

    Queries the same read-only connection under the same row and byte
    bounds, and the same plan timeout, that the artefact's own ``sql``
    capability enforces for the model — the granted view, not a second one a
    use case could widen.
    """

    async def query(
        self,
        statement: str,
        *,
        parameters: Mapping[str, Any] | None = None,
    ) -> Sequence[Mapping[str, Any]]:
        """Run a read-only statement bounded by this grant's own limits.

        Args:
            statement: SQL statement to run against the granted connection.
            parameters: Server-side bound parameters, when the statement
                uses them.

        Returns:
            Result rows, each as a column-name-to-value mapping, truncated
            to this grant's row and byte bounds.

        Raises:
            AgentRunError: With a code naming why the query did not run —
                the connection is outside this artefact's grants, or the
                grant's own bounds rejected the result.
        """
        ...


class AgentHandle(Protocol[AnswerT]):
    """A named agent reached from another use case, bound to this run's caller.

    Filled by the executor when a use case declares one in its ``execute``
    signature through :func:`loom.core.use_case.markers.Agent`, and never
    constructed directly. The type argument this Protocol carries —
    ``AgentHandle[SeverityAssessment]`` on the parameter's annotation — is
    what the compiler checks against the named agent's own declared output
    at start-up; :func:`Agent` itself returns an untyped value, for the same
    reason every other marker in this vocabulary does.

    Exactly three arguments cross this boundary on a per-run basis: the
    prompt, the shape of the answer, and which conversation it continues.
    Nothing else does. What the agent may reach, what it may cost and which
    model serves it are decided once — by the artefact and by deployment
    configuration — and stay there; a per-run argument that changed any of
    them would be a second place the same policy could drift.
    """

    @overload
    async def run(
        self,
        prompt: str,
        *,
        conversation_id: str | None = None,
    ) -> AgentAnswer[AnswerT]: ...

    @overload
    async def run(
        self,
        prompt: str,
        *,
        expect: type[ExpectedT],
        conversation_id: str | None = None,
    ) -> AgentAnswer[ExpectedT]: ...

    async def run(
        self,
        prompt: str,
        *,
        expect: type[ExpectedT] | None = None,
        conversation_id: str | None = None,
    ) -> AgentAnswer[AnswerT] | AgentAnswer[ExpectedT]:
        """Run the agent once and decode its answer.

        Without ``expect``, the answer is decoded into the artefact's own
        declared output shape — this handle's type argument. With
        ``expect``, that declared shape is a default rather than a ceiling:
        this run only is decoded into ``expect`` instead. The override
        applies to this call and nothing else; it is never merged with the
        artefact's declared shape and never carries over to the handle's
        next call.

        The artefact's own output check — the retry loop that asks the model
        to correct a violation of its declared schema — does not run when
        ``expect`` overrides the shape. That is forced, not chosen: the
        check is compiled against the declared schema, so handing it another
        shape would either fail inside the engine's own retry loop or invent
        a verdict loom has no basis for. Validating an overridden shape is
        the calling code's job instead, and it is better placed there:
        holding a typed answer, calling code can act on a bad verdict — for
        example asking again in a loop it controls — rather than only
        reporting one.

        Permissions never travel through this call. What the agent may
        reach comes from the artefact's own grants and from the identity
        already bound to this handle; ``expect`` changes what comes back,
        never what the agent is allowed to do.

        Args:
            prompt: Prompt for this run.
            expect: When given, decode this run's answer into this type
                instead of the artefact's declared output. Applies to this
                run only.
            conversation_id: Identifier of the conversation this run
                continues; ``None`` runs single-shot.

        Returns:
            The decoded answer, this run's own usage and its interaction id.

        Raises:
            AgentRunError: With ``AGENT_RUN_SHAPE_WITH_HOOK`` when ``expect``
                is given and the artefact's output hook command declares the
                output field — refused before the model is called, since the
                hook could not be handed an answer shaped by ``expect``.
        """
        ...

    async def run_text(
        self,
        prompt: str,
        *,
        conversation_id: str | None = None,
    ) -> AgentAnswer[str]:
        """Run the agent for open prose, pinning this run's answer to ``str``.

        A named spelling of ``run(prompt, expect=str)`` rather than a third
        overload of it — the name states the form this run asks for, open
        prose, not the author's intent, because the form is the only part
        loom knows. Being a shape pin, this mode runs no output check
        (:meth:`run`'s own note on ``expect`` applies here too: the check is
        compiled against the artefact's declared schema and cannot validate
        another one) and is refused before the model is called under
        exactly the condition ``run``'s ``expect`` is: when the artefact's
        output hook command declares the ``output`` field, which was
        compiled against the declared shape and cannot be handed ``str``
        instead.

        Args:
            prompt: Prompt for this run.
            conversation_id: Identifier of the conversation this run
                continues; ``None`` runs single-shot.

        Returns:
            The model's own prose, this run's usage and its interaction id.

        Raises:
            AgentRunError: With ``AGENT_RUN_SHAPE_WITH_HOOK`` when the
                artefact's output hook command declares the ``output``
                field — refused before the model is called, for the same
                reason :meth:`run` raises it with ``expect`` given.
        """
        ...

    def mcp(self, server: str) -> McpHandle:
        """Return the artefact's own filtered view of one ``mcp`` grant.

        Args:
            server: Server name as the artefact's own ``mcp`` capability
                declares it. Not verified at start-up — only a compiled
                agent name and its output type are — so a typo here is
                caught on first call, not before.

        Returns:
            The grant's own view, filtered exactly as the model's is.

        Raises:
            AgentRunError: With ``MCP_GRANT_UNKNOWN`` when the artefact
                declares no ``mcp`` grant on that server name.
        """
        ...

    def sql(self, connection: str) -> SqlGrantHandle:
        """Return the artefact's own bounded view of one ``sql`` grant.

        Args:
            connection: Connection name as the artefact's own ``sql``
                capability declares it. Not verified at start-up, for the
                same reason :meth:`mcp`'s ``server`` is not.

        Returns:
            A view bounded by that grant's own row and byte limits, under
            the plan's timeout.

        Raises:
            AgentRunError: With ``SQL_GRANT_UNKNOWN`` when the artefact
                declares no ``sql`` grant on that connection name.
        """
        ...

    def grants(self) -> tuple[str, ...]:
        """Return every grant name reachable through :meth:`mcp` and :meth:`sql`.

        Exists so application code — most usefully a test — can pin a grant
        name in one assertion instead of reading the artefact's YAML.

        Returns:
            Every ``mcp`` server name and ``sql`` connection name the
            artefact declares: every server first, then every connection,
            each group in declaration order.
        """
        ...


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


class McpToolInfo(LoomFrozenStruct, frozen=True, kw_only=True):
    """One tool a session's server advertises.

    Attributes:
        name: Tool name as the server exposes it.
        has_output_schema: Whether the server published a schema for this
            tool's structured result. A :class:`McpHandle` refuses a typed
            call on a tool for which this is ``False``, before any network
            call — see :meth:`McpHandle.call`.
    """

    name: str
    has_output_schema: bool


class McpToolCallResult(LoomFrozenStruct, frozen=True, kw_only=True):
    """The server's own answer to one ``call_tool``, protocol-level and undecoded.

    Attributes:
        ok: ``False`` when the server flagged the call as failed.
        structured: The tool's structured result, or ``None`` when the server
            returned none — including every call the server flagged failed,
            whose content is never treated as an answer.
    """

    ok: bool
    structured: object | None = None


class McpSession(Protocol):
    """Minimal MCP session the runtime needs from any client library.

    Migration (breaking, from v1.16.1): ``list_tools`` used to return tool
    names (``tuple[str, ...]``) and ``call_tool`` used to return the server's
    structured content directly (``object``). Both shapes shipped, so a
    third-party session implementing this Protocol has to update both methods.
    ``list_tools`` now returns :class:`McpToolInfo` so a caller can see which
    tools publish an output schema, and ``call_tool`` now returns
    :class:`McpToolCallResult` so a caller can see the server's own failure
    flag instead of having it silently folded into a successful-looking
    return. Nothing else about the Protocol moved.
    """

    async def list_tools(self) -> tuple[McpToolInfo, ...]:
        """Return the tools the server exposes.

        Returns:
            Every tool the server advertises, before any declared filter is
            applied, each carrying whether it publishes an output schema.
        """
        ...

    async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> McpToolCallResult:
        """Invoke one tool and return its protocol-level result.

        Args:
            name: Tool name as the server exposes it.
            arguments: Arguments to pass to the tool.

        Returns:
            The server's own error flag and structured content, neither
            interpreted nor decoded.
        """
        ...


class ToolsetContext(Protocol):
    """What a ``kind: python`` factory may reach while building its toolset.

    A build-time object: the engine hands it to the factory once, at start-up,
    and nothing keeps it alive afterwards. A factory resolves the remotes it
    needs in its body and keeps the session on the toolset it returns; it must
    not call :meth:`remote` lazily from a tool at run time.

    :meth:`remote` is bounded to the ``mcp`` grants of the same agent: it
    returns the worker's shared session for a server the agent's own artifact
    declared, and nothing else. Calls made through that session go to the
    shared connection directly, so they bypass the ``include``/``exclude``
    filter of the ``mcp`` grant.
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
