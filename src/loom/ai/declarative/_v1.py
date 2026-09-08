"""Tier-1 authored artifact structs for agent spec version 1.

These structs are the *authored* surface: what a human or a generator writes in
an ``.agent.yaml`` file. They are engine-agnostic and vendor-agnostic — no
engine, provider, model identifier or credential is representable here.

Every struct is frozen, keyword-only and rejects unknown fields, so an
unrecognised key is a decoding failure rather than a silently dropped value
(FR-005).

The module-level constants are the single source of truth for the published
JSON Schema: :mod:`loom.ai.declarative._schema` derives every pattern, default,
minimum and maximum from them so the schema cannot drift from the structs.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any, Final, Literal, get_args

import msgspec

SPEC_VERSION_V1: Final[int] = 1
"""Format version implemented by :class:`AgentSpecV1`."""

AGENT_NAME_PATTERN: Final[str] = r"^[a-z][a-z0-9_-]{0,62}$"
"""Pattern every agent name must satisfy."""

MODEL_ROLE_PATTERN: Final[str] = r"^[a-z][a-z0-9_-]{0,31}$"
"""Pattern every logical model role must satisfy."""

SYMBOL_REF_PATTERN: Final[str] = r"^[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*$"
"""Pattern of a ``module:symbol`` reference; filesystem paths are not representable."""

SKILLS_LIBRARY_PATTERN: Final[str] = r"^(\./[A-Za-z0-9._-]+|[A-Za-z0-9._-]+)$"
"""Pattern of a skill library name: ``./name`` beside the artifact, or a bare name.

``..`` is not representable, so a library can never escape its own directory.
"""

DEFAULT_MODEL_ROLE: Final[str] = "default"
"""Model role an artifact binds to when it declares none."""

RETRIES_DEFAULT: Final[int] = 2
RETRIES_MIN: Final[int] = 0
RETRIES_MAX: Final[int] = 10

RETRY_AXES_DESCRIPTION: Final[str] = (
    "Attempts an agent may spend. One number, three distinct axes. "
    "(1) Loom's provider retries replay the whole run after an infrastructure "
    "failure, and only for an agent holding no capability: replaying a run that "
    "may already have invoked an application use case would invoke it twice, so "
    "any capability disables this axis whatever the value. "
    "(2) The engine's tool-call budget replays a tool call the model got wrong, "
    "inside the same run. "
    "(3) The engine's output-validation budget asks the model again for an "
    "answer an output_check rejected, inside the same run. Declaring an "
    "output_check floors this axis at one, so a check can always correct once."
)
"""The three retry axes ``retries`` governs, written once.

This is the single wording: the published JSON Schema emits it as the
``policies.retries`` description, and the two prose sites that describe the
field — :class:`PolicySpec` and
:mod:`loom.ai.engines.pydantic_ai._spec` — point here instead of restating it,
so the three sites cannot drift apart.
"""

TOOL_TIMEOUT_MS_DEFAULT: Final[int] = 20000
TOOL_TIMEOUT_MS_MIN: Final[int] = 100
TOOL_TIMEOUT_MS_MAX: Final[int] = 600000

MAX_ITERATIONS_DEFAULT: Final[int] = 12
MAX_ITERATIONS_MIN: Final[int] = 1
MAX_ITERATIONS_MAX: Final[int] = 100

RUN_TIMEOUT_MS_DEFAULT: Final[int] = 120000
RUN_TIMEOUT_MS_MIN: Final[int] = 1000
RUN_TIMEOUT_MS_MAX: Final[int] = 1800000

MAX_HISTORY_BYTES_DEFAULT: Final[int] = 1_048_576
MAX_HISTORY_BYTES_MIN: Final[int] = 1_024
MAX_HISTORY_BYTES_MAX: Final[int] = 67_108_864

NativeToolName = Literal["web_search", "web_fetch", "code_execution"]
"""Tool the model provider runs in its own infrastructure, by its stable v1 name.

Each name is the ``kind`` the engine's native-tool class reports for itself, so
an artifact names the tool and the engine resolves the class. A name published
here is part of the v1 format forever.
"""

NATIVE_TOOLS: Final[tuple[NativeToolName, ...]] = get_args(NativeToolName)
"""Values ``NativeCapability.tool`` accepts, derived from :data:`NativeToolName`."""

_SymbolRef = Annotated[str, msgspec.Meta(pattern=SYMBOL_REF_PATTERN)]
_NonEmptyStr = Annotated[str, msgspec.Meta(min_length=1)]


class JsonSchemaOutput(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    tag="json_schema",
    tag_field="kind",
):
    """Structured answer described by an inline JSON Schema object.

    Canonical output form: what a generator emits.

    Args:
        schema: JSON Schema object describing the required answer.
    """

    schema: Mapping[str, Any]


class TypeRefOutput(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    tag="type_ref",
    tag_field="kind",
):
    """Structured answer described by an application type.

    Shortcut for hand-written applications; the reference is resolved at
    compile time.

    Args:
        ref: ``module:Symbol`` reference to the answer type.
    """

    ref: _SymbolRef


OutputSpec = JsonSchemaOutput | TypeRefOutput
"""Union of every supported output declaration, tagged on ``kind``."""


class OutputHookSpec(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
):
    """Use case the runtime executes once per completed run, with the validated output.

    The key uses the same vocabulary as :attr:`UsecaseCapability.keys` and is
    resolved against the same registry at compile time. The model never sees
    it: it is not a tool, and it never enters the instructions.

    The hook's own Command decides what else the run offers it, and declaring
    a field is the whole opt-in — this block gains no key for either:

    * ``tool_calls``: the run's tool traffic as
      :class:`~loom.ai.abc.ToolCallRecord` values, in call order. A record
      carries loom's own outcome vocabulary and never what the tool returned,
      so a hook that needs the data re-reads it through its own repositories.
      A ``kind: native`` tool is executed by the provider, emits no function
      events, and therefore never appears in the summary.
    * ``messages``: the run's new messages in the engine's serialised form,
      still tied to a conversation — ``None`` unless the run carried a
      ``conversation_id``.

    Args:
        usecase: Use-case key of the registry to execute with the validated output.
    """

    usecase: _NonEmptyStr


class ConversationSpec(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
):
    """Use case the runtime executes before a run that carries a ``conversation_id``.

    It returns the prior history of that conversation as opaque bytes in the
    engine's serialised form, or ``None`` on the first turn. The key uses the
    same vocabulary as :attr:`UsecaseCapability.keys` and is resolved against
    the same registry at compile time. The model never sees it: it is not a
    tool, and it never enters the instructions.

    Args:
        usecase: Use-case key of the registry that loads the prior history.
    """

    usecase: _NonEmptyStr


class DynamicInstructionsSpec(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
):
    """Application code contributing instructions to each request.

    A block rather than a bare reference, because it genuinely has two parts,
    and the same shape a ``kind: python`` capability already uses: a factory
    called once at build as ``factory(context, **params)``, returning the
    provider called once per **model request**.

    Per request, not per run: the engine rebuilds the instructions before every
    request it makes to the model, so a run that calls one tool calls the
    provider twice and a run reaching ``max_iterations`` calls it that many
    times. That multiplier is why the provider must be synchronous and do no
    I/O, and why a provider whose text varies between calls sends the model
    different instructions inside one run — the author's decision, and the
    author's to defend.

    It never replaces :attr:`AgentSpecV1.instructions`, which stays mandatory
    and literal: the literal composes first and the provider's text is
    appended to it.

    Args:
        factory: ``module:factory`` called once at build as
            ``factory(context, **params)``, returning an
            :data:`~loom.ai.abc.InstructionsProvider` called once per model
            request. A factory, never a constructed provider.
        params: Nested block passed to the factory as keyword arguments. The
            names are validated against the factory's signature at compile;
            the values are decoded YAML, not validated. Settings, never secrets.
    """

    factory: _SymbolRef
    params: dict[str, Any] = msgspec.field(default_factory=dict)


class UsecaseCapability(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    tag="usecase",
    tag_field="kind",
):
    """Explicitly granted business operations.

    Args:
        keys: Use-case keys granted to the agent. Never expanded automatically.
    """

    keys: Annotated[tuple[_NonEmptyStr, ...], msgspec.Meta(min_length=1)]


class SqlCapability(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    tag="sql",
    tag_field="kind",
):
    """Read-only access to a named SQL connection.

    Result bounds are mandatory: an unbounded query is not representable
    (FR-046b).

    Args:
        connection:       Named connection; compilation fails unless it is read-only.
        max_rows:         Maximum number of rows a single query may return.
        max_result_bytes: Maximum size of a single query result.
    """

    connection: _NonEmptyStr
    max_rows: Annotated[int, msgspec.Meta(ge=1)]
    max_result_bytes: Annotated[int, msgspec.Meta(ge=1)]


class McpCapability(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    tag="mcp",
    tag_field="kind",
):
    """Tools served by a named remote MCP server.

    The artifact *names* the server; it never locates it. Where the server
    lives, how to authenticate to it and how long to wait are deployment facts
    read from ``ai.mcp_servers``, so the same artifact moves between
    environments unchanged.

    Args:
        server:  Named server, resolved from ``ai.mcp_servers``.
        include: Tool names or glob patterns to expose; empty means all.
        exclude: Tool names or glob patterns to omit, applied after ``include``.
    """

    server: _NonEmptyStr
    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()


class SkillsCapability(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    tag="skills",
    tag_field="kind",
):
    """Packaged prompt material from one skill library.

    The artifact *names* a library; it never carries an absolute path.
    ``./name`` resolves beside the artifact and travels with it, a bare name
    resolves against ``ai.skills_root``, and ``..`` is not representable, so a
    library can never escape its own directory.

    Args:
        library: Skill library, either ``./name`` or a bare name.
        include: Skill names or glob patterns to expose; empty means all.
        exclude: Skill names or glob patterns to omit, applied after ``include``.
    """

    library: Annotated[str, msgspec.Meta(min_length=1, pattern=SKILLS_LIBRARY_PATTERN)]
    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()


class PythonCapability(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    tag="python",
    tag_field="kind",
):
    """Toolset built by application-owned Python code.

    Args:
        factory: ``module:factory`` called once at build as ``factory(context, **params)``.
            A factory, never a constructed object.
        params: Nested block passed to the factory as keyword arguments. The
            names are validated against the factory's signature at compile;
            the values are decoded YAML, not validated. Settings, never secrets.
    """

    factory: _SymbolRef
    params: dict[str, Any] = msgspec.field(default_factory=dict)


class A2ACapability(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    tag="a2a",
    tag_field="kind",
):
    """Delegation to a named remote agent reachable over A2A.

    The artifact *names* the agent; ``ai.a2a_agents`` knows where it is and how
    to authenticate to it.

    Args:
        agent:   Named remote agent, resolved from ``ai.a2a_agents``.
        include: Skill names or glob patterns to expose; empty means all.
        exclude: Skill names or glob patterns to omit, applied after ``include``.
    """

    agent: _NonEmptyStr
    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()


class NativeCapability(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    tag="native",
    tag_field="kind",
):
    """Tool the model provider executes in its own infrastructure.

    The artifact *names* the tool; whether the model bound to ``model_role``
    admits it is a deployment fact checked at compile time, and the provider
    runs it, so no toolset, timeout or credential of loom is involved.

    Args:
        tool: Provider tool, one of :data:`NATIVE_TOOLS`.
    """

    tool: NativeToolName


CapabilitySpec = (
    UsecaseCapability
    | SqlCapability
    | McpCapability
    | SkillsCapability
    | PythonCapability
    | A2ACapability
    | NativeCapability
)
"""Union of every supported capability declaration, tagged on ``kind``."""


class PolicySpec(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
):
    """Execution limits an agent runs under.

    Ranges are published as module constants and enforced by a later
    compilation phase, so an out-of-range value is reported as a coded issue
    rather than as a decoding failure.

    Args:
        retries:         Attempts an agent may spend, across the three axes
            :data:`RETRY_AXES_DESCRIPTION` names. It is one operator-facing
            knob and not one enforcement: a capability-bearing agent never
            replays a provider call, whatever this says.
        tool_timeout_ms: Deadline of a single tool call.
        max_iterations:  Maximum reason/act iterations in one run.
        run_timeout_ms:  Deadline of a whole run.
        max_history_bytes: Ceiling, in bytes, of the history a ``conversation``
            loader may return; a longer one fails the run.
    """

    retries: int = RETRIES_DEFAULT
    tool_timeout_ms: int = TOOL_TIMEOUT_MS_DEFAULT
    max_iterations: int = MAX_ITERATIONS_DEFAULT
    run_timeout_ms: int = RUN_TIMEOUT_MS_DEFAULT
    max_history_bytes: int = MAX_HISTORY_BYTES_DEFAULT


class AgentSpecV1(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
):
    """Authored agent definition, format version 1.

    Field order mirrors the published JSON Schema so an artifact reads the same
    way as the contract it validates against.

    Args:
        spec_version:  Format version; always ``1`` for this struct.
        name:          Unique agent name within the application.
        description:   What the agent does. Published in the A2A card.
        instructions:  Instructions the agent follows. Never published, and
            never a place to encode authorization.
        dynamic_instructions: Application code contributing instructions per
            request, or ``None`` when the literal is the whole prompt. The
            literal composes first and this text is appended to it, so this
            never replaces ``instructions``.
        model_role:    Logical model role bound to a concrete provider and
            model by deployment configuration.
        output:        Declaration of the structured answer the agent returns.
        output_check:  ``module:symbol`` reference to the rule the answer must
            satisfy beyond its schema, or ``None`` when the schema is the whole
            contract. A bare reference rather than a block: the value has one
            meaning and no tag to read. The symbol satisfies
            :data:`~loom.ai.abc.OutputCheck` — it returns ``None`` to accept and
            the text the model must read to correct itself to reject.
        on_output:     Use case executed once per completed run with the
            validated output; ``None`` when the artifact declares no hook.
        conversation:  Use case executed before a run that carries a
            ``conversation_id`` to load the prior history; ``None`` when the
            artifact declares no loader.
        capabilities:  Explicitly granted capabilities; empty by default.
        policies:      Execution limits; documented defaults when omitted.
        metadata:      Free-form string labels carried alongside the agent.
    """

    spec_version: Annotated[int, msgspec.Meta(ge=SPEC_VERSION_V1, le=SPEC_VERSION_V1)]
    name: Annotated[str, msgspec.Meta(pattern=AGENT_NAME_PATTERN)]
    description: _NonEmptyStr
    instructions: _NonEmptyStr
    dynamic_instructions: DynamicInstructionsSpec | None = None
    model_role: Annotated[str, msgspec.Meta(pattern=MODEL_ROLE_PATTERN)] = DEFAULT_MODEL_ROLE
    output: OutputSpec
    output_check: _SymbolRef | None = None
    on_output: OutputHookSpec | None = None
    conversation: ConversationSpec | None = None
    capabilities: tuple[CapabilitySpec, ...] = ()
    policies: PolicySpec = msgspec.field(default_factory=PolicySpec)
    metadata: Mapping[str, str] = msgspec.field(default_factory=dict)
