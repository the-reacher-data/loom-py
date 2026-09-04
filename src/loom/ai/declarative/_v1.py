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
from typing import Annotated, Any, Final

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

TOOL_TIMEOUT_MS_DEFAULT: Final[int] = 20000
TOOL_TIMEOUT_MS_MIN: Final[int] = 100
TOOL_TIMEOUT_MS_MAX: Final[int] = 600000

MAX_ITERATIONS_DEFAULT: Final[int] = 12
MAX_ITERATIONS_MIN: Final[int] = 1
MAX_ITERATIONS_MAX: Final[int] = 100

RUN_TIMEOUT_MS_DEFAULT: Final[int] = 120000
RUN_TIMEOUT_MS_MIN: Final[int] = 1000
RUN_TIMEOUT_MS_MAX: Final[int] = 1800000

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

    Args:
        usecase: Use-case key of the registry to execute with the validated output.
    """

    usecase: _NonEmptyStr


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
        factory: ``module:factory`` satisfying the toolset factory protocol.
            A factory, never a constructed object.
    """

    factory: _SymbolRef


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


CapabilitySpec = (
    UsecaseCapability
    | SqlCapability
    | McpCapability
    | SkillsCapability
    | PythonCapability
    | A2ACapability
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
        retries:         Attempts the engine makes before a failure is final.
        tool_timeout_ms: Deadline of a single tool call.
        max_iterations:  Maximum reason/act iterations in one run.
        run_timeout_ms:  Deadline of a whole run.
    """

    retries: int = RETRIES_DEFAULT
    tool_timeout_ms: int = TOOL_TIMEOUT_MS_DEFAULT
    max_iterations: int = MAX_ITERATIONS_DEFAULT
    run_timeout_ms: int = RUN_TIMEOUT_MS_DEFAULT


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
        model_role:    Logical model role bound to a concrete provider and
            model by deployment configuration.
        output:        Declaration of the structured answer the agent returns.
        on_output:     Use case executed once per completed run with the
            validated output; ``None`` when the artifact declares no hook.
        capabilities:  Explicitly granted capabilities; empty by default.
        policies:      Execution limits; documented defaults when omitted.
        metadata:      Free-form string labels carried alongside the agent.
    """

    spec_version: Annotated[int, msgspec.Meta(ge=SPEC_VERSION_V1, le=SPEC_VERSION_V1)]
    name: Annotated[str, msgspec.Meta(pattern=AGENT_NAME_PATTERN)]
    description: _NonEmptyStr
    instructions: _NonEmptyStr
    model_role: Annotated[str, msgspec.Meta(pattern=MODEL_ROLE_PATTERN)] = DEFAULT_MODEL_ROLE
    output: OutputSpec
    on_output: OutputHookSpec | None = None
    capabilities: tuple[CapabilitySpec, ...] = ()
    policies: PolicySpec = msgspec.field(default_factory=PolicySpec)
    metadata: Mapping[str, str] = msgspec.field(default_factory=dict)
