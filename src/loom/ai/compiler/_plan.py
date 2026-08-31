"""Compiled agent plan structs (Tier 3 of the data model).

The plan is the only artifact-derived input to every downstream stage
(FR-014): every string reference whose resolution is possible offline dies at
compile, so the plan carries resolved handles — the registered use-case types,
the SQL connection config, the imported toolset factory — never names.

Secret containment (invariant 4): the plan carries no literal secret.  The
one secret-bearing struct it embeds, :class:`~loom.ai.inference.InferenceTarget`,
redacts its references in ``repr`` and refuses msgspec encoding; the built
decoder in :class:`CompiledOutput` is likewise not msgspec-encodable, so an
accidental wire encode of a plan raises instead of leaking.

The plan is only ever built in memory by the compiler; it is never decoded
from JSON, which is why fields may hold arbitrary runtime handles.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar

import msgspec

from loom.ai.abc import ToolsetFactory
from loom.ai.declarative import PolicySpec, ToolFilter
from loom.ai.inference import InferenceTarget
from loom.core.engine.compilable import Compilable
from loom.core.model import LoomFrozenStruct
from loom.core.sql.config import SqlConnectionConfig


class CompiledOutput(LoomFrozenStruct, frozen=True, kw_only=True):
    """Structured-output contract with a decoder built at compile time.

    Interpreting the schema per response would be per-item reflection, so the
    decoder is constructed exactly once, at compile (research R-004,
    invariant 5).  The decode is strict: unknown fields are rejected, which is
    what makes returning the validated bytes unchanged safe.

    Attributes:
        schema: JSON Schema object handed to the model.
        decoder: Built ``msgspec`` JSON decoder producing the answer type.
    """

    schema: Mapping[str, Any]
    # ``Any`` type parameter: the decoded type is derived from the artifact's
    # schema at compile time, so it cannot be named statically.
    decoder: msgspec.json.Decoder[Any]


class CompiledUsecaseCapability(LoomFrozenStruct, frozen=True, kw_only=True):
    """Granted business operations resolved against the use-case registry.

    Attributes:
        keys: Granted use-case keys, carried for the self-description.
        use_cases: Registered use-case types, one per key, in key order.
    """

    kind: ClassVar[str] = "usecase"

    keys: tuple[str, ...]
    use_cases: tuple[type[Compilable], ...]


class CompiledSqlCapability(LoomFrozenStruct, frozen=True, kw_only=True):
    """Read-only SQL access resolved to its connection configuration.

    Attributes:
        connection: Connection name, carried for the self-description.
        config: Validated read-only connection configuration.
        max_rows: Maximum rows one query may return.
        max_result_bytes: Maximum size of one query result.
    """

    kind: ClassVar[str] = "sql"

    connection: str
    config: SqlConnectionConfig
    max_rows: int
    max_result_bytes: int


class CompiledMcpCapability(LoomFrozenStruct, frozen=True, kw_only=True):
    """Statically validated MCP server grant.

    The URL resolves over the network in ``__aenter__`` — it is one of the
    three declared exceptions to "strings die at compile" (invariant 3).

    Attributes:
        url: Validated ``https://`` server URL, free of inline credentials.
        tool_filter: Optional allow/deny lists over the exposed tools.
        headers_ref: Reference to deployment-resolved headers; never a secret.
    """

    kind: ClassVar[str] = "mcp"

    url: str
    tool_filter: ToolFilter | None = None
    headers_ref: str | None = None


class CompiledSkillsCapability(LoomFrozenStruct, frozen=True, kw_only=True):
    """Packaged skills resolved to their imported objects.

    Attributes:
        refs: ``module:symbol`` references, carried for the self-description.
        skills: Imported skill objects, one per reference, in order.
    """

    kind: ClassVar[str] = "skills"

    refs: tuple[str, ...]
    skills: tuple[object, ...]


class CompiledPythonCapability(LoomFrozenStruct, frozen=True, kw_only=True):
    """Application toolset factory resolved to an importable callable.

    Attributes:
        factory_ref: ``module:factory`` reference, for the self-description.
        factory: Imported factory satisfying :class:`~loom.ai.abc.ToolsetFactory`.
    """

    kind: ClassVar[str] = "python"

    factory_ref: str
    factory: ToolsetFactory


class CompiledA2ACapability(LoomFrozenStruct, frozen=True, kw_only=True):
    """Statically validated remote-agent grant.

    The URL resolves over the network in ``__aenter__`` — it is one of the
    three declared exceptions to "strings die at compile" (invariant 3).

    Attributes:
        url: Validated ``https://`` remote agent URL, free of credentials.
        skills: Optional subset of the remote agent's skills to use.
    """

    kind: ClassVar[str] = "a2a"

    url: str
    skills: tuple[str, ...] | None = None


CompiledCapability = (
    CompiledUsecaseCapability
    | CompiledSqlCapability
    | CompiledMcpCapability
    | CompiledSkillsCapability
    | CompiledPythonCapability
    | CompiledA2ACapability
)
"""Union of every compiled capability; each exposes its ``kind`` and handle."""


class AgentPlan(LoomFrozenStruct, frozen=True, kw_only=True):
    """Immutable compiled agent, the only input to every downstream stage.

    Attributes:
        name: Unique agent name within the application.
        description: What the agent does; published in the A2A card.
        instructions: Instructions the agent follows; never published.
        spec_version: Artifact format version, retained for self-description.
        inference: Resolved model binding; one binding, no fallback (FR-019a).
        output: Structured-output contract with its built decoder.
        capabilities: Compiled capabilities with resolved handles.
        policies: Validated execution limits.
        metadata: Free-form string labels carried alongside the agent.
        source_path: Artifact provenance for error messages, when known.
    """

    name: str
    description: str
    instructions: str
    spec_version: int
    inference: InferenceTarget
    output: CompiledOutput
    capabilities: tuple[CompiledCapability, ...] = ()
    policies: PolicySpec
    metadata: Mapping[str, str]
    source_path: str | None = None
