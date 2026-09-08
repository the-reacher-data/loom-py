"""Agent compiler: validated artifact in, immutable plan out.

Offline by construction (FR-010): nothing in this package touches the
network, reads credentials or loads entry points — ``supported_kinds``
arrives as a plain value resolved by the bootstrap.  Every phase accumulates
its issues and one :class:`AgentCompilationError` reports them all (FR-011).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from types import MappingProxyType

from loom.ai.abc import NativeToolSupport, OutputCheck
from loom.ai.compiler._plan import (
    AgentPlan,
    CompiledCapability,
    CompiledConversation,
    CompiledDynamicInstructions,
    CompiledOutput,
    CompiledOutputHook,
)
from loom.ai.compiler.phases._capabilities import compile_capabilities
from loom.ai.compiler.phases._conversation import compile_conversation
from loom.ai.compiler.phases._dynamic_instructions import compile_dynamic_instructions
from loom.ai.compiler.phases._hook import compile_output_hook
from loom.ai.compiler.phases._limits import validate_policies
from loom.ai.compiler.phases._model_role import resolve_model_role
from loom.ai.compiler.phases._output import compile_output
from loom.ai.compiler.phases._output_check import compile_output_check
from loom.ai.config import AiConfig
from loom.ai.declarative import AgentSpecV1, DecodedSpec
from loom.ai.errors import (
    AgentCompilationError,
    AgentCompilationIssue,
    agent_name_duplicate,
)
from loom.ai.inference import InferenceTarget
from loom.core.sql.config import SqlConfig
from loom.core.use_case.registry import UseCaseRegistry

_UNNAMED_SOURCE = "<in-memory spec>"

_CompileResult = tuple[AgentPlan | None, list[AgentCompilationIssue]]


@dataclass(frozen=True, slots=True)
class _Resolved:
    """Everything the phases resolved for one spec, gathered for the plan.

    The phases already ran and reported clean; this only carries their results
    across to :meth:`AgentCompiler._build_plan` so the plan is built from two
    values — the artifact and what compiling it produced — instead of from a
    positional list that grows with every optional field the format gains.

    Attributes:
        inference: Resolved model binding of the agent's role.
        output: Structured-output contract with its built decoder.
        output_check: Imported answer rule, when the artifact declares one.
        dynamic_instructions: Imported prompt factory, when declared.
        capabilities: Compiled capabilities with resolved handles.
        on_output: Compiled output hook, when declared.
        conversation: Compiled conversation loader, when declared.
    """

    inference: InferenceTarget
    output: CompiledOutput
    output_check: OutputCheck | None
    dynamic_instructions: CompiledDynamicInstructions | None
    capabilities: tuple[CompiledCapability, ...]
    on_output: CompiledOutputHook | None
    conversation: CompiledConversation | None


class AgentCompiler:
    """Compiles authored agent artifacts into immutable plans.

    Runs every validation phase over every spec, accumulates all the issues
    found and raises a single :class:`AgentCompilationError`, so a generator
    sees the whole picture at once (FR-011, SC-003).

    Args:
        config: Deployment configuration of the AI pillar.
        registry: Use-case registry the ``usecase`` grants resolve against.
        supported_kinds: Capability kinds the configured engine serves,
            resolved by the bootstrap and passed as a plain value — the
            compiler never imports an engine.
        sql: Data-layer configuration; ``None`` fails every ``sql`` grant
            with ``SQL_CONFIG_MISSING`` instead of skipping silently.
        native_tools: Oracle answering which provider tools a model binding
            admits; resolved from the engine by the bootstrap, never imported here.
    """

    def __init__(
        self,
        *,
        config: AiConfig,
        registry: UseCaseRegistry,
        supported_kinds: frozenset[str],
        sql: SqlConfig | None = None,
        native_tools: NativeToolSupport | None = None,
    ) -> None:
        self._config = config
        self._registry = registry
        self._supported_kinds = supported_kinds
        self._sql = sql
        self._native_tools = native_tools

    def compile(self, spec: AgentSpecV1, *, source_path: str | None = None) -> AgentPlan:
        """Validate one spec statically and return its immutable plan.

        Performs no network access, requires no model credentials and spends
        no tokens.

        Args:
            spec: Decoded artifact to compile.
            source_path: Artifact provenance; when given, every issue points
                at it as its ``component``.

        Returns:
            The compiled plan.

        Raises:
            AgentCompilationError: Aggregating one issue per problem found.
        """
        plan, issues = self._compile_one(spec, source_path)
        if plan is None:
            raise AgentCompilationError(issues)
        return plan

    def compile_all(self, specs: Sequence[AgentSpecV1 | DecodedSpec]) -> tuple[AgentPlan, ...]:
        """Compile a whole application, accumulating issues across specs.

        Duplicate agent names are an application-level fault: a single spec is
        always unique by itself, so ``AGENT_NAME_DUPLICATE`` can only be
        detected — and is only reported — here.

        Args:
            specs: Decoded artifacts of the application. Passing the
                :class:`~loom.ai.declarative.DecodedSpec` values as returned by
                ``load_specs`` keeps each artifact's path, which a ``./`` skill
                library resolves against; a bare
                :class:`~loom.ai.declarative.AgentSpecV1` has no path.

        Returns:
            One plan per spec, in input order.

        Raises:
            AgentCompilationError: Aggregating every issue of every spec.
        """
        entries = [_as_entry(item) for item in specs]
        issues: list[AgentCompilationIssue] = _duplicate_name_issues(entries)
        plans: list[AgentPlan] = []
        for spec, source_path in entries:
            plan, spec_issues = self._compile_one(spec, source_path)
            issues.extend(spec_issues)
            if plan is not None:
                plans.append(plan)
        if issues:
            raise AgentCompilationError(issues)
        return tuple(plans)

    def _compile_one(self, spec: AgentSpecV1, source_path: str | None) -> _CompileResult:
        component = source_path if source_path is not None else spec.name
        resolved, issues = self._resolve(spec, component, source_path)
        if resolved is None:
            return None, issues
        return self._build_plan(spec, resolved, source_path), []

    def _resolve(
        self, spec: AgentSpecV1, component: str, source_path: str | None
    ) -> tuple[_Resolved | None, list[AgentCompilationIssue]]:
        """Run every phase over one spec, accumulating the issues of all of them.

        Returns ``None`` for the resolution as soon as anything was reported,
        so the plan is only ever built out of a spec that compiled clean.
        """
        issues: list[AgentCompilationIssue] = []
        output, output_issues = compile_output(spec.output, component)
        issues.extend(output_issues)
        output_check, check_issues = compile_output_check(spec.output_check, component)
        issues.extend(check_issues)
        dynamic, dynamic_issues = compile_dynamic_instructions(spec.dynamic_instructions, component)
        issues.extend(dynamic_issues)
        on_output, hook_issues = compile_output_hook(
            spec, component=component, registry=self._registry
        )
        issues.extend(hook_issues)
        conversation, conversation_issues = compile_conversation(
            spec, component=component, registry=self._registry
        )
        issues.extend(conversation_issues)
        issues.extend(validate_policies(spec.policies, component))
        inference, role_issues = resolve_model_role(spec.model_role, self._config.models, component)
        issues.extend(role_issues)
        capabilities, capability_issues = self._resolve_capabilities(
            spec, component, source_path, inference
        )
        issues.extend(capability_issues)
        if issues or output is None or inference is None:
            return None, issues
        return (
            _Resolved(
                inference=inference,
                output=output,
                output_check=output_check,
                dynamic_instructions=dynamic,
                capabilities=capabilities,
                on_output=on_output,
                conversation=conversation,
            ),
            issues,
        )

    def _resolve_capabilities(
        self,
        spec: AgentSpecV1,
        component: str,
        source_path: str | None,
        inference: InferenceTarget | None,
    ) -> tuple[tuple[CompiledCapability, ...], list[AgentCompilationIssue]]:
        """Compile the grants against this compiler's deployment inputs."""
        return compile_capabilities(
            spec,
            component=component,
            config=self._config,
            registry=self._registry,
            sql=self._sql,
            supported_kinds=self._supported_kinds,
            inference=inference,
            native_tools=self._native_tools,
            source_path=source_path,
        )

    @staticmethod
    def _build_plan(spec: AgentSpecV1, resolved: _Resolved, source_path: str | None) -> AgentPlan:
        return AgentPlan(
            name=spec.name,
            description=spec.description,
            instructions=spec.instructions,
            dynamic_instructions=resolved.dynamic_instructions,
            spec_version=spec.spec_version,
            inference=resolved.inference,
            output=resolved.output,
            output_check=resolved.output_check,
            capabilities=resolved.capabilities,
            policies=spec.policies,
            on_output=resolved.on_output,
            conversation=resolved.conversation,
            metadata=MappingProxyType(dict(spec.metadata)),
            source_path=source_path,
        )


def _as_entry(item: AgentSpecV1 | DecodedSpec) -> tuple[AgentSpecV1, str | None]:
    if isinstance(item, DecodedSpec):
        return item.spec, item.source_path
    return item, None


def _duplicate_name_issues(
    entries: Sequence[tuple[AgentSpecV1, str | None]],
) -> list[AgentCompilationIssue]:
    """Report every name declared twice, naming the artifacts that declare it.

    The issue's value is its provenance: an operator needs the files to open,
    not the name repeated once per occurrence. A spec handed over without a
    path (a bare :class:`~loom.ai.declarative.AgentSpecV1`) has no artifact to
    name, so it is reported as an explicit placeholder rather than silently
    dropped, which would leave the list shorter than the occurrence count.
    """
    sources: dict[str, list[str]] = defaultdict(list)
    for spec, source_path in entries:
        sources[spec.name].append(source_path if source_path is not None else _UNNAMED_SOURCE)
    return [agent_name_duplicate(name, paths) for name, paths in sources.items() if len(paths) > 1]
