"""``AgentPlan`` → ``pydantic_ai.AgentSpec`` (native-inside).

The engine has a declarative form of its own, so the adapter translates into
it and lets ``Agent.from_spec()`` build the agent. Nothing here wraps an
engine primitive in a loom equivalent: the translation is a projection of the
plan's fields onto the engine's, and everything the plan carries that the
engine has no field for stays with loom.

What is deliberately **not** projected:

* ``metadata`` — ownership and cost-centre facts; never sent to a provider.
* ``model_role`` — a loom concept; the concrete model is bound separately
  (``_models``) and passed as an object, not as a ``provider:model`` string.
* ``run_timeout_ms`` / ``max_iterations`` — enforced by
  :class:`~loom.ai.runtime.AgentRuntime`, which supervises every stream; a
  second enforcement here would be a hidden, divergent limit.
* ``tool_timeout_ms`` — enforced on loom's side, by
  ``_guards.capability_call`` around every granted tool and by
  ``AgentRuntime``'s own tool deadline. Those two agree: same code, same
  retry class, so which one fires first is not observable. Projecting it as
  the engine's ``tool_timeout`` too
  would race the two deadlines over the same value: whichever fired first
  decided the outcome, and the engine's own expiry is classified
  ``PROVIDER_UNAVAILABLE`` (retried) where loom's is ``TOOL_TIMEOUT`` (not
  retried). One value, two retry behaviours, chosen by the event loop.

``retries`` **is** projected, and it is not the same axis as the runtime's
``plan.policies.retries + 1`` attempts (``_engine``): the engine's counter
replays a failed *tool* call inside one run, loom's replays a failed
*provider* call across runs. They share one artifact field on purpose — a
single operator-facing knob — but neither enforcement subsumes the other.

``output_mode`` (``ai.models.<role>``) is projected next to the spec rather than
inside it: ``AgentSpec.output_schema`` has no mode field, so the mode travels as
``Agent.from_spec(output_type=...)`` via :func:`build_output_type`. Absent, the
engine keeps resolving the mode itself from ``output_schema``.

``instructions`` projects each compiled block's literal text, in authored
order, through :func:`_literal_instructions`. A block's ``name`` does not
travel through this projection — ``AgentSpec.instructions`` has no field for
one — and a block declaring ``template`` fails the translation rather than
reaching the engine unrendered.
"""

from __future__ import annotations

from typing import Any, assert_never, cast

from pydantic_ai import AgentSpec, NativeOutput, StructuredDict, TemplateStr, ToolOutput

from loom.ai.compiler import AgentPlan
from loom.ai.errors import AgentCompilationError, instruction_block_invalid
from loom.ai.inference import OutputMode


def build_agent_spec(plan: AgentPlan) -> AgentSpec:
    """Project a compiled plan onto the engine's own spec type.

    ``output_schema`` instructs the model on the shape to produce; it does not
    validate the answer (research R-004), which is why the plan's decoder owns
    validation at the boundary (see ``_output``). How the engine asks for that
    shape (tool call or native structured output) is not part of the spec;
    :func:`build_output_type` overrides it when the binding pins a mode.

    Args:
        plan: Compiled agent plan.

    Returns:
        The engine spec ``Agent.from_spec()`` consumes.

    Raises:
        AgentCompilationError: A block declares ``template``; this projection
            carries literal text only.
    """
    schema: dict[str, Any] = dict(plan.output.schema)
    return AgentSpec(
        name=plan.name,
        description=plan.description,
        instructions=_literal_instructions(plan),
        output_schema=schema,
        retries=plan.policies.retries,
    )


def _literal_instructions(plan: AgentPlan) -> list[TemplateStr[Any] | str]:
    """Project the plan's instruction blocks onto the engine's literal list.

    Every block's ``text`` reaches the engine unchanged and in authored
    order. A block's ``name`` does not travel through this projection:
    ``AgentSpec.instructions`` has no field for it, and the engine only reads
    a block name off its own ``InstructionPart``, which this keyword route
    never builds — no promise is broken, since a block's name is never an
    engine-addressable id either way.

    Args:
        plan: Compiled agent plan.

    Returns:
        One string per instruction block, in authored order.

    Raises:
        AgentCompilationError: A block declares ``template``. Rendering one
            requires the engine's own template compiler, which this
            projection does not carry.
    """
    templated_issues = [
        instruction_block_invalid(
            plan.name,
            f"instruction block at position {index} declares template "
            f"'{block.template}', which this engine build does not render",
        )
        for index, block in enumerate(plan.instructions)
        if block.template is not None
    ]
    if templated_issues:
        raise AgentCompilationError(templated_issues)
    literal: list[TemplateStr[Any] | str] = [block.text for block in plan.instructions]
    return literal


def build_output_type(plan: AgentPlan) -> ToolOutput[Any] | NativeOutput[Any] | None:
    """Pin the engine's output mode when the model binding declares one.

    Wraps the plan's output schema in the engine's own marker, with no name
    or description, so the engine builds the same ``StructuredDict`` it would
    build from ``output_schema`` alone but with the mode fixed instead of
    resolved per provider. The value has already been validated against
    :data:`~loom.ai.inference.OUTPUT_MODES` when the config loaded.

    The dispatch is exhaustive rather than defaulted: an unhandled mode fails
    type checking here (``assert_never``) and raises at run time, so a value
    that reached this point without the config check — a plan built in
    process, a mode loom deliberately excludes such as ``prompted`` — cannot
    be silently served as ``native``.

    Args:
        plan: Compiled agent plan.

    Returns:
        ``ToolOutput`` for ``tool``, ``NativeOutput`` for ``native``, ``None``
        when the binding leaves the mode to the engine.

    Raises:
        AssertionError: The binding names a mode loom does not offer.
    """
    declared = plan.inference.output_mode
    if declared is None:
        return None
    # The struct field is ``str`` (msgspec would reject a Literal during the
    # decode, before the config check could name the role), so the narrowing
    # happens here, where the dispatch below either handles the value or
    # refuses it.
    mode = cast("OutputMode", declared)
    structured = StructuredDict(dict(plan.output.schema))
    if mode == "tool":
        return ToolOutput(structured)
    if mode == "native":
        return NativeOutput(structured)
    assert_never(mode)
