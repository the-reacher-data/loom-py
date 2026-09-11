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

``instructions`` and ``description`` are never set on the spec built here.
Both travel to ``Agent.from_spec`` as keywords instead
(:func:`~loom.ai.engines.pydantic_ai._instructions.build_instructions`,
``create_engine``), the technique already used for ``output_type``. Two
reasons, one for each field:

* ``pydantic_ai.agent.spec.AgentSpec.instructions`` is typed
  ``TemplateStr[Any] | str | list[TemplateStr[Any] | str] | None`` and admits
  neither an ``InstructionPart`` (needed to carry a block's ``name``) nor a
  callable (needed to render a templated block); ``Agent.from_spec``'s own
  ``instructions=`` keyword is typed ``AgentInstructions[Any]``, which
  admits both.
* ``AgentSpec.description`` is typed ``TemplateStr[Any] | str | None``, and
  that field's own validator (``TemplateStr.__get_pydantic_core_schema__``)
  compiles any string containing ``{{`` into a template, later rendered
  against the whole dependency bundle by ``Agent.render_description`` and
  attached to the run span as ``gen_ai.agent.description``. Against the
  real bundle that render raises; against a bundle whose fields all
  serialise it leaks the caller's own subject onto the span. A plain
  ``str`` keyword bypasses that validator entirely — ``Agent.from_spec``
  merges keyword-first for ``description`` — so the description reaches
  the span exactly as authored. loom does not offer a templated
  ``description``: only ``instructions`` renders against state.
"""

from __future__ import annotations

from typing import Any, assert_never, cast

from pydantic_ai import AgentSpec, NativeOutput, StructuredDict, ToolOutput

from loom.ai.compiler import AgentPlan
from loom.ai.inference import OutputMode


def build_agent_spec(plan: AgentPlan) -> AgentSpec:
    """Project a compiled plan onto the engine's own spec type.

    ``output_schema`` instructs the model on the shape to produce; it does not
    validate the answer (research R-004), which is why the plan's decoder owns
    validation at the boundary (see ``_output``). How the engine asks for that
    shape (tool call or native structured output) is not part of the spec;
    :func:`build_output_type` overrides it when the binding pins a mode.
    ``instructions`` and ``description`` are left unset; see this module's
    docstring for why both travel as keywords instead.

    Args:
        plan: Compiled agent plan.

    Returns:
        The engine spec ``Agent.from_spec()`` consumes.
    """
    schema: dict[str, Any] = dict(plan.output.schema)
    return AgentSpec(
        name=plan.name,
        output_schema=schema,
        retries=plan.policies.retries,
    )


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
