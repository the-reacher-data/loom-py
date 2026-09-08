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
* ``instructions`` / ``description`` — the engine types both as
  ``TemplateStr | str``, a union whose discriminator is literally the presence
  of ``{{`` in the text, and whose template branch imports a templating package
  a default install does not carry. An artifact's instructions and description
  are literal by contract, so building the spec with them would turn a prompt
  that merely mentions braces into an ``ImportError`` at start-up. They travel
  as keyword arguments of ``Agent.from_spec()`` instead (``provider``), where
  the engine keeps a plain string untouched.
* ``tool_timeout_ms`` — enforced on loom's side, by
  ``_guards.capability_call`` around every granted tool and by
  ``AgentRuntime``'s own tool deadline. Those two agree: same code, same
  retry class, so which one fires first is not observable. Projecting it as
  the engine's ``tool_timeout`` too
  would race the two deadlines over the same value: whichever fired first
  decided the outcome, and the engine's own expiry is classified
  ``PROVIDER_UNAVAILABLE`` (retried) where loom's is ``TOOL_TIMEOUT`` (not
  retried). One value, two retry behaviours, chosen by the event loop.

``retries`` **is** projected, as the engine's split budget rather than as a
bare int: the engine keeps a tool-call axis and an output-validation axis, and
a bare int only sets both to the same number, which leaves an artifact
declaring an ``output_check`` with a check that cannot correct anything at
``retries: 0``. Projecting the pair explicitly is what lets
:func:`build_agent_spec` floor the output axis for such an artifact. For every
other artifact the projection is the engine's own normalisation of a bare int,
so the behaviour is unchanged.

The three axes the one artifact field governs — loom's provider retries, the
engine's tool-call budget and the engine's output-validation budget — are
described once, in
:data:`~loom.ai.declarative._v1.RETRY_AXES_DESCRIPTION`, which the published
JSON Schema emits and the policy field points at. Neither the runtime's
``plan.policies.retries + 1`` attempts (``_engine``) nor either engine budget
subsumes the others: they share one operator-facing knob on purpose.

``output_mode`` (``ai.models.<role>``) is projected next to the spec rather than
inside it: ``AgentSpec.output_schema`` has no mode field, so the mode travels as
``Agent.from_spec(output_type=...)`` via :func:`build_output_type`. Absent, the
engine keeps resolving the mode itself from ``output_schema``.
"""

from __future__ import annotations

from typing import Any, Final, assert_never, cast

from pydantic_ai import AgentRetries, AgentSpec, NativeOutput, StructuredDict, ToolOutput

from loom.ai.compiler import AgentPlan
from loom.ai.inference import OutputMode

_CHECKED_OUTPUT_RETRIES_FLOOR: Final[int] = 1
"""Output attempts an artifact declaring an ``output_check`` always gets.

A rejection is only useful if the model may answer again, so a declared zero is
raised to one. A declared value above it is left alone: the artifact asked for
more, not for less.
"""


def build_agent_spec(plan: AgentPlan) -> AgentSpec:
    """Project a compiled plan onto the engine's own spec type.

    ``output_schema`` instructs the model on the shape to produce; it does not
    validate the answer (research R-004), which is why the plan's decoder owns
    validation at the boundary (see ``_output``). How the engine asks for that
    shape (tool call or native structured output) is not part of the spec;
    :func:`build_output_type` overrides it when the binding pins a mode.

    The spec carries no ``instructions`` and no ``description``: both are
    template-typed on the engine side and travel as keyword arguments instead,
    for the reason this module's docstring gives.

    Args:
        plan: Compiled agent plan.

    Returns:
        The engine spec ``Agent.from_spec()`` consumes.
    """
    schema: dict[str, Any] = dict(plan.output.schema)
    return AgentSpec(
        name=plan.name,
        output_schema=schema,
        retries=_retry_budget(plan),
    )


def _retry_budget(plan: AgentPlan) -> AgentRetries:
    """Split the artifact's one ``retries`` value into the engine's two axes.

    Both axes carry the declared value, which is what the engine derives from a
    bare int itself, so nothing changes for an artifact without an output check.
    With one, the output axis is floored at
    :data:`_CHECKED_OUTPUT_RETRIES_FLOOR`: a check that cannot ask for a second
    answer can only fail runs.
    """
    declared = plan.policies.retries
    if plan.output_check is None:
        return AgentRetries(tools=declared, output=declared)
    return AgentRetries(tools=declared, output=max(declared, _CHECKED_OUTPUT_RETRIES_FLOOR))


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
