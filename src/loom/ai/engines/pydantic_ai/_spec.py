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
"""

from __future__ import annotations

from typing import Any

from pydantic_ai import AgentSpec

from loom.ai.compiler import AgentPlan


def build_agent_spec(plan: AgentPlan) -> AgentSpec:
    """Project a compiled plan onto the engine's own spec type.

    ``output_schema`` instructs the model on the shape to produce; it does not
    validate the answer (research R-004), which is why the plan's decoder owns
    validation at the boundary (see ``_output``).

    Args:
        plan: Compiled agent plan.

    Returns:
        The engine spec ``Agent.from_spec()`` consumes.
    """
    schema: dict[str, Any] = dict(plan.output.schema)
    return AgentSpec(
        name=plan.name,
        description=plan.description,
        instructions=plan.instructions,
        output_schema=schema,
        retries=plan.policies.retries,
    )
