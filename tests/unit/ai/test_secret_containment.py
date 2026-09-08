"""Secret containment for ``loom.ai.inference.InferenceTarget`` (T025, 011/T204).

``InferenceTarget`` carries a ``credentials_ref`` and vendor ``options``; both
may point at (or parametrise) secrets. The struct must therefore redact them in
``__repr__``/``__str__``, and any msgspec encoding of the struct must either be
refused outright or omit those values in clear.

The containment is asserted on the compiled ``AgentPlan`` too, and not only on
the binding it embeds. The plan is the object every stage carries around, so it
is the one a stray ``log.info(plan)`` or a debug endpoint would reach for; and
it now closes over application code — the artifact's ``output_check`` and its
``dynamic_instructions`` factory — which makes "the plan is not encodable" a
property worth pinning rather than an accident of its current fields.

These tests pin FR-018 / data-model invariant 4: no secret-bearing value may
leak through a traceback repr or through an encoded plan.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import msgspec
import pytest
from msgspec import structs

from loom.ai.abc import InstructionsProvider, ToolsetContext
from loom.ai.compiler._plan import AgentPlan, CompiledDynamicInstructions
from loom.ai.inference import InferenceTarget
from tests.helpers.pydantic_ai_engine import make_plan

_CREDENTIALS_REF = "ref/to/secret-name"
_OPTION_SENTINEL = "SENTINEL_OPT_VALUE"


@pytest.fixture()
def target() -> InferenceTarget:
    """Build a target carrying both redactable fields."""
    return InferenceTarget(
        provider="openai",
        model="gpt-test",
        credentials_ref=_CREDENTIALS_REF,
        options={"api_key_param": _OPTION_SENTINEL},
    )


def test_repr_no_contiene_credentials_ref_cuando_esta_definido(
    target: InferenceTarget,
) -> None:
    """``repr`` must redact the credentials reference."""
    assert _CREDENTIALS_REF not in repr(target)


def test_repr_no_contiene_valores_de_options_cuando_estan_definidos(
    target: InferenceTarget,
) -> None:
    """``repr`` must redact vendor option values."""
    assert _OPTION_SENTINEL not in repr(target)


def test_str_no_contiene_credentials_ref_cuando_esta_definido(
    target: InferenceTarget,
) -> None:
    """``str`` must redact the credentials reference."""
    assert _CREDENTIALS_REF not in str(target)


def test_str_no_contiene_valores_de_options_cuando_estan_definidos(
    target: InferenceTarget,
) -> None:
    """``str`` must redact vendor option values."""
    assert _OPTION_SENTINEL not in str(target)


def test_encoding_json_no_expone_secretos_cuando_se_codifica_el_struct(
    target: InferenceTarget,
) -> None:
    """Encoding the struct either fails or omits the secret-bearing values.

    Both behaviours satisfy invariant 4: what must never happen is JSON bytes
    carrying the reference or the option value in clear.
    """
    try:
        encoded = msgspec.json.encode(target)
    except TypeError:
        return
    text = encoded.decode("utf-8")
    assert _CREDENTIALS_REF not in text and _OPTION_SENTINEL not in text


def _check(answer: Mapping[str, Any]) -> str | None:
    """An artifact's ``output_check``: application code the plan now carries."""
    del answer
    return None


def _factory(context: ToolsetContext) -> InstructionsProvider:
    """An artifact's ``dynamic_instructions`` factory: more application code."""
    del context
    return lambda request: request.prompt


@pytest.fixture()
def plan(target: InferenceTarget) -> AgentPlan:
    """A compiled plan binding the secret-bearing target and carrying both callables."""
    return structs.replace(
        make_plan(),
        inference=target,
        output_check=_check,
        dynamic_instructions=CompiledDynamicInstructions(
            factory_ref="tests:_factory", factory=_factory, params={}
        ),
    )


def test_el_plan_compilado_no_es_codificable(plan: AgentPlan) -> None:
    """A compiled plan cannot be serialised, so it cannot leak the binding it embeds.

    The plan holds a compiled decoder, a read-only mapping and two application
    callables — the artifact's ``output_check`` and its ``dynamic_instructions``
    factory. None of those is encodable, so a stage that tried to serialise the
    plan fails loudly instead of emitting the credentials reference in clear.

    A pinned regression guard rather than a mutation-falsifiable assertion: the
    property rests on several independent fields at once, so no single edit to
    production code flips it. What it catches is the plan drifting towards a
    plain data struct, which is the change that would make invariant 4 depend on
    the redaction alone.
    """
    with pytest.raises(TypeError):
        msgspec.json.encode(plan)


def test_el_repr_del_plan_no_expone_secretos_del_binding(plan: AgentPlan) -> None:
    """The plan is what a stray log line reaches for, so its repr must redact too."""
    rendered = repr(plan)

    assert _CREDENTIALS_REF not in rendered
    assert _OPTION_SENTINEL not in rendered


def test_las_referencias_a_codigo_de_la_aplicacion_sobreviven_en_el_plan(plan: AgentPlan) -> None:
    """Containment is redaction, never deletion: the plan still carries what it compiled."""
    assert plan.output_check is _check
    assert plan.dynamic_instructions is not None
    assert plan.dynamic_instructions.factory is _factory
