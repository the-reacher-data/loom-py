"""Dynamic-instructions phase (011/T304): the prompt factory resolves offline or is refused.

Three faults, three codes, all reported at compile time so a broken reference
can never surface as a run whose model silently loses half its instructions.

The codes matter as much as the refusals: the same signature check serves a
``kind: python`` capability, and a mis-signed instructions factory must never
be reported with a ``python`` capability code. That is what the last test here
pins, and it is the reason the shared check returns a named fault instead of a
ready-made issue.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from loom.ai.declarative import AgentSpecV1, DynamicInstructionsSpec
from loom.ai.errors import AgentCompilationIssue, AgentErrorCode

_VALID_REF = "myapp.agents.prompts:build_checklist"
_NO_CONTEXT_REF = "myapp.agents.prompts:build_checklist_without_context"
_NOT_CALLABLE_REF = "myapp.agents.prompts:NOT_CALLABLE"
_UNRESOLVABLE_REF = "no_such_pkg_zz.prompts:build_checklist"

pytestmark = pytest.mark.usefixtures("fake_myapp_path")


def _block(ref: str = _VALID_REF, **params: Any) -> DynamicInstructionsSpec:
    """Declare the block as an artifact would, with its parameters."""
    return DynamicInstructionsSpec(factory=ref, params=dict(params))


class TestRechazos:
    @pytest.mark.parametrize(
        ("block", "code", "field"),
        [
            (
                _block(_UNRESOLVABLE_REF),
                AgentErrorCode.DYNAMIC_INSTRUCTIONS_UNRESOLVABLE,
                "dynamic_instructions.factory",
            ),
            (
                _block(_NOT_CALLABLE_REF),
                AgentErrorCode.DYNAMIC_INSTRUCTIONS_NOT_CALLABLE,
                "dynamic_instructions.factory",
            ),
            (
                _block(_NO_CONTEXT_REF),
                AgentErrorCode.DYNAMIC_INSTRUCTIONS_NOT_CALLABLE,
                "dynamic_instructions.factory",
            ),
            (
                _block(unknown_setting=1),
                AgentErrorCode.DYNAMIC_INSTRUCTIONS_PARAMS_REJECTED,
                "dynamic_instructions.params",
            ),
        ],
        ids=["unresolvable", "not_callable", "no_context_slot", "params_rejected"],
    )
    def test_falla_en_compilacion_con_su_propio_codigo_cuando_la_factoria_no_sirve(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
        block: DynamicInstructionsSpec,
        code: AgentErrorCode,
        field: str,
    ) -> None:
        """Each fault has its own code and points at the key that carries it."""
        issue = single_issue_for(spec_factory(dynamic_instructions=block))

        assert (issue.code, issue.field) == (code, field)

    def test_el_mensaje_de_params_nombra_el_parametro_rechazado(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        """An operator has to read which setting the factory does not accept."""
        issue = single_issue_for(spec_factory(dynamic_instructions=_block(unknown_setting=1)))

        assert "unknown_setting" in issue.message

    def test_una_factoria_mal_firmada_no_se_reporta_como_capacidad_python(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        """The shared signature check must not leak the other caller's vocabulary."""
        issue = single_issue_for(spec_factory(dynamic_instructions=_block(unknown_setting=1)))

        assert "PYTHON" not in issue.code.value
        assert "python" not in issue.message


class TestAceptacion:
    def test_el_plan_lleva_la_factoria_importada_cuando_la_referencia_resuelve(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., Any],
    ) -> None:
        """The plan carries the handle, never the name: strings die at compile."""
        from myapp.agents.prompts import build_checklist

        plan = plan_for(spec_factory(dynamic_instructions=_block(locale="es")))

        assert plan.dynamic_instructions is not None
        assert plan.dynamic_instructions.factory is build_checklist
        assert plan.dynamic_instructions.factory_ref == _VALID_REF
        assert plan.dynamic_instructions.params == {"locale": "es"}

    def test_el_plan_no_lleva_factoria_cuando_el_artefacto_no_la_declara(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., Any],
    ) -> None:
        """The field is optional, and absent means absent — not a provider returning nothing."""
        plan = plan_for(spec_factory())

        assert plan.dynamic_instructions is None

    def test_el_plan_conserva_las_instrucciones_literales_cuando_hay_factoria(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., Any],
    ) -> None:
        """The block adds to the literal; it never stands in for it."""
        plan = plan_for(spec_factory(dynamic_instructions=_block()))

        assert plan.instructions == "Answer using only the prompt. Say so when unsure."
