"""Output-check phase (011/T203): the answer rule is resolved offline or refused.

Three faults, three codes, all reported at compile time so a broken reference
can never surface as a failed run. The coroutine case is the one that is not
merely a broken reference: the engine would happily await it, and the awaited
coroutine object is truthy, so every answer would be rejected with an
unreadable message. Refusing it here is what makes the synchronous contract of
:data:`~loom.ai.abc.OutputCheck` an enforcement rather than a docstring.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from loom.ai.declarative import AgentSpecV1
from loom.ai.errors import AgentCompilationIssue, AgentErrorCode

_VALID_REF = "myapp.agents.checks:report_is_complete"
_COROUTINE_REF = "myapp.agents.checks:report_is_complete_async"
_NOT_CALLABLE_REF = "myapp.agents.checks:NOT_CALLABLE"
_UNRESOLVABLE_REF = "no_such_pkg_zz.checks:report_is_complete"

pytestmark = pytest.mark.usefixtures("fake_myapp_path")


class TestRechazos:
    @pytest.mark.parametrize(
        ("ref", "code"),
        [
            (_UNRESOLVABLE_REF, AgentErrorCode.OUTPUT_CHECK_UNRESOLVABLE),
            (_NOT_CALLABLE_REF, AgentErrorCode.OUTPUT_CHECK_NOT_CALLABLE),
            (_COROUTINE_REF, AgentErrorCode.OUTPUT_CHECK_COROUTINE_UNSUPPORTED),
        ],
        ids=["unresolvable", "not_callable", "coroutine"],
    )
    def test_falla_en_compilacion_con_su_propio_codigo_cuando_el_check_no_sirve(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
        ref: str,
        code: AgentErrorCode,
    ) -> None:
        """Each fault has its own code, so an operator reads what to fix."""
        issue = single_issue_for(spec_factory(output_check=ref))

        assert (issue.code, issue.field) == (code, "output_check")

    def test_el_mensaje_de_la_corrutina_explica_por_que_no_se_admite(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        """The refusal names the retry loop the check runs inside."""
        issue = single_issue_for(spec_factory(output_check=_COROUTINE_REF))

        assert "synchronous" in issue.message
        assert _COROUTINE_REF in issue.message


class TestAceptacion:
    def test_el_plan_lleva_la_funcion_importada_cuando_el_check_resuelve(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., Any],
    ) -> None:
        """The plan carries the handle, never the name: strings die at compile."""
        from myapp.agents.checks import report_is_complete

        plan = plan_for(spec_factory(output_check=_VALID_REF))

        assert plan.output_check is report_is_complete

    def test_el_plan_no_lleva_check_cuando_el_artefacto_no_lo_declara(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., Any],
    ) -> None:
        """The field is optional, and absent means absent — not a check that accepts."""
        plan = plan_for(spec_factory())

        assert plan.output_check is None
