"""Run-time contract of ``HOOK_FAILED`` and the ``interaction_id`` of a run error.

A hook failure is neither the provider's fault nor the model's: it is the
application's own use case failing after the answer was produced.  Classifying
it as ``INFRASTRUCTURE`` would make the retry policy replay a run whose hook
already began recording, so its class is pinned here (U1, AC7).
"""

from __future__ import annotations

from loom.ai.errors import (
    AgentRunError,
    AgentRunErrorClass,
    AgentRunErrorCode,
    is_retriable,
    run_error_class,
)


def test_hook_failed_es_de_aplicacion_y_no_reintentable() -> None:
    """The hook is not retried: its class is ``APPLICATION`` and never retriable."""
    code = AgentRunErrorCode.HOOK_FAILED

    assert run_error_class(code) is AgentRunErrorClass.APPLICATION
    assert is_retriable(code) is False


def test_el_error_de_ejecucion_no_lleva_interaction_id_cuando_no_se_indica() -> None:
    """Pre-admission failures have no interaction to name."""
    error = AgentRunError(AgentRunErrorCode.HOOK_FAILED, "the output hook failed")

    assert error.interaction_id is None


def test_el_error_de_ejecucion_conserva_el_interaction_id_cuando_se_indica() -> None:
    """The keyword survives so a transport can echo the interaction to the caller."""
    error = AgentRunError(
        AgentRunErrorCode.HOOK_FAILED, "the output hook failed", interaction_id="int-1"
    )

    assert error.interaction_id == "int-1"
    assert error.code is AgentRunErrorCode.HOOK_FAILED
    assert str(error) == "the output hook failed"


def test_el_status_http_de_hook_failed_esta_mapeado_explicitamente() -> None:
    from loom.ai.fastapi.endpoints import _STATUS_BY_CODE

    assert _STATUS_BY_CODE[AgentRunErrorCode.HOOK_FAILED] == 500
