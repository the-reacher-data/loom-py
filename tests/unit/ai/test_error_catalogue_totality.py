"""Every code-keyed catalogue must cover every code.

Two catalogues map an :class:`~loom.ai.errors.AgentRunErrorCode` to something
each transport needs.  Both document themselves as total, and "total" was a
comment: one raises ``KeyError`` on a gap and the other degrades silently, so
adding a code without its entry either breaks a run or quietly ships a generic
message.  These tests make the claim checkable, so CI reports the gap instead
of production.
"""

from __future__ import annotations

from loom.ai.a2a.server import _RUN_ERROR_DETAILS
from loom.ai.errors import _RUN_ERROR_CLASSES, AgentRunErrorCode


def test_toda_clase_de_error_de_ejecucion_esta_mapeada() -> None:
    """``run_error_class`` raises ``KeyError`` for a code it does not know."""
    assert set(_RUN_ERROR_CLASSES) == set(AgentRunErrorCode)


def test_todo_detalle_a2a_esta_mapeado() -> None:
    """The A2A detail catalogue degrades silently, so a gap ships unnoticed."""
    assert set(_RUN_ERROR_DETAILS) == set(AgentRunErrorCode)
