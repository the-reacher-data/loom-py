"""The mount notice names the identity each capability kind actually runs as."""

from __future__ import annotations

import pytest

from loom.ai.a2a._binding import _identity_notice as a2a_identity_notice
from loom.ai.config import AgentEndpointConfig
from loom.ai.fastapi.endpoints import _identity_notice


def test_promete_la_identidad_del_llamante_cuando_solo_hay_capacidades_locales() -> None:
    """Local kinds do run as the verified caller, so the notice may say so."""
    notice = _identity_notice(AgentEndpointConfig(enabled=True, auth="jwt"), ("sql", "usecase"))

    assert "runs as that verified identity" in notice
    assert "credential this deployment configured" not in notice


@pytest.mark.parametrize("remote", ["mcp", "a2a"])
def test_nombra_la_credencial_del_despliegue_cuando_hay_capacidades_remotas(remote: str) -> None:
    """A remote endpoint sees the deployment's credential, never the caller's."""
    notice = _identity_notice(AgentEndpointConfig(enabled=True, auth="jwt"), ("sql", remote))

    assert remote in notice
    assert "credential this deployment configured" in notice
    assert "who calls does not bound what the remote side allows" in notice


def test_advierte_que_no_hay_identidad_cuando_el_mount_es_anonimo() -> None:
    """An anonymous mount has no identity to promise, remote kinds or not."""
    notice = _identity_notice(
        AgentEndpointConfig(enabled=True, auth="jwt", allow_anonymous=True), ("mcp",)
    )

    assert "callers are NOT authenticated" in notice


def test_avisa_de_que_el_id_es_la_credencial_cuando_el_mount_anonimo_conversa() -> None:
    """Anonymous callers share one subject, so only ``conversation_id`` separates threads."""
    endpoint = AgentEndpointConfig(enabled=True, auth="jwt", allow_anonymous=True)

    conversational = _identity_notice(endpoint, ("sql",), conversational=True)
    single_shot = _identity_notice(endpoint, ("sql",))

    assert "callers are NOT authenticated" in conversational
    assert "separated by 'conversation_id' alone" in conversational
    assert "conversation_id" not in single_shot


def test_avisa_de_que_el_context_id_es_la_credencial_cuando_el_mount_a2a_anonimo_conversa() -> None:
    """AC6: over A2A the anonymous notice names ``contextId`` as the only separator."""
    conversational = a2a_identity_notice(True, conversational=True)
    single_shot = a2a_identity_notice(True)

    assert "callers are NOT authenticated" in conversational
    assert "contextId" in conversational
    assert "credential" in conversational
    assert "contextId" not in single_shot
    assert "credential" not in single_shot


@pytest.mark.parametrize("conversational", [True, False])
def test_no_cambia_el_aviso_a2a_autenticado_cuando_el_plan_conversa(conversational: bool) -> None:
    """AC6: a verified caller is the credential, so the authenticated notice is unchanged."""
    notice = a2a_identity_notice(False, conversational=conversational)

    assert notice == a2a_identity_notice(False)
    assert "contextId" not in notice
