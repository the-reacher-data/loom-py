"""The mount notice names the identity each capability kind actually runs as."""

from __future__ import annotations

import pytest

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
