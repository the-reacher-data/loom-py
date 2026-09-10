"""The mount notice names the identity each capability kind actually runs as."""

from __future__ import annotations

import pytest

from loom.ai.a2a._binding import _identity_notice as a2a_identity_notice
from loom.ai.config import AgentEndpointConfig
from loom.ai.fastapi.endpoints import _identity_notice


def test_promises_the_callers_identity_when_only_local_capabilities_are_granted() -> None:
    """Local kinds do run as the verified caller, so the notice may say so."""
    notice = _identity_notice(AgentEndpointConfig(enabled=True, auth="jwt"), ("sql", "usecase"))

    assert "runs as that verified identity" in notice
    assert "credential this deployment configured" not in notice


@pytest.mark.parametrize("remote", ["mcp", "a2a"])
def test_names_the_deployment_credential_when_remote_capabilities_are_granted(remote: str) -> None:
    """A remote endpoint sees the deployment's credential, never the caller's."""
    notice = _identity_notice(AgentEndpointConfig(enabled=True, auth="jwt"), ("sql", remote))

    assert remote in notice
    assert "credential this deployment configured" in notice
    assert "who calls does not bound what the remote side allows" in notice


def test_warns_that_there_is_no_identity_when_the_mount_is_anonymous() -> None:
    """An anonymous mount has no identity to promise, remote kinds or not."""
    notice = _identity_notice(
        AgentEndpointConfig(enabled=True, auth="jwt", allow_anonymous=True), ("mcp",)
    )

    assert "callers are NOT authenticated" in notice


def test_warns_that_the_id_is_the_credential_for_a_conversational_anonymous_mount() -> None:
    """Anonymous callers share one subject, so only ``conversation_id`` separates threads."""
    endpoint = AgentEndpointConfig(enabled=True, auth="jwt", allow_anonymous=True)

    conversational = _identity_notice(endpoint, ("sql",), conversational=True)
    single_shot = _identity_notice(endpoint, ("sql",))

    assert "callers are NOT authenticated" in conversational
    assert "separated by 'conversation_id' alone" in conversational
    assert "conversation_id" not in single_shot


def test_warns_that_context_id_is_the_credential_for_a_conversational_anonymous_a2a_mount() -> None:
    """AC6: over A2A the anonymous notice names ``contextId`` as the only separator."""
    conversational = a2a_identity_notice(True, conversational=True)
    single_shot = a2a_identity_notice(True)

    assert "callers are NOT authenticated" in conversational
    assert "contextId" in conversational
    assert "credential" in conversational
    assert "contextId" not in single_shot
    assert "credential" not in single_shot


@pytest.mark.parametrize("conversational", [True, False])
def test_does_not_change_the_authenticated_a2a_notice_when_the_plan_is_conversational(
    conversational: bool,
) -> None:
    """AC6: a verified caller is the credential, so the authenticated notice is unchanged."""
    notice = a2a_identity_notice(False, conversational=conversational)

    assert notice == a2a_identity_notice(False)
    assert "contextId" not in notice
