"""``ai.a2a.base_url`` is published verbatim in the capability card.

The card is the pillar's one unauthenticated surface, so an ``http://``
address advertises a plaintext channel to every discovering client and
userinfo publishes a credential outright. The compiler already applies this
rule to the remote URLs an artifact names; these pin it where the published
one actually lives (FR-038).
"""

from __future__ import annotations

import pytest

from loom.ai.config import A2AConfig
from loom.ai.errors import AgentCompilationError, AgentErrorCode


def _codes(exc: AgentCompilationError) -> list[AgentErrorCode]:
    return [issue.code for issue in exc.issues]


def test_acepta_una_url_https_limpia() -> None:
    """The shape a correct deployment publishes."""
    assert A2AConfig(base_url="https://api.example.com", expose=("triage",)) is not None


@pytest.mark.parametrize(
    "url",
    [
        "http://api.example.com",
        "https://ada:secret@api.example.com",
        "https://api.example.com?token=abc",
        "https://",
    ],
)
def test_rechaza_una_url_que_no_es_publicable(url: str) -> None:
    """Plaintext, embedded credentials, a query string, or no host at all."""
    with pytest.raises(AgentCompilationError) as caught:
        A2AConfig(base_url=url, expose=("triage",))
    assert _codes(caught.value) == [AgentErrorCode.A2A_BASE_URL_INVALID]


def test_el_mensaje_no_publica_la_credencial_que_rechaza() -> None:
    """Refusing a leak must not become the leak: the message is redacted."""
    with pytest.raises(AgentCompilationError) as caught:
        A2AConfig(base_url="https://ada:CANARY_PWD@api.example.com", expose=("triage",))
    assert "CANARY_PWD" not in str(caught.value)
