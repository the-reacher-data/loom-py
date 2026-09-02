"""What actually reaches the MCP client when a server declares a credential.

Everything here asserts on the transport the engine hands to
``pydantic_ai.mcp.MCPToolset`` — its headers and its ``auth`` object — rather
than on the absence of an exception. A toolset that constructs cleanly while
sending no credential is exactly the failure this module exists to catch.

The MCP client lives in the optional ``mcp-tests`` group, so the module skips
where it is absent, as ``test_mcp_end_to_end.py`` does.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from loom.ai.compiler import CompiledMcpAuth, CompiledMcpCapability
from loom.ai.engines.pydantic_ai._mcp import build_mcp_toolset

from ...helpers.mcp_auth_plugin import third_party_strategy

pytest.importorskip(
    "pydantic_ai.mcp", reason="the MCP client is not installed: uv sync --group mcp-tests"
)

_URL = "https://orders.example.com/mcp"


def _transport(capability: CompiledMcpCapability) -> Any:
    """Build the toolset and return the transport its client will speak through."""
    return build_mcp_toolset(capability).client.transport


@pytest.fixture(autouse=True)
def _isolated_sharing() -> Any:
    """Empty the per-server sharing map: identity assertions must not leak between tests."""
    from loom.ai import mcp_auth

    mcp_auth._STRATEGIES._by_server.clear()
    yield
    mcp_auth._STRATEGIES._by_server.clear()


class TestHeadersRefLlegaAlCliente:
    """The compile-time refusal is gone; the header must actually travel."""

    def test_el_cliente_lleva_la_cabecera_cuando_el_servidor_declara_headers_ref(self) -> None:
        capability = CompiledMcpCapability(
            server="orders", url=_URL, headers_ref="X-API-Key=abc123"
        )

        assert _transport(capability).headers == {"X-API-Key": "abc123"}

    def test_el_cliente_no_lleva_credencial_cuando_el_servidor_no_declara_ninguna(self) -> None:
        """The no-auth path is unchanged, which is what keeps artifacts portable."""
        capability = CompiledMcpCapability(server="orders", url=_URL)

        transport = _transport(capability)

        assert not getattr(transport, "headers", None)
        assert getattr(transport, "auth", None) is None


class TestEstrategiaOauth:
    """``kind: oauth`` runs the MCP client's own flow, not one loom wrote."""

    def test_el_cliente_monta_su_propio_flujo_oauth(self) -> None:
        from fastmcp.client.auth.oauth import OAuth

        capability = CompiledMcpCapability(
            server="catalog",
            url="https://catalog.example.com/mcp",
            auth=CompiledMcpAuth(kind="oauth"),
        )

        assert isinstance(_transport(capability).auth, OAuth)


class TestEstrategiaBearer:
    """``kind: bearer`` is the shape configuration cannot express by hand."""

    def test_la_peticion_del_cliente_lleva_authorization_bearer(self) -> None:
        """Asserted on the request the transport's auth actually produces."""
        import httpx

        token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhLWIifQ.s_g-9"
        capability = CompiledMcpCapability(
            server="catalog",
            url="https://catalog.example.com/mcp",
            auth=CompiledMcpAuth(kind="bearer", settings=(("token_ref", token),)),
        )

        auth = _transport(capability).auth
        request = next(auth.auth_flow(httpx.Request("POST", "https://catalog.example.com/mcp")))

        assert request.headers["Authorization"] == f"Bearer {token}"


class TestEstrategiaDeTerceros:
    """The extension point is only real if someone who is not loom can use it."""

    def test_el_cliente_recibe_la_estrategia_construida_con_sus_ajustes(
        self, tmp_path: Path
    ) -> None:
        capability = CompiledMcpCapability(
            server="orders",
            url=_URL,
            auth=CompiledMcpAuth(
                kind="agent-session",
                settings=(
                    ("session_url", "https://orders.example.com/auth/agent/session"),
                    ("bootstrap_ref", "/agents/prod/agent-sales"),
                ),
            ),
        )

        with third_party_strategy(tmp_path, name="agent-session"):
            auth = _transport(capability).auth

        assert (type(auth).__name__, auth.bootstrap_ref) == (
            "AgentSessionAuth",
            "/agents/prod/agent-sales",
        )

    def test_dos_agentes_del_mismo_servidor_comparten_una_sola_instancia(
        self, tmp_path: Path
    ) -> None:
        """Identity, not equality: one credential per server, however many agents hold it."""
        capability = CompiledMcpCapability(
            server="orders",
            url=_URL,
            auth=CompiledMcpAuth(
                kind="agent-session",
                settings=(
                    ("session_url", "https://orders.example.com/auth/agent/session"),
                    ("bootstrap_ref", "/agents/prod/agent-sales"),
                ),
            ),
        )
        # Two grants of the same server, as two agents' plans carry them: the
        # filters differ, the server does not.
        first_agent = CompiledMcpCapability(
            server=capability.server, url=capability.url, auth=capability.auth, include=("read_*",)
        )

        with third_party_strategy(tmp_path, name="agent-session"):
            shared = _transport(capability).auth
            other = _transport(first_agent).auth

        assert shared is other
