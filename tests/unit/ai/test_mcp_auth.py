"""MCP authentication: what the deployment declares, and what loom refuses.

Two layers are pinned here and nothing else:

* ``loom.ai.config`` — the compile-time refusals. An unregistered strategy, a
  literal secret anywhere in the ``auth`` block, and ``headers_ref`` together
  with ``auth`` are all faults of the deployment, so they must be found while
  the configuration is decoded rather than at the first message in production.
* ``loom.ai.mcp_auth`` — resolution itself: the strategy name is looked up in a
  real entry-point group, constructed from its settings, and the instance is
  shared per server.

The third-party strategy is installed as a genuine distribution (see
``tests.helpers.mcp_auth_plugin``) rather than by patching the loader: the
extension point only means something if someone who is not loom can use it.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from loom.ai.compiler import CompiledMcpAuth
from loom.ai.config import AiConfig, McpServerConfig
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.inference import InferenceTarget
from loom.ai.mcp_auth import (
    bearer_token,
    headers_from_ref,
    is_strategy_registered,
    registered_strategy_names,
    shared_auth,
    standard_oauth,
    static_headers,
)

from ...helpers.mcp_auth_plugin import third_party_strategy

_URL = "https://knowledge.example.com/mcp"


def _config_with(server: McpServerConfig) -> AiConfig:
    """Build a valid ``AiConfig`` around the one server under test."""
    return AiConfig(
        engine="pydantic-ai",
        specs=("ai/agents/*/agent.yaml",),
        models={"default": InferenceTarget(provider="openai", model="gpt-test")},
        mcp_servers={"knowledge": server},
    )


def _codes(error: AgentCompilationError) -> list[AgentErrorCode]:
    return [issue.code for issue in error.issues]


@pytest.fixture(autouse=True)
def _isolated_sharing() -> Iterator[None]:
    """Empty the per-server sharing map so one test cannot seed another.

    Reaching into the private map is deliberate: the sharing is process-wide by
    design, and a test asserting *identity* would otherwise depend on whichever
    test ran first.
    """
    from loom.ai import mcp_auth

    mcp_auth._STRATEGIES._by_server.clear()
    yield
    mcp_auth._STRATEGIES._by_server.clear()


class TestConfiguracionDelBloqueAuth:
    """``ai.mcp_servers.<name>.auth`` is refused before anything connects."""

    def test_falla_con_auth_strategy_unknown_cuando_la_estrategia_no_esta_registrada(
        self,
    ) -> None:
        """A name nobody registers must fail at compile, not at the first message."""
        server = McpServerConfig(url=_URL, auth={"kind": "nobody-registers-this"})

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(server)

        assert AgentErrorCode.MCP_AUTH_STRATEGY_UNKNOWN in _codes(excinfo.value)

    def test_el_mensaje_nombra_la_estrategia_y_las_registradas_cuando_no_existe(self) -> None:
        """The operator must be able to act on the message without reading loom."""
        server = McpServerConfig(url=_URL, auth={"kind": "nobody-registers-this"})

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(server)

        message = str(excinfo.value)
        assert "nobody-registers-this" in message
        assert "oauth" in message and "static" in message

    def test_falla_con_auth_strategy_unknown_cuando_el_bloque_no_declara_kind(self) -> None:
        """A block without ``kind`` names no strategy at all."""
        server = McpServerConfig(url=_URL, auth={"session_url": "https://auth.example.com/token"})

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(server)

        assert AgentErrorCode.MCP_AUTH_STRATEGY_UNKNOWN in _codes(excinfo.value)

    def test_falla_con_credentials_inline_cuando_un_ajuste_lleva_un_secreto_literal(self) -> None:
        """The inline-credential rule covers the whole block, not just ``headers_ref``."""
        server = McpServerConfig(
            url=_URL, auth={"kind": "oauth", "bootstrap_ref": "sk-abc123def456ghi789"}
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(server)

        assert AgentErrorCode.MCP_CREDENTIALS_INLINE in _codes(excinfo.value)

    def test_el_mensaje_no_contiene_el_secreto_cuando_rechaza_un_ajuste(self) -> None:
        """The rejection must not leak the very secret it rejects."""
        literal = "sk-abc123def456ghi789"
        server = McpServerConfig(url=_URL, auth={"kind": "oauth", "bootstrap_ref": literal})

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(server)

        assert literal not in str(excinfo.value)

    def test_falla_con_auth_conflict_cuando_convive_con_headers_ref(self) -> None:
        """Two credentials on one connection is ambiguous, so it is refused."""
        server = McpServerConfig(url=_URL, headers_ref="X-API-Key=abc123", auth={"kind": "oauth"})

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(server)

        assert AgentErrorCode.MCP_AUTH_CONFLICT in _codes(excinfo.value)

    def test_acepta_un_token_con_forma_de_jwt_como_ajuste(self) -> None:
        """A JWT is base64url with dots: the inline-credential test must let it through."""
        server = McpServerConfig(
            url=_URL,
            auth={"kind": "bearer", "token_ref": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhLWIifQ.s_g-9"},
        )

        config = _config_with(server)

        assert config.mcp_servers["knowledge"].auth is not None

    def test_rechaza_un_ajuste_con_espacios_aunque_sea_un_bearer_compuesto(self) -> None:
        """The composed header is exactly what must not live in configuration."""
        server = McpServerConfig(
            url=_URL, auth={"kind": "bearer", "token_ref": "Bearer eyJhbGciOiJIUzI1NiJ9"}
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(server)

        assert AgentErrorCode.MCP_CREDENTIALS_INLINE in _codes(excinfo.value)

    def test_acepta_el_servidor_cuando_la_estrategia_esta_registrada(self) -> None:
        """``oauth`` ships with loom, so it decodes without complaint."""
        server = McpServerConfig(url=_URL, auth={"kind": "oauth"})

        config = _config_with(server)

        assert config.mcp_servers["knowledge"].auth == {"kind": "oauth"}

    def test_acepta_una_estrategia_de_terceros_cuando_su_distribucion_esta_instalada(
        self, tmp_path: Path
    ) -> None:
        """A deployment's own strategy is as valid as loom's own."""
        server = McpServerConfig(
            url=_URL,
            auth={
                "kind": "agent-session",
                "session_url": "https://orders.example.com/auth/agent/session",
                "bootstrap_ref": "/agents/prod/agent-sales",
            },
        )

        with third_party_strategy(tmp_path, name="agent-session"):
            config = _config_with(server)

        assert config.mcp_servers["knowledge"].auth is not None


class TestCabecerasDeHeadersRef:
    """``headers_ref`` reaches loom already resolved; this reads its payload."""

    def test_devuelve_la_cabecera_cuando_el_valor_es_un_par_nombre_valor(self) -> None:
        assert headers_from_ref("server 'kb'", "X-API-Key=abc123") == {"X-API-Key": "abc123"}

    def test_devuelve_vacio_cuando_el_servidor_no_declara_credencial(self) -> None:
        assert headers_from_ref("server 'kb'", None) == {}

    @pytest.mark.parametrize(
        "payload", ["just-a-name", "=abc123", "X-API-Key="], ids=["no_pair", "no_name", "no_value"]
    )
    def test_falla_con_headers_ref_invalid_cuando_el_valor_no_es_un_par(self, payload: str) -> None:
        """A payload loom cannot turn into a header would silently send nothing."""
        with pytest.raises(AgentCompilationError) as excinfo:
            headers_from_ref("server 'kb'", payload)

        assert _codes(excinfo.value) == [AgentErrorCode.MCP_HEADERS_REF_INVALID]

    def test_el_mensaje_no_contiene_el_valor_cuando_lo_rechaza(self) -> None:
        payload = "sk-abc123def456ghi789"

        with pytest.raises(AgentCompilationError) as excinfo:
            headers_from_ref("server 'kb'", payload)

        assert payload not in str(excinfo.value)


class TestEstrategiasQueLoomRegistra:
    """Both are thin delegations: loom implements no login flow of its own."""

    def test_oauth_devuelve_el_centinela_que_el_cliente_mcp_entiende(self) -> None:
        assert standard_oauth() == "oauth"

    def test_static_anade_la_cabecera_a_cada_peticion(self) -> None:
        """The strategy is exercised through the httpx contract, not by inspection."""
        httpx = pytest.importorskip("httpx")
        auth = static_headers(headers_ref="X-API-Key=abc123")

        flow = auth.auth_flow(httpx.Request("GET", "https://knowledge.example.com/mcp"))
        request = next(flow)

        assert request.headers["X-API-Key"] == "abc123"

    def test_bearer_presenta_el_token_en_la_cabecera_authorization(self) -> None:
        """The header the strategy composes is what configuration cannot carry."""
        httpx = pytest.importorskip("httpx")
        auth = bearer_token(token_ref="eyJhbGci.eyJzdWIi-abc_123")

        flow = auth.auth_flow(httpx.Request("GET", "https://catalog.example.com/mcp"))
        request = next(flow)

        assert request.headers["Authorization"] == "Bearer eyJhbGci.eyJzdWIi-abc_123"

    def test_loom_registra_oauth_bearer_y_static_y_nada_mas(self) -> None:
        """Loom hard-codes no vendor: the three names it ships are generic."""
        assert registered_strategy_names() == ["bearer", "oauth", "static"]


class TestResolucionCompartidaPorServidor:
    """One instance per server: the credential belongs to the deployment."""

    def test_devuelve_none_cuando_el_servidor_no_declara_estrategia(self) -> None:
        assert shared_auth("knowledge", None) is None

    def test_construye_la_estrategia_de_terceros_con_sus_ajustes(self, tmp_path: Path) -> None:
        """Settings become keyword arguments of the registered object."""
        auth = CompiledMcpAuth(
            kind="agent-session",
            settings=(
                ("session_url", "https://orders.example.com/auth/agent/session"),
                ("bootstrap_ref", "/agents/prod/agent-sales"),
            ),
        )

        with third_party_strategy(tmp_path, name="agent-session"):
            built = shared_auth("orders", auth)

        assert (built.session_url, built.bootstrap_ref) == (  # type: ignore[union-attr]
            "https://orders.example.com/auth/agent/session",
            "/agents/prod/agent-sales",
        )

    def test_comparte_una_sola_instancia_cuando_dos_llamadas_nombran_el_mismo_servidor(
        self, tmp_path: Path
    ) -> None:
        """Identity, not equality: a renewing strategy holds the live token."""
        auth = CompiledMcpAuth(
            kind="agent-session",
            settings=(
                ("session_url", "https://orders.example.com/auth/agent/session"),
                ("bootstrap_ref", "/agents/prod/agent-sales"),
            ),
        )

        with third_party_strategy(tmp_path, name="agent-session"):
            first = shared_auth("orders", auth)
            second = shared_auth("orders", auth)

        assert first is second

    def test_no_comparte_entre_servidores_distintos(self, tmp_path: Path) -> None:
        """Two servers are two credentials, however alike their settings look."""
        auth = CompiledMcpAuth(
            kind="agent-session",
            settings=(
                ("session_url", "https://orders.example.com/auth/agent/session"),
                ("bootstrap_ref", "/agents/prod/agent-sales"),
            ),
        )

        with third_party_strategy(tmp_path, name="agent-session"):
            orders = shared_auth("orders", auth)
            catalog = shared_auth("catalog", auth)

        assert orders is not catalog

    def test_falla_con_auth_strategy_invalid_cuando_la_estrategia_rechaza_sus_ajustes(
        self, tmp_path: Path
    ) -> None:
        """A settings key the strategy does not take is a deployment fault, named as one."""
        auth = CompiledMcpAuth(kind="agent-session", settings=(("unexpected", "value"),))

        with third_party_strategy(tmp_path, name="agent-session"):  # noqa: SIM117
            with pytest.raises(AgentCompilationError) as excinfo:
                shared_auth("orders", auth)

        assert _codes(excinfo.value) == [AgentErrorCode.MCP_AUTH_STRATEGY_INVALID]

    def test_is_strategy_registered_es_falso_para_un_nombre_vacio(self) -> None:
        assert is_strategy_registered("") is False
