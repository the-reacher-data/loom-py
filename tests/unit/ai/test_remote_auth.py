"""Outbound authentication: what the deployment declares, and what loom refuses.

Two layers are pinned here and nothing else:

* ``loom.ai.config`` — the compile-time refusals, applied identically to an
  MCP server and to an A2A agent. An unregistered strategy, a literal secret
  anywhere in the ``auth`` block, and ``headers_ref`` together with ``auth``
  are all faults of the deployment, so they must be found while the
  configuration is decoded rather than at the first message in production.
* ``loom.ai.remote_auth`` — resolution itself: the strategy name is looked up in
  a real entry-point group, constructed from its settings, and the instance is
  shared per endpoint.

The third-party strategy is installed as a genuine distribution (see
``tests.helpers.remote_auth_plugin``) rather than by patching the loader: the
extension point only means something if someone who is not loom can use it.
"""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from loom.ai.compiler import CompiledRemoteAuth
from loom.ai.config import A2AAgentConfig, AiConfig, McpServerConfig
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.inference import InferenceTarget
from loom.ai.remote_auth import (
    _checked,
    bearer_token,
    headers_from_ref,
    is_strategy_registered,
    registered_strategy_names,
    shared_a2a_auth,
    shared_mcp_auth,
    standard_oauth,
    static_headers,
)

from ...helpers.remote_auth_plugin import third_party_strategy

_URL = "https://knowledge.example.com/mcp"
_AGENT_URL = "https://market.example.com/a2a"


def _config_with(server: McpServerConfig) -> AiConfig:
    """Build a valid ``AiConfig`` around the one server under test."""
    return AiConfig(
        engine="pydantic-ai",
        specs=("ai/agents/*/agent.yaml",),
        models={"default": InferenceTarget(provider="openai", model="gpt-test")},
        mcp_servers={"knowledge": server},
    )


def _config_with_agent(agent: A2AAgentConfig) -> AiConfig:
    """Build a valid ``AiConfig`` around the one remote agent under test."""
    return AiConfig(
        engine="pydantic-ai",
        specs=("ai/agents/*/agent.yaml",),
        models={"default": InferenceTarget(provider="openai", model="gpt-test")},
        a2a_agents={"market": agent},
    )


def _codes(error: AgentCompilationError) -> list[AgentErrorCode]:
    return [issue.code for issue in error.issues]


@pytest.fixture(autouse=True)
def _isolated_sharing() -> Iterator[None]:
    """Empty the per-endpoint sharing map so one test cannot seed another.

    Reaching into the private map is deliberate: the sharing is process-wide by
    design, and a test asserting *identity* would otherwise depend on whichever
    test ran first.
    """
    from loom.ai import remote_auth

    remote_auth._STRATEGIES._by_endpoint.clear()
    yield
    remote_auth._STRATEGIES._by_endpoint.clear()


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
        """The strategy is exercised as a client drives it, not by inspection."""
        httpx = pytest.importorskip("httpx")
        auth = static_headers(headers_ref="X-API-Key=abc123")

        request = auth(httpx.Request("GET", "https://knowledge.example.com/mcp"))

        assert request.headers["X-API-Key"] == "abc123"

    def test_bearer_presenta_el_token_en_la_cabecera_authorization(self) -> None:
        """The header the strategy composes is what configuration cannot carry."""
        httpx = pytest.importorskip("httpx")
        auth = bearer_token(token_ref="eyJhbGci.eyJzdWIi-abc_123")

        request = auth(httpx.Request("GET", "https://catalog.example.com/mcp"))

        assert request.headers["Authorization"] == "Bearer eyJhbGci.eyJzdWIi-abc_123"

    @pytest.mark.parametrize(
        "build",
        [
            lambda: bearer_token(token_ref="a.b-c_1"),
            lambda: static_headers(headers_ref="X-API-Key=abc123"),
        ],
        ids=["bearer", "static"],
    )
    def test_la_estrategia_devuelve_la_peticion_que_recibe(self, build: Any) -> None:
        """Both clients wrap the callable as ``yield self._func(request)``.

        A callable that mutated the request but returned ``None`` would send
        ``None`` instead of it, and no header assertion would notice.
        """
        httpx = pytest.importorskip("httpx")
        request = httpx.Request("GET", "https://knowledge.example.com/mcp")

        assert build()(request) is request

    def test_ninguna_estrategia_importa_una_libreria_http(self) -> None:
        """Checked in a fresh interpreter, which is the only place it means anything.

        ``loom.ai.config`` imports this module at load time, so an in-process
        check finds whatever the running test session already imported and
        passes however the strategies are written.
        """
        probe = subprocess.run(  # noqa: S603 - the interpreter running the suite
            [sys.executable, "-c", _NO_HTTP_IMPORT_PROBE],
            capture_output=True,
            text=True,
            check=False,
        )

        assert probe.returncode == 0, probe.stderr

    def test_loom_registra_oauth_bearer_y_static_y_nada_mas(self) -> None:
        """Loom hard-codes no vendor: the three names it ships are generic."""
        assert registered_strategy_names() == ["bearer", "oauth", "static"]


class TestLoQueLoomAceptaDeUnaEstrategia:
    """``_checked`` accepts what both clients accept, and nothing else."""

    def test_acepta_un_invocable(self) -> None:
        """The shape both clients wrap in their own ``FunctionAuth``."""

        def auth(request: Any) -> Any:
            return request

        assert _checked("callable-strategy", auth) is auth

    def test_acepta_un_objeto_con_auth_flow(self) -> None:
        """A class written against either flavour satisfies the same probe."""

        class _Flavoured:
            def auth_flow(self, request: Any) -> Any:
                yield request

        built = _Flavoured()

        assert _checked("agent-session", built) is built

    def test_acepta_el_centinela_del_cliente_mcp(self) -> None:
        assert _checked("oauth", "oauth") == "oauth"

    def test_falla_con_auth_strategy_invalid_cuando_no_es_ninguna_de_las_tres_formas(self) -> None:
        """An object no client can use would otherwise connect unauthenticated."""
        with pytest.raises(AgentCompilationError) as excinfo:
            _checked("agent-session", object())

        assert _codes(excinfo.value) == [AgentErrorCode.MCP_AUTH_STRATEGY_INVALID]


class TestResolucionCompartidaPorServidor:
    """One instance per server: the credential belongs to the deployment."""

    def test_devuelve_none_cuando_el_servidor_no_declara_estrategia(self) -> None:
        assert shared_mcp_auth("knowledge", None) is None

    def test_construye_la_estrategia_de_terceros_con_sus_ajustes(self, tmp_path: Path) -> None:
        """Settings become keyword arguments of the registered object."""
        auth = CompiledRemoteAuth(
            kind="agent-session",
            settings=(
                ("session_url", "https://orders.example.com/auth/agent/session"),
                ("bootstrap_ref", "/agents/prod/agent-sales"),
            ),
        )

        with third_party_strategy(tmp_path, name="agent-session"):
            built = shared_mcp_auth("orders", auth)

        assert (built.session_url, built.bootstrap_ref) == (  # type: ignore[union-attr]
            "https://orders.example.com/auth/agent/session",
            "/agents/prod/agent-sales",
        )

    def test_comparte_una_sola_instancia_cuando_dos_llamadas_nombran_el_mismo_servidor(
        self, tmp_path: Path
    ) -> None:
        """Identity, not equality: a renewing strategy holds the live token."""
        auth = CompiledRemoteAuth(
            kind="agent-session",
            settings=(
                ("session_url", "https://orders.example.com/auth/agent/session"),
                ("bootstrap_ref", "/agents/prod/agent-sales"),
            ),
        )

        with third_party_strategy(tmp_path, name="agent-session"):
            first = shared_mcp_auth("orders", auth)
            second = shared_mcp_auth("orders", auth)

        assert first is second

    def test_no_comparte_entre_servidores_distintos(self, tmp_path: Path) -> None:
        """Two servers are two credentials, however alike their settings look."""
        auth = CompiledRemoteAuth(
            kind="agent-session",
            settings=(
                ("session_url", "https://orders.example.com/auth/agent/session"),
                ("bootstrap_ref", "/agents/prod/agent-sales"),
            ),
        )

        with third_party_strategy(tmp_path, name="agent-session"):
            orders = shared_mcp_auth("orders", auth)
            catalog = shared_mcp_auth("catalog", auth)

        assert orders is not catalog

    def test_falla_con_auth_strategy_invalid_cuando_la_estrategia_rechaza_sus_ajustes(
        self, tmp_path: Path
    ) -> None:
        """A settings key the strategy does not take is a deployment fault, named as one."""
        auth = CompiledRemoteAuth(kind="agent-session", settings=(("unexpected", "value"),))

        with third_party_strategy(tmp_path, name="agent-session"):  # noqa: SIM117
            with pytest.raises(AgentCompilationError) as excinfo:
                shared_mcp_auth("orders", auth)

        assert _codes(excinfo.value) == [AgentErrorCode.MCP_AUTH_STRATEGY_INVALID]

    def test_is_strategy_registered_es_falso_para_un_nombre_vacio(self) -> None:
        assert is_strategy_registered("") is False


class TestFallosDeResolucionDeLaEstrategia:
    """Every resolution failure reaches the deployment as a coded issue.

    The loader is faked here, unlike the rest of this module: two distributions
    claiming one strategy name cannot be installed side by side in the test
    environment, and the strategy that is missing at start-up was, by
    definition, present at decode.
    """

    @staticmethod
    def _install(monkeypatch: pytest.MonkeyPatch, dist_names: tuple[str, ...]) -> None:
        """Register ``kind='agent-session'`` once per name in ``dist_names``."""

        class _Dist:
            def __init__(self, name: str) -> None:
                self.name = name

        class _EntryPoint:
            group = "loom.ai.remote_auth"

            def __init__(self, dist_name: str) -> None:
                self.name = "agent-session"
                self.dist = _Dist(dist_name)

            def load(self) -> Any:
                return lambda request: request

        class _EntryPoints:
            def select(self, *, group: str) -> tuple[_EntryPoint, ...]:
                if group != "loom.ai.remote_auth":
                    return ()
                return tuple(_EntryPoint(name) for name in dist_names)

        from loom.core.plugins import entrypoints as entrypoints_module

        monkeypatch.setattr(entrypoints_module, "entry_points", _EntryPoints)

    def test_falla_con_auth_strategy_invalid_cuando_dos_distribuciones_la_registran(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._install(monkeypatch, ("loom-auth-alpha", "loom-auth-beta"))

        with pytest.raises(AgentCompilationError) as excinfo:
            shared_mcp_auth("orders", CompiledRemoteAuth(kind="agent-session", settings=()))

        assert _codes(excinfo.value) == [AgentErrorCode.MCP_AUTH_STRATEGY_INVALID]

    def test_el_mensaje_nombra_ambas_distribuciones_cuando_hay_duplicado(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._install(monkeypatch, ("loom-auth-alpha", "loom-auth-beta"))

        with pytest.raises(AgentCompilationError) as excinfo:
            shared_mcp_auth("orders", CompiledRemoteAuth(kind="agent-session", settings=()))

        message = str(excinfo.value)
        assert "loom-auth-alpha" in message and "loom-auth-beta" in message

    def test_falla_con_auth_strategy_invalid_cuando_ya_no_esta_registrada(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A distribution uninstalled between decode and start-up is named, not crashed on."""
        self._install(monkeypatch, ())

        with pytest.raises(AgentCompilationError) as excinfo:
            shared_mcp_auth("orders", CompiledRemoteAuth(kind="agent-session", settings=()))

        assert _codes(excinfo.value) == [AgentErrorCode.MCP_AUTH_STRATEGY_INVALID]


class TestConfiguracionDelBloqueAuthDeUnAgenteRemoto:
    """``ai.a2a_agents.<name>.auth`` is held to exactly the MCP rules."""

    def test_falla_con_auth_strategy_unknown_cuando_la_estrategia_no_esta_registrada(self) -> None:
        """Otherwise the agent would connect unauthenticated at the first delegation."""
        agent = A2AAgentConfig(url=_AGENT_URL, auth={"kind": "nobody-registers-this"})

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with_agent(agent)

        assert AgentErrorCode.MCP_AUTH_STRATEGY_UNKNOWN in _codes(excinfo.value)

    def test_el_mensaje_nombra_la_estrategia_y_las_registradas_cuando_no_existe(self) -> None:
        agent = A2AAgentConfig(url=_AGENT_URL, auth={"kind": "nobody-registers-this"})

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with_agent(agent)

        message = str(excinfo.value)
        assert "nobody-registers-this" in message
        assert "bearer" in message and "static" in message

    def test_falla_con_credentials_inline_cuando_un_ajuste_lleva_un_secreto_literal(self) -> None:
        agent = A2AAgentConfig(
            url=_AGENT_URL, auth={"kind": "bearer", "token_ref": "sk-abc123def456ghi789"}
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with_agent(agent)

        assert AgentErrorCode.MCP_CREDENTIALS_INLINE in _codes(excinfo.value)

    def test_el_mensaje_no_contiene_el_secreto_cuando_rechaza_un_ajuste(self) -> None:
        literal = "sk-abc123def456ghi789"
        agent = A2AAgentConfig(url=_AGENT_URL, auth={"kind": "bearer", "token_ref": literal})

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with_agent(agent)

        assert literal not in str(excinfo.value)

    def test_falla_con_auth_conflict_cuando_convive_con_headers_ref(self) -> None:
        """Two credentials on one connection is as ambiguous here as it is for MCP."""
        agent = A2AAgentConfig(
            url=_AGENT_URL, headers_ref="X-API-Key=abc123", auth={"kind": "bearer"}
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with_agent(agent)

        assert AgentErrorCode.MCP_AUTH_CONFLICT in _codes(excinfo.value)

    def test_acepta_el_agente_cuando_la_estrategia_esta_registrada(self) -> None:
        agent = A2AAgentConfig(url=_AGENT_URL, auth={"kind": "bearer", "token_ref": "a.b-c_1"})

        config = _config_with_agent(agent)

        assert config.a2a_agents["market"].auth == {"kind": "bearer", "token_ref": "a.b-c_1"}

    def test_acepta_una_estrategia_de_terceros_cuando_su_distribucion_esta_instalada(
        self, tmp_path: Path
    ) -> None:
        """One group: a strategy registered for MCP is offered to A2A unchanged."""
        agent = A2AAgentConfig(
            url=_AGENT_URL,
            auth={
                "kind": "agent-session",
                "session_url": "https://market.example.com/auth/agent/session",
                "bootstrap_ref": "/agents/prod/agent-sales",
            },
        )

        with third_party_strategy(tmp_path, name="agent-session"):
            config = _config_with_agent(agent)

        assert config.a2a_agents["market"].auth is not None


class TestResolucionCompartidaPorAgenteRemoto:
    """One instance per configured agent, from the same registry MCP uses."""

    def test_devuelve_none_cuando_el_agente_no_declara_estrategia(self) -> None:
        assert shared_a2a_auth("market", None) is None

    def test_construye_la_estrategia_de_terceros_con_sus_ajustes(self, tmp_path: Path) -> None:
        auth = CompiledRemoteAuth(
            kind="agent-session",
            settings=(
                ("session_url", "https://market.example.com/auth/agent/session"),
                ("bootstrap_ref", "/agents/prod/agent-sales"),
            ),
        )

        with third_party_strategy(tmp_path, name="agent-session"):
            built = shared_a2a_auth("market", auth)

        assert (built.session_url, built.bootstrap_ref) == (  # type: ignore[attr-defined]
            "https://market.example.com/auth/agent/session",
            "/agents/prod/agent-sales",
        )

    def test_comparte_una_sola_instancia_cuando_dos_agentes_nombran_el_mismo_remoto(
        self,
    ) -> None:
        """Identity, not equality: the credential belongs to the deployment."""
        auth = CompiledRemoteAuth(kind="bearer", settings=(("token_ref", "a.b-c_1"),))

        first = shared_a2a_auth("market", auth)
        second = shared_a2a_auth("market", auth)

        assert first is second

    def test_no_comparte_con_un_servidor_mcp_del_mismo_nombre(self) -> None:
        """A server and an agent registered alike are two endpoints, two credentials."""
        auth = CompiledRemoteAuth(kind="bearer", settings=(("token_ref", "a.b-c_1"),))

        assert shared_a2a_auth("orders", auth) is not shared_mcp_auth("orders", auth)

    def test_falla_con_auth_strategy_invalid_cuando_la_estrategia_es_el_centinela_oauth(
        self,
    ) -> None:
        """``oauth`` delegates to the MCP client's flow; A2A must refuse, not connect bare."""
        auth = CompiledRemoteAuth(kind="oauth")

        with pytest.raises(AgentCompilationError) as excinfo:
            shared_a2a_auth("market", auth)

        assert _codes(excinfo.value) == [AgentErrorCode.MCP_AUTH_STRATEGY_INVALID]

    def test_falla_con_auth_strategy_invalid_cuando_la_estrategia_rechaza_sus_ajustes(
        self, tmp_path: Path
    ) -> None:
        auth = CompiledRemoteAuth(kind="agent-session", settings=(("unexpected", "value"),))

        with third_party_strategy(tmp_path, name="agent-session"):  # noqa: SIM117
            with pytest.raises(AgentCompilationError) as excinfo:
                shared_a2a_auth("market", auth)

        assert _codes(excinfo.value) == [AgentErrorCode.MCP_AUTH_STRATEGY_INVALID]


_NO_HTTP_IMPORT_PROBE = """
import sys

from loom.ai.remote_auth import bearer_token, static_headers

bearer_token(token_ref="a.b-c_1")
static_headers(headers_ref="X-API-Key=abc123")

resident = sorted(name for name in ("httpx", "httpx2") if name in sys.modules)
assert not resident, f"remote_auth imported an HTTP library: {resident}"
"""
"""Source of the fresh interpreter that pins AC2.

Kept as a module constant so the probe reads as code rather than as an
argument, and so the test body stays about the assertion.
"""
