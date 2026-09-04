"""Start-up tolerance of the remote clients (``ai.remote_clients``, F3).

Declaring an ``mcp`` or ``a2a`` grant used to make network reachability a
start-up requirement: an application could not boot offline and its start-up
tests became integration tests. ``ai.remote_clients: optional`` drops a
**connection** failure to a warning instead, with three carve-outs this module
pins one by one:

* a missing client factory is a wiring bug, not an offline network, and stays
  fatal under both values;
* tool-filter verification keeps failing closed for the servers that did open,
  which is why the pass gets a fresh budget rather than the one an unreachable
  server exhausted;
* nothing becomes lazy: a run still connects on its own, exactly as on master.

Every dependency is a local stub: no network, no credential, no token.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping, Sequence

import pytest

from loom.ai.compiler._plan import AgentPlan, CompiledA2ACapability
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.runtime import AgentRuntime
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from tests.integration.ai.conftest import (
    CountingEngineProvider,
    RecordingMcpSession,
    StubDepsFactory,
    StubMcpClient,
    make_ai_config,
    make_mcp_capability,
    make_mcp_servers,
    make_plan,
    mcp_client_factory,
)

_SERVER_A = "alpha-tools"
_SERVER_B = "beta-tools"
_REMOTE = "translations"
_AGENT = "analyst"

_LOGGER = "loom.ai.runtime._lifecycle"
_REFUSED = "connection refused by https://alpha-tools.internal/mcp"

_MISSING_MCP_FACTORY = "no MCP client factory is configured"
_MISSING_A2A_FACTORY = "no A2A client factory is configured"


def _codes(error: AgentCompilationError) -> set[AgentErrorCode]:
    """Return the distinct issue codes an aggregated compilation error carries."""
    return {issue.code for issue in error.issues}


def _make_a2a_capability(agent: str = _REMOTE) -> CompiledA2ACapability:
    """Build an A2A capability naming a registered remote agent."""
    return CompiledA2ACapability(agent=agent, url=f"https://{agent}.internal/a2a")


def _a2a_client_factory(
    clients: Mapping[str, StubMcpClient],
) -> Callable[[CompiledA2ACapability], StubMcpClient]:
    """Build an ``A2AClientFactory`` resolving a stub client per registered name."""

    def _factory(capability: CompiledA2ACapability) -> StubMcpClient:
        return clients[capability.agent]

    return _factory


def _runtime(
    *,
    plans: Sequence[AgentPlan],
    deps: StubDepsFactory,
    container: LoomContainer,
    clients: Mapping[str, StubMcpClient] | None = None,
    a2a_clients: Mapping[str, StubMcpClient] | None = None,
    remote_clients: str = "required",
    startup_timeout_ms: int = 500,
    health_cache_ttl_ms: int = 5,
    with_mcp_factory: bool = True,
    with_a2a_factory: bool = True,
) -> AgentRuntime:
    """Assemble an ``AgentRuntime`` over local stubs only."""
    mcp = dict(clients or {})
    a2a = dict(a2a_clients or {})
    return AgentRuntime(
        plans=list(plans),
        config=make_ai_config(
            mcp_servers=make_mcp_servers(*mcp),
            remote_clients=remote_clients,
            startup_timeout_ms=startup_timeout_ms,
            health_cache_ttl_ms=health_cache_ttl_ms,
        ),
        engine_provider=CountingEngineProvider(),  # type: ignore[arg-type]
        deps=deps,
        container=container,
        mcp_client_factory=mcp_client_factory(mcp) if with_mcp_factory else None,  # type: ignore[arg-type]
        a2a_client_factory=_a2a_client_factory(a2a) if with_a2a_factory else None,
    )


def _refusing_client(server: str, log: list[str]) -> StubMcpClient:
    """Build a stub client whose connection is refused with a URL-bearing text."""
    return StubMcpClient(
        label=server,
        session=RecordingMcpSession(),
        log=log,
        connect_error=_REFUSED,
    )


def _hanging_client(server: str, log: list[str]) -> StubMcpClient:
    """Build a stub client whose connection never completes."""
    return StubMcpClient(
        label=server,
        session=RecordingMcpSession(),
        log=log,
        never_connects=True,
    )


async def _until(predicate: Callable[[], bool], *, timeout: float = 2.0) -> None:
    """Poll *predicate* until it holds, failing the test if it never does."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("the runtime never reached the expected state")


class TestArranqueRequerido:
    """``required`` is today's behaviour, byte for byte (AC6)."""

    async def test_aborta_cuando_el_servidor_mcp_rechaza_la_conexion(
        self, lifecycle_log: list[str], deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A refused MCP connection still aborts start-up with its stable code."""
        runtime = _runtime(
            plans=(make_plan(_AGENT, capabilities=(make_mcp_capability(_SERVER_A),)),),
            clients={_SERVER_A: _refusing_client(_SERVER_A, lifecycle_log)},
            deps=deps,
            container=container,
        )

        with pytest.raises(AgentCompilationError) as failure:
            await runtime.__aenter__()

        assert AgentErrorCode.MCP_SERVER_UNREACHABLE in _codes(failure.value)

    async def test_aborta_cuando_el_agente_a2a_rechaza_la_conexion(
        self, lifecycle_log: list[str], deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A refused A2A connection still aborts start-up with its stable code."""
        runtime = _runtime(
            plans=(make_plan(_AGENT, capabilities=(_make_a2a_capability(),)),),
            a2a_clients={_REMOTE: _refusing_client(_REMOTE, lifecycle_log)},
            deps=deps,
            container=container,
        )

        with pytest.raises(AgentCompilationError) as failure:
            await runtime.__aenter__()

        assert AgentErrorCode.A2A_AGENT_UNREACHABLE in _codes(failure.value)

    async def test_aborta_cuando_la_conexion_se_cuelga(
        self, lifecycle_log: list[str], deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A hang past the budget still aborts start-up under ``required`` (AC8)."""
        runtime = _runtime(
            plans=(make_plan(_AGENT, capabilities=(make_mcp_capability(_SERVER_A),)),),
            clients={_SERVER_A: _hanging_client(_SERVER_A, lifecycle_log)},
            deps=deps,
            container=container,
            startup_timeout_ms=50,
        )

        with pytest.raises(AgentCompilationError) as failure:
            async with asyncio.timeout(0.5):
                await runtime.__aenter__()

        assert AgentErrorCode.MCP_SERVER_UNREACHABLE in _codes(failure.value)


class TestArranqueOpcional:
    """``optional`` drops a connection failure to a warning (AC7)."""

    async def test_arranca_cuando_el_servidor_mcp_rechaza_la_conexion(
        self, lifecycle_log: list[str], deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The runtime boots with no network behind the declared grant."""
        runtime = _runtime(
            plans=(make_plan(_AGENT, capabilities=(make_mcp_capability(_SERVER_A),)),),
            clients={_SERVER_A: _refusing_client(_SERVER_A, lifecycle_log)},
            deps=deps,
            container=container,
            remote_clients="optional",
        )

        async with runtime:
            assert runtime.has_agent(_AGENT)

    async def test_arranca_cuando_el_agente_a2a_rechaza_la_conexion(
        self, lifecycle_log: list[str], deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The tolerance covers the A2A clients too, not only MCP."""
        runtime = _runtime(
            plans=(make_plan(_AGENT, capabilities=(_make_a2a_capability(),)),),
            a2a_clients={_REMOTE: _refusing_client(_REMOTE, lifecycle_log)},
            deps=deps,
            container=container,
            remote_clients="optional",
        )

        async with runtime:
            assert runtime.has_agent(_AGENT)

    async def test_avisa_nombrando_el_servidor_cuando_tolera_el_fallo(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A tolerated failure is a WARNING carrying the code and the server."""
        runtime = _runtime(
            plans=(make_plan(_AGENT, capabilities=(make_mcp_capability(_SERVER_A),)),),
            clients={_SERVER_A: _refusing_client(_SERVER_A, lifecycle_log)},
            deps=deps,
            container=container,
            remote_clients="optional",
        )

        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            async with runtime:
                pass

        warnings = [record.getMessage() for record in caplog.records if record.levelno >= 30]
        assert any(
            _SERVER_A in message and AgentErrorCode.MCP_SERVER_UNREACHABLE in message
            for message in warnings
        )

    async def test_no_registra_la_direccion_en_el_aviso_cuando_tolera_el_fallo(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The transport's reason can carry a URL, so it stays out of WARNING."""
        runtime = _runtime(
            plans=(make_plan(_AGENT, capabilities=(make_mcp_capability(_SERVER_A),)),),
            clients={_SERVER_A: _refusing_client(_SERVER_A, lifecycle_log)},
            deps=deps,
            container=container,
            remote_clients="optional",
        )

        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            async with runtime:
                pass

        assert not [record for record in caplog.records if _REFUSED in record.getMessage()], (
            "the transport's reason reached routine logs"
        )

    async def test_registra_el_motivo_en_debug_cuando_tolera_el_fallo(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The reason stays available to an operator who asks for DEBUG."""
        runtime = _runtime(
            plans=(make_plan(_AGENT, capabilities=(make_mcp_capability(_SERVER_A),)),),
            clients={_SERVER_A: _refusing_client(_SERVER_A, lifecycle_log)},
            deps=deps,
            container=container,
            remote_clients="optional",
        )

        with caplog.at_level(logging.DEBUG, logger=_LOGGER):
            async with runtime:
                pass

        assert any(_REFUSED in record.getMessage() for record in caplog.records)

    async def test_reporta_la_dependencia_no_disponible_tras_la_primera_sonda(
        self, lifecycle_log: list[str], deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The health probe names the missing dependency once its first pass ran."""
        runtime = _runtime(
            plans=(make_plan(_AGENT, capabilities=(make_mcp_capability(_SERVER_A),)),),
            clients={_SERVER_A: _refusing_client(_SERVER_A, lifecycle_log)},
            deps=deps,
            container=container,
            remote_clients="optional",
        )

        async with runtime:
            await _until(lambda: runtime._health.get(_AGENT) is not None)
            health = await runtime.health(_AGENT)

        assert health.checks[f"mcp:{_SERVER_A}"] == "unavailable"

    async def test_cierra_los_clientes_abiertos_cuando_tolera_un_fallo(
        self, lifecycle_log: list[str], deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The clients that did open are still closed on exit."""
        clients = {
            _SERVER_A: _refusing_client(_SERVER_A, lifecycle_log),
            _SERVER_B: StubMcpClient(
                label=_SERVER_B, session=RecordingMcpSession(), log=lifecycle_log
            ),
        }
        plan = make_plan(
            _AGENT,
            capabilities=(make_mcp_capability(_SERVER_A), make_mcp_capability(_SERVER_B)),
        )
        runtime = _runtime(
            plans=(plan,),
            clients=clients,
            deps=deps,
            container=container,
            remote_clients="optional",
        )

        async with runtime:
            pass

        assert lifecycle_log == [f"open:{_SERVER_B}", f"close:{_SERVER_B}"]


class TestPresupuestoTrasTolerar:
    """The filter pass gets a fresh budget, and still fails closed (AC8)."""

    @staticmethod
    def _mixed_plan(
        *, list_delay_ms: int
    ) -> tuple[AgentPlan, dict[str, StubMcpClient], RecordingMcpSession]:
        """One hanging server plus one opened server carrying a declared filter."""
        log: list[str] = []
        session = RecordingMcpSession(tools=("alpha",), list_delay_ms=list_delay_ms)
        clients = {
            _SERVER_A: _hanging_client(_SERVER_A, log),
            _SERVER_B: StubMcpClient(label=_SERVER_B, session=session, log=log),
        }
        plan = make_plan(
            _AGENT,
            capabilities=(
                make_mcp_capability(_SERVER_A),
                make_mcp_capability(_SERVER_B, include=("alpha",)),
            ),
        )
        return plan, clients, session

    async def test_verifica_el_filtro_del_servidor_abierto_cuando_otro_se_cuelga(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The hang spent the shared budget; the opened server's filter is still checked."""
        plan, clients, session = self._mixed_plan(list_delay_ms=0)
        runtime = _runtime(
            plans=(plan,),
            clients=clients,
            deps=deps,
            container=container,
            remote_clients="optional",
            startup_timeout_ms=50,
        )

        async with asyncio.timeout(1.0):
            async with runtime:
                listed = session.listed

        assert listed == 1, "the opened server's tools were never listed"

    async def test_aborta_cuando_el_listado_del_servidor_abierto_expira(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A connected server whose own listing times out still fails closed."""
        plan, clients, _session = self._mixed_plan(list_delay_ms=200)
        runtime = _runtime(
            plans=(plan,),
            clients=clients,
            deps=deps,
            container=container,
            remote_clients="optional",
            startup_timeout_ms=50,
        )

        with pytest.raises(AgentCompilationError) as failure:
            async with asyncio.timeout(1.0):
                await runtime.__aenter__()

        assert AgentErrorCode.MCP_SERVER_UNREACHABLE in _codes(failure.value)
        assert _SERVER_B in str(failure.value)

    async def test_aborta_cuando_se_cuelga_y_el_modo_es_requerido(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The same mixed hang still aborts start-up under ``required``."""
        plan, clients, _session = self._mixed_plan(list_delay_ms=0)
        runtime = _runtime(
            plans=(plan,),
            clients=clients,
            deps=deps,
            container=container,
            startup_timeout_ms=50,
        )

        with pytest.raises(AgentCompilationError) as failure:
            async with asyncio.timeout(1.0):
                await runtime.__aenter__()

        assert AgentErrorCode.MCP_SERVER_UNREACHABLE in _codes(failure.value)
        assert _SERVER_A in str(failure.value)


class TestFiltroDeServidorCaido:
    """FR-025 is waived only for the servers that never connected (AC9)."""

    async def test_arranca_cuando_el_filtro_apunta_a_un_servidor_caido(
        self, lifecycle_log: list[str], deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A filter on a server that never opened does not fail start-up."""
        runtime = _runtime(
            plans=(
                make_plan(_AGENT, capabilities=(make_mcp_capability(_SERVER_A, include=("x",)),)),
            ),
            clients={_SERVER_A: _refusing_client(_SERVER_A, lifecycle_log)},
            deps=deps,
            container=container,
            remote_clients="optional",
        )

        async with runtime:
            assert runtime.has_agent(_AGENT)

    async def test_ejecuta_el_agente_cuando_sus_otros_servidores_abrieron(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        """An agent whose other servers opened still serves runs."""
        clients = {
            _SERVER_A: _refusing_client(_SERVER_A, lifecycle_log),
            _SERVER_B: StubMcpClient(
                label=_SERVER_B,
                session=RecordingMcpSession(tools=("alpha",)),
                log=lifecycle_log,
            ),
        }
        plan = make_plan(
            _AGENT,
            capabilities=(
                make_mcp_capability(_SERVER_A, include=("x",)),
                make_mcp_capability(_SERVER_B, include=("alpha",)),
            ),
        )
        runtime = _runtime(
            plans=(plan,),
            clients=clients,
            deps=deps,
            container=container,
            remote_clients="optional",
        )

        async with runtime:
            result = await runtime.run(_AGENT, "question", identity=identity)

        assert result.output == {"answer": "42"}


class TestFabricaAusente:
    """A missing client factory is a wiring bug and stays fatal (AC10)."""

    @pytest.mark.parametrize("remote_clients", ["required", "optional"])
    async def test_aborta_cuando_falta_la_fabrica_mcp(
        self, deps: StubDepsFactory, container: LoomContainer, remote_clients: str
    ) -> None:
        """An ``mcp`` grant with no factory aborts under both values."""
        runtime = _runtime(
            plans=(make_plan(_AGENT, capabilities=(make_mcp_capability(_SERVER_A),)),),
            deps=deps,
            container=container,
            remote_clients=remote_clients,
            with_mcp_factory=False,
        )

        with pytest.raises(AgentCompilationError) as failure:
            await runtime.__aenter__()

        assert _MISSING_MCP_FACTORY in str(failure.value)

    @pytest.mark.parametrize("remote_clients", ["required", "optional"])
    async def test_aborta_cuando_falta_la_fabrica_a2a(
        self, deps: StubDepsFactory, container: LoomContainer, remote_clients: str
    ) -> None:
        """An ``a2a`` grant with no factory aborts under both values."""
        runtime = _runtime(
            plans=(make_plan(_AGENT, capabilities=(_make_a2a_capability(),)),),
            deps=deps,
            container=container,
            remote_clients=remote_clients,
            with_a2a_factory=False,
        )

        with pytest.raises(AgentCompilationError) as failure:
            await runtime.__aenter__()

        assert _MISSING_A2A_FACTORY in str(failure.value)

    async def test_distingue_la_fabrica_ausente_de_una_conexion_fallida(
        self, lifecycle_log: list[str], deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The wiring bug is reported apart from the tolerated connection failure."""
        plan = make_plan(
            _AGENT,
            capabilities=(make_mcp_capability(_SERVER_A), _make_a2a_capability()),
        )
        runtime = _runtime(
            plans=(plan,),
            clients={_SERVER_A: _refusing_client(_SERVER_A, lifecycle_log)},
            deps=deps,
            container=container,
            remote_clients="optional",
            with_a2a_factory=False,
        )

        with pytest.raises(AgentCompilationError) as failure:
            await runtime.__aenter__()

        assert _codes(failure.value) == {AgentErrorCode.A2A_AGENT_UNREACHABLE}
        assert _MISSING_A2A_FACTORY in str(failure.value)
        assert _REFUSED not in str(failure.value)


class TestEjecucionSinClienteDeArranque:
    """Nothing becomes lazy: the run path is untouched (AC11)."""

    async def test_ejecuta_cuando_el_servidor_no_abrio_en_el_arranque(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        """A run against a never-opened server behaves exactly as on master.

        The engine builds its own toolset and connects on its own, so the run
        never reads the start-up session and the start-up client is not
        reopened. Pinned here so a change to it is a decision, not a surprise.
        """
        client = _refusing_client(_SERVER_A, lifecycle_log)
        runtime = _runtime(
            plans=(make_plan(_AGENT, capabilities=(make_mcp_capability(_SERVER_A),)),),
            clients={_SERVER_A: client},
            deps=deps,
            container=container,
            remote_clients="optional",
        )

        async with runtime:
            result = await runtime.run(_AGENT, "question", identity=identity)

        assert result.output == {"answer": "42"}
        assert lifecycle_log == []
