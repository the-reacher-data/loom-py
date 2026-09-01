"""Lifecycle, start-up validation and shared-session tests of ``AgentRuntime``.

Covers T072 (open/close ordering and per-worker client sharing), T073 (tool
filters applied against the tools the server actually offers), T077 (single
exit stack, task affinity, bounded concurrent start-up), T078 (a shared MCP
session is never poisoned by a cancelled neighbour) and T079 (read-only drift
re-verified against live SQL configuration).

Every dependency is a local stub: no network, no credential, no token.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.runtime import AgentRuntime, SharedMcpSession
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from tests.integration.ai.conftest import (
    CountingEngineProvider,
    InterleavingSensitiveSession,
    RecordingMcpSession,
    ScriptedEngine,
    StubDepsFactory,
    StubMcpClient,
    make_ai_config,
    make_mcp_capability,
    make_mcp_servers,
    make_plan,
    make_sql_capability,
    make_sql_config,
    mcp_client_factory,
    mcp_server_url,
)

_SERVER_A = "alpha-tools"
_SERVER_B = "beta-tools"
_SERVER_C = "gamma-tools"


def _codes(error: AgentCompilationError) -> set[AgentErrorCode]:
    """Return the distinct issue codes an aggregated compilation error carries."""
    return {issue.code for issue in error.issues}


def _build_runtime(
    *,
    plans: tuple[object, ...],
    clients: dict[str, StubMcpClient],
    provider: CountingEngineProvider,
    deps: StubDepsFactory,
    container: LoomContainer,
    config_kwargs: dict[str, Any] | None = None,
    sql_config: object | None = None,
) -> AgentRuntime:
    """Assemble an ``AgentRuntime`` over local stubs only."""
    return AgentRuntime(
        plans=list(plans),  # type: ignore[arg-type]
        config=make_ai_config(mcp_servers=make_mcp_servers(*clients), **(config_kwargs or {})),
        engine_provider=provider,  # type: ignore[arg-type]
        deps=deps,
        container=container,
        sql_config=sql_config,  # type: ignore[arg-type]
        mcp_client_factory=mcp_client_factory(clients),  # type: ignore[arg-type]
    )


class TestCicloDeVida:
    """Opening and closing the live clients through one exit stack (T072/T077)."""

    async def test_abre_todos_los_clientes_cuando_entra_el_runtime(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """``__aenter__`` opens every MCP client the plans declare."""
        clients = {
            _SERVER_A: StubMcpClient(label="a", session=RecordingMcpSession(), log=lifecycle_log),
            _SERVER_B: StubMcpClient(label="b", session=RecordingMcpSession(), log=lifecycle_log),
        }
        plan = make_plan(
            capabilities=(make_mcp_capability(_SERVER_A), make_mcp_capability(_SERVER_B)),
        )
        runtime = _build_runtime(
            plans=(plan,),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        async with runtime:
            assert sorted(lifecycle_log) == ["open:a", "open:b"]

    async def test_cierra_en_orden_inverso_cuando_sale_el_runtime(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Clients close in strict reverse order of the order they opened in."""
        clients = {
            _SERVER_A: StubMcpClient(label="a", session=RecordingMcpSession(), log=lifecycle_log),
            _SERVER_B: StubMcpClient(
                label="b",
                session=RecordingMcpSession(),
                log=lifecycle_log,
                connect_delay_ms=20,
            ),
        }
        plan = make_plan(
            capabilities=(make_mcp_capability(_SERVER_A), make_mcp_capability(_SERVER_B)),
        )
        runtime = _build_runtime(
            plans=(plan,),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        async with runtime:
            pass

        assert lifecycle_log == ["open:a", "open:b", "close:b", "close:a"]

    async def test_construye_un_solo_motor_cuando_hay_varias_invocaciones(
        self,
        lifecycle_log: list[str],
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Engines are built once per plan, never per request (FR-026)."""
        clients = {
            _SERVER_A: StubMcpClient(label="a", session=RecordingMcpSession(), log=lifecycle_log)
        }
        provider = CountingEngineProvider()
        runtime = _build_runtime(
            plans=(make_plan(capabilities=(make_mcp_capability(_SERVER_A),)),),
            clients=clients,
            provider=provider,
            deps=deps,
            container=container,
        )

        async with runtime:
            await runtime.run("analyst", "p1", identity=identity)
            await runtime.run("analyst", "p2", identity=identity)
            async with runtime.run_stream("analyst", "p3", identity=identity) as stream:
                async for _ in stream:
                    pass

        assert provider.calls == ["analyst"]

    async def test_abre_el_cliente_una_sola_vez_cuando_hay_varias_invocaciones(
        self,
        lifecycle_log: list[str],
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The MCP client is shared per worker, not opened per call (FR-026)."""
        clients = {
            _SERVER_A: StubMcpClient(label="a", session=RecordingMcpSession(), log=lifecycle_log)
        }
        runtime = _build_runtime(
            plans=(make_plan(capabilities=(make_mcp_capability(_SERVER_A),)),),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        async with runtime:
            await runtime.run("analyst", "p1", identity=identity)
            await runtime.run("analyst", "p2", identity=identity)
            async with runtime.run_stream("analyst", "p3", identity=identity) as stream:
                async for _ in stream:
                    pass

        assert lifecycle_log.count("open:a") == 1

    async def test_rechaza_el_cierre_cuando_sale_en_otra_tarea(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Exiting the stack from another task is refused, not silently done (T077)."""
        clients = {
            _SERVER_A: StubMcpClient(label="a", session=RecordingMcpSession(), log=lifecycle_log)
        }
        runtime = _build_runtime(
            plans=(make_plan(capabilities=(make_mcp_capability(_SERVER_A),)),),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )
        await runtime.__aenter__()

        async def _exit_elsewhere() -> None:
            await runtime.__aexit__(None, None, None)

        try:
            with pytest.raises(RuntimeError):
                await asyncio.create_task(_exit_elsewhere())
        finally:
            await runtime.__aexit__(None, None, None)


class TestArranqueConcurrente:
    """Start-up is concurrent, bounded and never hangs (T077)."""

    async def test_arranca_en_paralelo_cuando_hay_varios_clientes(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Two 40 ms connections must not cost 80 ms: start-up is concurrent."""
        clients = {
            _SERVER_A: StubMcpClient(
                label="a", session=RecordingMcpSession(), log=lifecycle_log, connect_delay_ms=40
            ),
            _SERVER_B: StubMcpClient(
                label="b", session=RecordingMcpSession(), log=lifecycle_log, connect_delay_ms=40
            ),
        }
        plan = make_plan(
            capabilities=(make_mcp_capability(_SERVER_A), make_mcp_capability(_SERVER_B)),
        )
        runtime = _build_runtime(
            plans=(plan,),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
            config_kwargs={"startup_timeout_ms": 1000},
        )

        loop = asyncio.get_running_loop()
        started = loop.time()
        async with runtime:
            elapsed = loop.time() - started

        assert elapsed < 0.070, f"start-up took {elapsed:.3f}s; the two clients ran in sequence"

    async def test_aborta_nombrando_el_servidor_cuando_no_conecta(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """An unreachable server aborts start-up naming its registered server."""
        clients = {
            _SERVER_A: StubMcpClient(
                label="a",
                session=RecordingMcpSession(),
                log=lifecycle_log,
                never_connects=True,
            )
        }
        runtime = _build_runtime(
            plans=(make_plan(capabilities=(make_mcp_capability(_SERVER_A),)),),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
            config_kwargs={"startup_timeout_ms": 50},
        )

        # The outer timeout is twice the declared budget: if start-up hangs the
        # test fails on TimeoutError instead of blocking the suite.
        with pytest.raises(AgentCompilationError) as failure:
            async with asyncio.timeout(0.1):
                await runtime.__aenter__()

        assert _SERVER_A in str(failure.value)

    async def test_reporta_mcp_server_unreachable_cuando_no_conecta(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The abort carries the stable ``MCP_SERVER_UNREACHABLE`` code."""
        clients = {
            _SERVER_A: StubMcpClient(
                label="a",
                session=RecordingMcpSession(),
                log=lifecycle_log,
                never_connects=True,
            )
        }
        runtime = _build_runtime(
            plans=(make_plan(capabilities=(make_mcp_capability(_SERVER_A),)),),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
            config_kwargs={"startup_timeout_ms": 50},
        )

        with pytest.raises(AgentCompilationError) as failure:
            async with asyncio.timeout(0.1):
                await runtime.__aenter__()

        assert AgentErrorCode.MCP_SERVER_UNREACHABLE in _codes(failure.value)


class TestPresupuestoDeArranque:
    """``startup_timeout_ms`` bounds the whole of start-up exactly once."""

    @staticmethod
    def _slow_listing_runtime(
        *,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> AgentRuntime:
        """Three servers whose tool listings alone outlast a single budget."""
        servers = (_SERVER_A, _SERVER_B, _SERVER_C)
        clients = {
            server: StubMcpClient(
                label=server,
                session=RecordingMcpSession(tools=("alpha",), list_delay_ms=50),
                log=lifecycle_log,
            )
            for server in servers
        }
        plans = tuple(
            make_plan(
                f"agent-{index}",
                capabilities=(make_mcp_capability(server, include=("alpha",)),),
            )
            for index, server in enumerate(servers)
        )
        return _build_runtime(
            plans=plans,
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
            config_kwargs={"startup_timeout_ms": 80},
        )

    async def test_no_supera_el_presupuesto_cuando_hay_varias_capacidades_mcp(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Three 50 ms listings share one 80 ms budget, they do not each get one."""
        runtime = self._slow_listing_runtime(
            lifecycle_log=lifecycle_log, deps=deps, container=container
        )

        loop = asyncio.get_running_loop()
        started = loop.time()
        with pytest.raises(AgentCompilationError):
            await runtime.__aenter__()
        elapsed = loop.time() - started

        assert elapsed < 0.130, (
            f"start-up took {elapsed:.3f}s: the budget was spent once per capability"
        )

    async def test_nombra_el_servidor_cuando_expira_el_presupuesto(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The expired budget is a coded issue naming the server, not a bare timeout."""
        runtime = self._slow_listing_runtime(
            lifecycle_log=lifecycle_log, deps=deps, container=container
        )

        with pytest.raises(AgentCompilationError) as failure:
            await runtime.__aenter__()

        assert AgentErrorCode.MCP_SERVER_UNREACHABLE in _codes(failure.value)
        assert _SERVER_C in str(failure.value)

    async def test_lista_las_herramientas_una_vez_cuando_dos_planes_comparten_servidor(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """One shared session means one ``list_tools`` round trip, not one per plan."""
        session = RecordingMcpSession(tools=("alpha", "beta"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        plans = (
            make_plan("first", capabilities=(make_mcp_capability(_SERVER_A, include=("alpha",)),)),
            make_plan("second", capabilities=(make_mcp_capability(_SERVER_A, include=("beta",)),)),
        )
        runtime = _build_runtime(
            plans=plans,
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        async with runtime:
            assert session.listed == 1

    async def test_aplica_el_filtro_de_cada_plan_cuando_comparten_servidor(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The single listing is still checked against every plan's own filter."""
        session = RecordingMcpSession(tools=("alpha", "beta"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        plans = (
            make_plan("first", capabilities=(make_mcp_capability(_SERVER_A, include=("alpha",)),)),
            make_plan("second", capabilities=(make_mcp_capability(_SERVER_A, include=("gamma",)),)),
        )
        runtime = _build_runtime(
            plans=plans,
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        with pytest.raises(AgentCompilationError) as failure:
            await runtime.__aenter__()

        assert AgentErrorCode.TOOL_FILTER_MATCHES_NOTHING in _codes(failure.value)
        assert "second" in str(failure.value)


class TestFiltroDeHerramientas:
    """Declared filters are applied against the real tool list (T073)."""

    async def test_aborta_cuando_el_filtro_no_casa_ninguna_herramienta(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A filter matching none of the offered tools fails start-up (FR-025)."""
        session = RecordingMcpSession(tools=("alpha", "beta"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        capability = make_mcp_capability(_SERVER_A, include=("gamma",))
        runtime = _build_runtime(
            plans=(make_plan(capabilities=(capability,)),),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        with pytest.raises(AgentCompilationError) as failure:
            await runtime.__aenter__()

        assert AgentErrorCode.TOOL_FILTER_MATCHES_NOTHING in _codes(failure.value)

    async def test_arranca_cuando_el_filtro_casa_una_herramienta(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A filter keeping at least one offered tool starts up normally."""
        session = RecordingMcpSession(tools=("alpha", "beta"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        capability = make_mcp_capability(_SERVER_A, include=("alpha",))
        runtime = _build_runtime(
            plans=(make_plan(capabilities=(capability,)),),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        async with runtime:
            assert runtime.has_agent("analyst")

    async def test_arranca_cuando_un_glob_casa_parte_de_las_herramientas(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """``include`` is glob-matched, so a pattern selects a family of tools."""
        session = RecordingMcpSession(tools=("search_web", "search_docs", "delete_index"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        capability = make_mcp_capability(_SERVER_A, include=("search_*",))
        runtime = _build_runtime(
            plans=(make_plan(capabilities=(capability,)),),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        async with runtime:
            assert runtime.has_agent("analyst")

    async def test_aborta_cuando_el_glob_no_casa_ninguna_herramienta(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A glob is matched, not compared literally: an unmatched one still fails."""
        session = RecordingMcpSession(tools=("search_web", "delete_index"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        capability = make_mcp_capability(_SERVER_A, include=("write_*",))
        runtime = _build_runtime(
            plans=(make_plan(capabilities=(capability,)),),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        with pytest.raises(AgentCompilationError) as failure:
            await runtime.__aenter__()

        assert AgentErrorCode.TOOL_FILTER_MATCHES_NOTHING in _codes(failure.value)

    async def test_aborta_cuando_el_exclude_vacia_lo_que_el_glob_incluye(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """``exclude`` is applied after ``include`` and can empty the selection."""
        session = RecordingMcpSession(tools=("search_web", "search_docs"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        capability = make_mcp_capability(_SERVER_A, include=("search_*",), exclude=("search_*",))
        runtime = _build_runtime(
            plans=(make_plan(capabilities=(capability,)),),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        with pytest.raises(AgentCompilationError) as failure:
            await runtime.__aenter__()

        assert AgentErrorCode.TOOL_FILTER_MATCHES_NOTHING in _codes(failure.value)

    async def test_nombra_el_servidor_registrado_y_no_su_url_cuando_el_filtro_falla(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The issue names the registered server: a URL would publish the topology."""
        session = RecordingMcpSession(tools=("alpha",))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        capability = make_mcp_capability(_SERVER_A, include=("gamma",))
        runtime = _build_runtime(
            plans=(make_plan(capabilities=(capability,)),),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        with pytest.raises(AgentCompilationError) as failure:
            await runtime.__aenter__()

        message = str(failure.value)
        assert _SERVER_A in message and mcp_server_url(_SERVER_A) not in message


class TestDerivaDeSoloLectura:
    """Live SQL configuration is re-verified at start-up (T079, FR-046)."""

    async def test_aborta_cuando_la_conexion_ya_no_es_de_solo_lectura(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A plan compiled against a read-only connection refuses a writable one."""
        plan = make_plan(capabilities=(make_sql_capability("reporting", readonly=True),))
        runtime = _build_runtime(
            plans=(plan,),
            clients={},
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
            sql_config=make_sql_config("reporting", readonly=False),
        )

        with pytest.raises(AgentCompilationError) as failure:
            await runtime.__aenter__()

        assert AgentErrorCode.SQL_READONLY_DRIFT in _codes(failure.value)

    async def test_nombra_la_conexion_cuando_hay_deriva(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The drift issue names the connection the operator must fix."""
        plan = make_plan(capabilities=(make_sql_capability("reporting", readonly=True),))
        runtime = _build_runtime(
            plans=(plan,),
            clients={},
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
            sql_config=make_sql_config("reporting", readonly=False),
        )

        with pytest.raises(AgentCompilationError) as failure:
            await runtime.__aenter__()

        assert "reporting" in str(failure.value)

    async def test_arranca_cuando_la_conexion_sigue_siendo_de_solo_lectura(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """No drift means start-up proceeds with the SQL capability live."""
        plan = make_plan(capabilities=(make_sql_capability("reporting", readonly=True),))
        runtime = _build_runtime(
            plans=(plan,),
            clients={},
            provider=CountingEngineProvider(engines={"analyst": ScriptedEngine()}),
            deps=deps,
            container=container,
            sql_config=make_sql_config("reporting", readonly=True),
        )

        async with runtime:
            assert runtime.agent_names() == ("analyst",)


class TestSesionMcpCompartida:
    """A cancelled caller must not poison a shared JSON-RPC session (T078)."""

    async def test_completa_la_llamada_en_vuelo_cuando_se_cancela_el_llamador(self) -> None:
        """The shielded in-flight call runs to completion despite cancellation."""
        session = InterleavingSensitiveSession(delay_ms=20)
        shared = SharedMcpSession(session, label="alpha")  # type: ignore[arg-type]

        first = asyncio.create_task(shared.call_tool("echo", {"token": "a"}))
        while not session.started:
            await asyncio.sleep(0)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first

        assert session.completed == ["a"], "the in-flight JSON-RPC call was abandoned mid-frame"

    async def test_devuelve_su_propio_resultado_cuando_el_vecino_se_cancela(self) -> None:
        """The surviving caller gets its own answer, never the cancelled one's."""
        session = InterleavingSensitiveSession(delay_ms=20)
        shared = SharedMcpSession(session, label="alpha")  # type: ignore[arg-type]

        first = asyncio.create_task(shared.call_tool("echo", {"token": "a"}))
        while not session.started:
            await asyncio.sleep(0)
        second = asyncio.create_task(shared.call_tool("echo", {"token": "b"}))
        await asyncio.sleep(0)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first

        assert await second == "b"

    async def test_no_entrelaza_las_llamadas_cuando_comparten_la_sesion(self) -> None:
        """Two concurrent runs over one session are serialised, never interleaved."""
        session = InterleavingSensitiveSession(delay_ms=20)
        shared = SharedMcpSession(session, label="alpha")  # type: ignore[arg-type]

        first = asyncio.create_task(shared.call_tool("echo", {"token": "a"}))
        while not session.started:
            await asyncio.sleep(0)
        second = asyncio.create_task(shared.call_tool("echo", {"token": "b"}))
        await asyncio.sleep(0)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        await second

        assert session.interleaved is False
