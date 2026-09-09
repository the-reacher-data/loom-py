"""``AgentRuntime`` opening and validating a use-case-only MCP server (T203).

A ``Mcp()`` marker's server never appears in any compiled agent plan, so it
would be invisible to ``_open_clients``/``_verify_tool_filters`` unless its
own capability is folded into their inputs. This module pins that folding,
both of its early-return traps (``_open_clients``'s ``if not mcp and not
a2a``, ``_verify_tool_filters``'s ``if not targets``), the standalone
``include`` check, and the grant a resolved handle would read from.

Every dependency is a local stub: no network, no credential, no token.
"""

from __future__ import annotations

import pytest

from loom.ai.errors import AgentCompilationError, AgentErrorCode, AgentRunError, AgentRunErrorCode
from loom.ai.runtime import AgentRuntime
from loom.ai.runtime._grants import McpGrantView
from loom.ai.runtime._lifecycle import UseCaseMcpGrant
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
_USECASE = "orders.get_order_status"
_PARAMETER = "gateway"


def _codes(error: AgentCompilationError) -> set[AgentErrorCode]:
    return {issue.code for issue in error.issues}


def _grant(
    server: str,
    *,
    include: tuple[str, ...],
    usecase: str = _USECASE,
    parameter: str = _PARAMETER,
) -> UseCaseMcpGrant:
    return UseCaseMcpGrant(
        capability=make_mcp_capability(server, include=include),
        usecase=usecase,
        parameter=parameter,
    )


def _runtime(
    *,
    plans: tuple[object, ...] = (),
    clients: dict[str, StubMcpClient],
    use_case_mcp: tuple[UseCaseMcpGrant, ...],
    deps: StubDepsFactory,
    container: LoomContainer,
    remote_clients: str = "required",
) -> AgentRuntime:
    return AgentRuntime(
        plans=list(plans),  # type: ignore[arg-type]
        config=make_ai_config(
            mcp_servers=make_mcp_servers(*clients), remote_clients=remote_clients
        ),
        engine_provider=CountingEngineProvider(),  # type: ignore[arg-type]
        deps=deps,
        container=container,
        mcp_client_factory=mcp_client_factory(clients),  # type: ignore[arg-type]
        use_case_mcp=use_case_mcp,
    )


class TestServidorSoloDeclaradoPorUnCasoDeUso:
    """Ningún plan de agente lo nombra; solo el binding ``Mcp()`` lo hace."""

    async def test_se_abre_y_se_lista_aunque_ningun_agente_lo_declare(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        session = RecordingMcpSession(tools=("search", "delete"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        runtime = _runtime(
            clients=clients,
            use_case_mcp=(_grant(_SERVER_A, include=("search",)),),
            deps=deps,
            container=container,
        )

        async with runtime:
            assert lifecycle_log == ["open:a"]
            assert session.listed == 1
            grant = runtime.use_case_mcp_grant(_SERVER_A)

        assert grant is not None
        assert grant.capability.server == _SERVER_A

    async def test_cero_planes_de_agente_arranca_verifica_include_y_la_llamada_funciona(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        """FR-04/AC2/AC8a of spec 015: zero agent plans, use-case-only server."""
        session = RecordingMcpSession(
            tools=("search", "delete"),
            schemas=("search",),
            results={"search": {"hits": 3}},
        )
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        binding = _grant(_SERVER_A, include=("search",))
        runtime = _runtime(
            clients=clients,
            use_case_mcp=(binding,),
            deps=deps,
            container=container,
        )

        async with runtime:
            grant = runtime.use_case_mcp_grant(_SERVER_A)
            assert grant is not None
            # The caller's own filter comes from its own binding (S6), never
            # from the shared substrate grant this runtime hands back — see
            # 'plan.md's "Where the caller's include comes from" note.
            view = McpGrantView(
                agent=_USECASE,
                capability=binding.capability,
                session=grant.session,
                catalogue=grant.catalogue,
                timeout_s=1.0,
                identity=identity,
                observability=None,
            )
            assert view.tools() == ("search",)
            result = await view.call_untyped("search", {})
            assert result == {"hits": 3}
            with pytest.raises(AgentRunError) as excinfo:
                await view.call_untyped("delete", {})
            assert excinfo.value.code is AgentRunErrorCode.TOOL_UNKNOWN

    async def test_un_include_que_no_casa_nada_aborta_nombrando_caso_de_uso_parametro_y_servidor(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        session = RecordingMcpSession(tools=("search",))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        runtime = _runtime(
            clients=clients,
            use_case_mcp=(_grant(_SERVER_A, include=("no-such-tool",)),),
            deps=deps,
            container=container,
        )

        with pytest.raises(AgentCompilationError) as failure:
            await runtime.__aenter__()

        assert AgentErrorCode.TOOL_FILTER_MATCHES_NOTHING in _codes(failure.value)
        message = str(failure.value)
        assert _USECASE in message
        assert _PARAMETER in message
        assert _SERVER_A in message

    async def test_la_expiracion_del_listado_sigue_abortando(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        session = RecordingMcpSession(tools=("search",), list_delay_ms=50)
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        runtime = AgentRuntime(
            plans=[],
            config=make_ai_config(mcp_servers=make_mcp_servers(_SERVER_A), startup_timeout_ms=10),
            engine_provider=CountingEngineProvider(),  # type: ignore[arg-type]
            deps=deps,
            container=container,
            mcp_client_factory=mcp_client_factory(clients),  # type: ignore[arg-type]
            use_case_mcp=(_grant(_SERVER_A, include=("search",)),),
        )

        with pytest.raises(AgentCompilationError) as failure:
            await runtime.__aenter__()

        assert AgentErrorCode.MCP_SERVER_UNREACHABLE in _codes(failure.value)


class TestServidorCompartidoConUnAgente:
    """Un agente y un caso de uso declaran el mismo servidor."""

    async def test_el_cliente_y_el_listado_ocurren_una_sola_vez(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        session = RecordingMcpSession(tools=("search", "delete"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        plan = make_plan(capabilities=(make_mcp_capability(_SERVER_A, include=("search",)),))
        runtime = _runtime(
            plans=(plan,),
            clients=clients,
            use_case_mcp=(_grant(_SERVER_A, include=("delete",)),),
            deps=deps,
            container=container,
        )

        async with runtime:
            assert lifecycle_log == ["open:a"]
            assert session.listed == 1
            grant = runtime.use_case_mcp_grant(_SERVER_A)

        assert grant is not None

    async def test_el_include_del_caso_de_uso_alcanza_lo_que_el_agente_excluye(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        session = RecordingMcpSession(
            tools=("search", "delete"), schemas=("delete",), results={"delete": {"ok": True}}
        )
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        plan = make_plan(capabilities=(make_mcp_capability(_SERVER_A, include=("search",)),))
        binding = _grant(_SERVER_A, include=("delete",))
        runtime = _runtime(
            plans=(plan,),
            clients=clients,
            use_case_mcp=(binding,),
            deps=deps,
            container=container,
        )

        async with runtime:
            agent_grant = runtime.grants("analyst").mcp[_SERVER_A]
            agent_view = McpGrantView(
                agent="analyst",
                capability=agent_grant.capability,
                session=agent_grant.session,
                catalogue=agent_grant.catalogue,
                timeout_s=1.0,
                identity=identity,
                observability=None,
            )
            assert agent_view.tools() == ("search",)

            use_case_grant = runtime.use_case_mcp_grant(_SERVER_A)
            assert use_case_grant is not None
            # Same note as above: the caller's own filter is its own
            # binding's 'include', not the shared substrate grant's.
            use_case_view = McpGrantView(
                agent=_USECASE,
                capability=binding.capability,
                session=use_case_grant.session,
                catalogue=use_case_grant.catalogue,
                timeout_s=1.0,
                identity=identity,
                observability=None,
            )
            assert use_case_view.tools() == ("delete",)
            result = await use_case_view.call_untyped("delete", {})
            assert result == {"ok": True}


class TestDosCasosDeUsoSobreElMismoServidor:
    """FR-15/AC3a: cada uno con su propio ``include``, sin colisionar."""

    async def test_cada_lookup_es_inequivoco(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        session = RecordingMcpSession(tools=("search", "delete"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        runtime = _runtime(
            clients=clients,
            use_case_mcp=(
                _grant(_SERVER_A, include=("search",), usecase="orders.first", parameter="a"),
                _grant(_SERVER_A, include=("delete",), usecase="orders.second", parameter="b"),
            ),
            deps=deps,
            container=container,
        )

        async with runtime:
            grant = runtime.use_case_mcp_grant(_SERVER_A)

        # Both bindings resolve the same server-keyed grant (FR-15): the
        # 'include' a caller sees comes from its own binding at resolution
        # time (PR3), not from this shared grant's catalogue.
        assert grant is not None
        assert {tool.name for tool in grant.catalogue} == {"search", "delete"}
        # H1: the shared substrate never collapses onto either binding's own
        # 'include' — it carries none at all, explicitly.
        assert grant.capability.include == ()
        assert grant.capability.exclude == ()


class TestGuardiasDeRetornoTemprano:
    """Las dos guardas que T203 debe atravesar (S6, `_lifecycle.py`)."""

    async def test_cero_planes_de_agente_igual_abre_el_servidor(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Mutar el paso 4 (contribuir bajo la guarda) debe poner esto en rojo."""
        session = RecordingMcpSession(tools=("search",))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        runtime = _runtime(
            clients=clients,
            use_case_mcp=(_grant(_SERVER_A, include=("search",)),),
            deps=deps,
            container=container,
        )

        async with runtime:
            assert lifecycle_log == ["open:a"]

    async def test_cero_planes_de_agente_igual_lista_y_verifica_el_include(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Mutar el paso 5 (contribuir bajo la guarda) debe poner esto en rojo:
        el catálogo se quedaría vacío y la llamada fallaría con TOOL_UNKNOWN
        contra un servidor que abrió limpio, en vez de abortar el arranque."""
        session = RecordingMcpSession(tools=("search",))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        runtime = _runtime(
            clients=clients,
            use_case_mcp=(_grant(_SERVER_A, include=("no-such-tool",)),),
            deps=deps,
            container=container,
        )

        with pytest.raises(AgentCompilationError) as failure:
            await runtime.__aenter__()

        assert AgentErrorCode.TOOL_FILTER_MATCHES_NOTHING in _codes(failure.value)


class TestUseCaseMcpGrantAntesDeEntrar:
    """H4: ``use_case_mcp_grant`` exige un runtime entrado, como ``_require_slot``."""

    def test_lanza_runtime_error_si_nunca_se_entro(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        runtime = _runtime(
            clients={},
            use_case_mcp=(_grant(_SERVER_A, include=("search",)),),
            deps=deps,
            container=container,
        )

        with pytest.raises(RuntimeError, match="must be entered before use"):
            runtime.use_case_mcp_grant(_SERVER_A)


class TestUseCaseGrantsSeLimpianAlSalir:
    """H5: ``__aexit__`` vacía ``_use_case_grants`` (T203), no solo ``_grants``.

    Blanco: ``use_case_mcp_grant`` nunca observa esto por su propia guarda
    (``_stack is None``), así que se lee el diccionario privado directamente,
    igual que ``test_runtime_remote_clients.py`` ya lee ``runtime._health``.
    Sin este vaciado un runtime reentrado repartiría, durante la ventana
    entre ``__aexit__`` y el siguiente ``__aenter__`` que fallase antes de
    reconstruirlo, grants que apuntan a sesiones ya cerradas.
    """

    async def test_tras_salir_el_diccionario_de_grants_queda_vacio(
        self, lifecycle_log: list[str], deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        session = RecordingMcpSession(tools=("search",))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        runtime = _runtime(
            clients=clients,
            use_case_mcp=(_grant(_SERVER_A, include=("search",)),),
            deps=deps,
            container=container,
        )

        async with runtime:
            assert runtime.use_case_mcp_grant(_SERVER_A) is not None

        assert runtime._use_case_grants == {}


class TestServidorToleradoInalcanzableNoDisparaElFiltro:
    """H6: la guarda ``if catalogue is None: continue`` de ``_use_case_filter_issues``.

    Bajo ``remote_clients: optional`` un servidor cuya conexión se toleró
    nunca fue listado, así que su ``include`` no puede compararse contra
    nada: la salida de este servidor la reporta la comprobación de conexión,
    no el filtro. Sin esta guarda un ``include`` que en teoría no casaría
    nada lanzaría ``TOOL_FILTER_MATCHES_NOTHING`` encima de un servidor ya
    reportado inalcanzable, duplicando el fallo con un código erróneo.
    """

    async def test_arranca_tolerando_el_fallo_sin_lanzar_filtro_no_casa_nada(
        self, lifecycle_log: list[str], deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        client = StubMcpClient(label="a", session=None, log=lifecycle_log, connect_error="refused")
        runtime = _runtime(
            clients={_SERVER_A: client},
            use_case_mcp=(_grant(_SERVER_A, include=("no-such-tool",)),),
            deps=deps,
            container=container,
            remote_clients="optional",
        )

        async with runtime:
            assert runtime.use_case_mcp_grant(_SERVER_A) is None


class TestDespliegueSoloDeAgentes:
    """Un despliegue sin ``Mcp()`` no cambia de comportamiento (``use_case_mcp=()``)."""

    async def test_el_arranque_no_cambia_cuando_no_hay_marcador(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        session = RecordingMcpSession(tools=("search",))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        plan = make_plan(capabilities=(make_mcp_capability(_SERVER_A, include=("search",)),))
        runtime = _runtime(
            plans=(plan,), clients=clients, use_case_mcp=(), deps=deps, container=container
        )

        async with runtime:
            assert runtime.use_case_mcp_grant(_SERVER_A) is None
            assert runtime.has_agent("analyst")
