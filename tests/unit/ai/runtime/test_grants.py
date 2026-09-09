"""``McpGrantView`` and ``SqlGrantView``: the concrete grant handles (T301–T303).

Covers the filter refusing an excluded tool without touching the session,
typed and untyped calls, the three network-facing failure modes and their
ordering, and the SQL view's role binding and byte-bound truncation.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import msgspec
import pytest

from loom.ai.abc import McpToolCallResult, McpToolInfo
from loom.ai.compiler import CompiledMcpCapability, CompiledSqlCapability
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.runtime._grants import McpGrantView, SqlGrantView
from loom.core.identity import ANONYMOUS, Identity
from loom.core.sql.abc.contracts import SqlColumn, SqlQueryResult
from loom.core.sql.config import SqlConfig, SqlConnectionConfig
from loom.core.sql.service import SqlQueryService
from tests.integration.ai.conftest import make_mcp_capability, make_sql_capability

_AGENT = "triage"
_CALLER = Identity(subject="ada", roles=("analyst",), mechanism="test")


class FakeSession:
    """A double of ``SharedMcpSession`` recording every call it served."""

    def __init__(
        self,
        *,
        tools: tuple[McpToolInfo, ...],
        results: Mapping[str, McpToolCallResult] | None = None,
    ) -> None:
        self._tools = tools
        self._results = dict(results or {})
        self.calls: list[str] = []

    async def list_tools(self) -> tuple[McpToolInfo, ...]:
        return self._tools

    async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> McpToolCallResult:
        self.calls.append(name)
        return self._results.get(name, McpToolCallResult(ok=True, structured=None))


class _Severity(msgspec.Struct, frozen=True):
    level: int


def _view(
    *,
    capability: CompiledMcpCapability,
    session: FakeSession,
    catalogue: tuple[McpToolInfo, ...],
) -> McpGrantView:
    return McpGrantView(
        agent=_AGENT,
        capability=capability,
        session=session,  # type: ignore[arg-type]
        catalogue=catalogue,
        timeout_s=1.0,
        identity=_CALLER,
        observability=None,
    )


class TestElFiltroDelPermisoMcp:
    """El filtro es el mismo include/exclude del artefacto, sin tocar la red."""

    async def test_una_tool_excluida_se_rechaza_sin_llamar_a_la_sesion(self) -> None:
        capability = make_mcp_capability(exclude=("write_*",))
        catalogue = (
            McpToolInfo(name="read_orders", has_output_schema=True),
            McpToolInfo(name="write_orders", has_output_schema=True),
        )
        session = FakeSession(tools=catalogue)
        view = _view(capability=capability, session=session, catalogue=catalogue)

        with pytest.raises(AgentRunError) as excinfo:
            await view.call_untyped("write_orders", {})

        assert excinfo.value.code is AgentRunErrorCode.TOOL_UNKNOWN
        assert session.calls == []

    async def test_el_mensaje_de_tool_fuera_del_permiso_es_el_que_muestra_use_case_dsl_md(
        self,
    ) -> None:
        """Pins the exact message ``docs/rest/use-case-dsl.md`` quotes for ``TOOL_UNKNOWN``.

        Edit one without the other and this test is the gap the next review
        catches.
        """
        capability = make_mcp_capability(server="runbooks", include=("search_incident",))
        catalogue = (
            McpToolInfo(name="search_incident", has_output_schema=True),
            McpToolInfo(name="delete_incident", has_output_schema=True),
        )
        session = FakeSession(tools=catalogue)
        view = _view(capability=capability, session=session, catalogue=catalogue)

        with pytest.raises(AgentRunError) as excinfo:
            await view.call_untyped("delete_incident", {})

        assert excinfo.value.code is AgentRunErrorCode.TOOL_UNKNOWN
        assert str(excinfo.value) == (
            "mcp server 'runbooks' grants no tool named 'delete_incident'; "
            "tools this grant admits: search_incident"
        )

    def test_tools_solo_lista_lo_que_el_filtro_admite(self) -> None:
        capability = make_mcp_capability(include=("read_*",))
        catalogue = (
            McpToolInfo(name="read_orders", has_output_schema=True),
            McpToolInfo(name="write_orders", has_output_schema=True),
        )
        view = _view(
            capability=capability, session=FakeSession(tools=catalogue), catalogue=catalogue
        )

        assert view.tools() == ("read_orders",)


class TestLlamadaTipada:
    """``call`` decodifica el contenido estructurado en ``expect``."""

    async def test_decodifica_el_contenido_estructurado(self) -> None:
        capability = make_mcp_capability()
        catalogue = (McpToolInfo(name="severity", has_output_schema=True),)
        session = FakeSession(
            tools=catalogue,
            results={"severity": McpToolCallResult(ok=True, structured={"level": 4})},
        )
        view = _view(capability=capability, session=session, catalogue=catalogue)

        result = await view.call("severity", {}, expect=_Severity)

        assert result == _Severity(level=4)

    async def test_una_tool_sin_esquema_se_rechaza_antes_de_la_red(self) -> None:
        capability = make_mcp_capability()
        catalogue = (McpToolInfo(name="untyped_tool", has_output_schema=False),)
        session = FakeSession(tools=catalogue)
        view = _view(capability=capability, session=session, catalogue=catalogue)

        with pytest.raises(AgentRunError) as excinfo:
            await view.call("untyped_tool", {}, expect=_Severity)

        assert excinfo.value.code is AgentRunErrorCode.TOOL_UNTYPED
        assert session.calls == []

    async def test_el_mensaje_de_tool_sin_forma_es_el_que_muestra_use_case_dsl_md(self) -> None:
        """Pins the exact message ``docs/rest/use-case-dsl.md`` quotes for ``TOOL_UNTYPED``."""
        capability = make_mcp_capability(server="runbooks")
        catalogue = (McpToolInfo(name="legacy_lookup", has_output_schema=False),)
        session = FakeSession(tools=catalogue)
        view = _view(capability=capability, session=session, catalogue=catalogue)

        with pytest.raises(AgentRunError) as excinfo:
            await view.call("legacy_lookup", {}, expect=_Severity)

        assert excinfo.value.code is AgentRunErrorCode.TOOL_UNTYPED
        assert str(excinfo.value) == (
            "tool 'legacy_lookup' of mcp server 'runbooks' publishes no output "
            "schema; call it with call_untyped() instead"
        )

    async def test_sin_contenido_estructurado_se_rechaza_como_no_estructurado(self) -> None:
        capability = make_mcp_capability()
        catalogue = (McpToolInfo(name="severity", has_output_schema=True),)
        session = FakeSession(
            tools=catalogue, results={"severity": McpToolCallResult(ok=True, structured=None)}
        )
        view = _view(capability=capability, session=session, catalogue=catalogue)

        with pytest.raises(AgentRunError) as excinfo:
            await view.call("severity", {}, expect=_Severity)

        assert excinfo.value.code is AgentRunErrorCode.TOOL_RESULT_UNSTRUCTURED

    async def test_un_flag_de_error_se_reporta_antes_de_intentar_decodificar(self) -> None:
        capability = make_mcp_capability()
        catalogue = (McpToolInfo(name="severity", has_output_schema=True),)
        session = FakeSession(
            tools=catalogue,
            # A payload that would fail to decode into '_Severity' too, so a
            # wrong precedence would report TOOL_DECODE_FAILED instead.
            results={"severity": McpToolCallResult(ok=False, structured={"level": "not-an-int"})},
        )
        view = _view(capability=capability, session=session, catalogue=catalogue)

        with pytest.raises(AgentRunError) as excinfo:
            await view.call("severity", {}, expect=_Severity)

        assert excinfo.value.code is AgentRunErrorCode.TOOL_CALL_FAILED

    async def test_un_contenido_que_no_encaja_falla_por_decodificacion(self) -> None:
        capability = make_mcp_capability()
        catalogue = (McpToolInfo(name="severity", has_output_schema=True),)
        session = FakeSession(
            tools=catalogue,
            results={"severity": McpToolCallResult(ok=True, structured={"level": "not-an-int"})},
        )
        view = _view(capability=capability, session=session, catalogue=catalogue)

        with pytest.raises(AgentRunError) as excinfo:
            await view.call("severity", {}, expect=_Severity)

        assert excinfo.value.code is AgentRunErrorCode.TOOL_DECODE_FAILED
        assert "level" in str(excinfo.value)


class TestLlamadaSinTipar:
    async def test_devuelve_el_json_del_servidor_sin_exigir_esquema(self) -> None:
        capability = make_mcp_capability()
        catalogue = (McpToolInfo(name="raw_tool", has_output_schema=False),)
        session = FakeSession(
            tools=catalogue,
            results={"raw_tool": McpToolCallResult(ok=True, structured={"anything": True})},
        )
        view = _view(capability=capability, session=session, catalogue=catalogue)

        result = await view.call_untyped("raw_tool", {})

        assert result == {"anything": True}

    async def test_sin_contenido_estructurado_devuelve_un_mapa_vacio(self) -> None:
        """La ausencia legítima de contenido — no una contradicción — se pliega a ``{}``."""
        capability = make_mcp_capability()
        catalogue = (McpToolInfo(name="raw_tool", has_output_schema=False),)
        session = FakeSession(
            tools=catalogue,
            results={"raw_tool": McpToolCallResult(ok=True, structured=None)},
        )
        view = _view(capability=capability, session=session, catalogue=catalogue)

        result = await view.call_untyped("raw_tool", {})

        assert result == {}

    async def test_un_contenido_no_mapa_se_rechaza_en_vez_de_descartarse(self) -> None:
        """Una lista o un escalar contradicen el protocolo; no se pliegan a ``{}`` en silencio."""
        capability = make_mcp_capability()
        catalogue = (McpToolInfo(name="raw_tool", has_output_schema=False),)
        session = FakeSession(
            tools=catalogue,
            results={"raw_tool": McpToolCallResult(ok=True, structured=["not", "a", "mapping"])},
        )
        view = _view(capability=capability, session=session, catalogue=catalogue)

        with pytest.raises(AgentRunError) as excinfo:
            await view.call_untyped("raw_tool", {})

        assert excinfo.value.code is AgentRunErrorCode.TOOL_RESULT_UNSTRUCTURED
        assert "list" in str(excinfo.value)


def _sql_view(
    *, capability: CompiledSqlCapability, identity: Identity, service: SqlQueryService
) -> SqlGrantView:
    return SqlGrantView(
        agent=_AGENT,
        capability=capability,
        sql_query_service=service,
        identity=identity,
        observability=None,
    )


class FakeSqlQueryService(SqlQueryService):
    """A double of ``SqlQueryService`` returning a fixed result and recording roles."""

    def __init__(self, result: SqlQueryResult) -> None:
        super().__init__(executors={}, config=SqlConfig(connections={}))
        self._result = result
        self.roles: tuple[str, ...] | None = None

    async def execute(
        self,
        sql: str,
        *,
        connection: str,
        roles: Sequence[str] | None = None,
        parameters: Mapping[str, Any] | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> SqlQueryResult:
        del sql, connection, parameters, limit, offset
        self.roles = tuple(roles) if roles is not None else None
        return self._result


def _result(*rows: tuple[Any, ...]) -> SqlQueryResult:
    return SqlQueryResult(
        columns=(SqlColumn(name="id", type="Int64"), SqlColumn(name="name", type="String")),
        rows=rows,
        row_count=len(rows),
        limit=1000,
        offset=0,
        has_more=False,
        elapsed_ms=1.0,
    )


class TestRolesDelPermisoSql:
    """Los roles vienen del llamante verificado, nunca de un argumento."""

    async def test_corre_con_los_roles_del_llamante_bajo_el_allowlist(self) -> None:
        capability = make_sql_capability()
        capability = msgspec.structs.replace(
            capability,
            config=SqlConnectionConfig(
                backend="clickhouse",
                url=capability.config.url,
                allowed_roles=("analyst", "auditor"),
                readonly=True,
            ),
        )
        service = FakeSqlQueryService(_result())
        view = _sql_view(capability=capability, identity=_CALLER, service=service)

        await view.query("select 1")

        assert service.roles == ("analyst",)

    async def test_un_llamante_anonimo_se_rechaza_antes_de_consultar(self) -> None:
        capability = make_sql_capability()
        capability = msgspec.structs.replace(
            capability,
            config=SqlConnectionConfig(
                backend="clickhouse",
                url=capability.config.url,
                allowed_roles=("analyst",),
                readonly=True,
            ),
        )
        service = FakeSqlQueryService(_result())
        view = _sql_view(capability=capability, identity=ANONYMOUS, service=service)

        with pytest.raises(AgentRunError) as excinfo:
            await view.query("select 1")

        assert excinfo.value.code is AgentRunErrorCode.UNAUTHORIZED
        assert service.roles is None


class TestCotasDelResultadoSql:
    """La cota de bytes trunca en vez de rechazar."""

    async def test_recorta_filas_finales_para_no_superar_max_result_bytes(self) -> None:
        capability = make_sql_capability()
        capability = msgspec.structs.replace(
            capability,
            config=SqlConnectionConfig(
                backend="clickhouse",
                url=capability.config.url,
                allowed_roles=("analyst",),
                readonly=True,
            ),
            max_result_bytes=80,
        )
        rows = tuple((i, f"row-{i}") for i in range(20))
        service = FakeSqlQueryService(_result(*rows))
        view = _sql_view(capability=capability, identity=_CALLER, service=service)

        result = await view.query("select * from t")

        assert len(result) < len(rows)
        assert len(msgspec.json.encode(list(result))) <= capability.max_result_bytes

    @pytest.mark.parametrize(
        ("max_result_bytes", "expected_rows"),
        [
            (20, 0),  # one byte short of the first row's own encoded array
            (21, 1),  # exactly the first row's own encoded array
            (40, 1),  # one byte short of both rows plus their separator
            (41, 2),  # exactly both rows plus their separator
        ],
    )
    async def test_el_corte_cae_exactamente_en_el_limite_de_bytes(
        self, max_result_bytes: int, expected_rows: int
    ) -> None:
        """Pins the exact boundary: the encoded array overhead and the row
        separator are counted once each, not approximated.
        """
        capability = make_sql_capability()
        capability = msgspec.structs.replace(
            capability,
            config=SqlConnectionConfig(
                backend="clickhouse",
                url=capability.config.url,
                allowed_roles=("analyst",),
                readonly=True,
            ),
            max_result_bytes=max_result_bytes,
        )
        rows = ((1, "a"), (2, "b"))
        service = FakeSqlQueryService(_result(*rows))
        view = _sql_view(capability=capability, identity=_CALLER, service=service)

        result = await view.query("select * from t")

        assert len(result) == expected_rows
        assert len(msgspec.json.encode(list(result))) <= max_result_bytes
