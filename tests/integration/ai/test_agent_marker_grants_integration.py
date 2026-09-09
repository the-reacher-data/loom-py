"""End-to-end coverage of the grant handles and the three run modes (spec 014, PR3).

Drives the whole path a consumer would, exactly as ``test_agent_marker_integration.py``
does for PR2: a real ``UseCaseCompiler`` compiles a use case declaring ``Agent()``, a
real ``RuntimeExecutor`` runs it, and a real ``AgentRuntime`` serves the named agent —
only the model and the MCP/SQL network are faked, at the edge.

Covers points 3, 4 and 5 of the standing integration requirement in
``specs/014-model-as-actor/tasks.md``: the three run modes over one artefact; a
granted tool call filtered by the artefact's own grant, with an excluded tool
refused before any network call; and a granted query run with the caller's own
roles under the grant's bounds.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import msgspec
import pytest

from loom.ai.abc import AgentHandle
from loom.ai.compiler._plan import CompiledCapability
from loom.ai.config import AiConfig, McpServerConfig
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.runtime import AgentRuntime
from loom.ai.runtime._handle import agent_marker_resolver
from loom.core.di import LoomContainer
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.identity import Identity
from loom.core.sql.abc.contracts import SqlColumn, SqlQueryResult
from loom.core.sql.config import SqlConfig, SqlConnectionConfig
from loom.core.sql.service import SqlQueryService
from loom.core.use_case import Agent, UseCase
from tests.integration.ai.conftest import (
    MARKER_AGENT_NAME,
    CountingEngineProvider,
    RecordingMcpSession,
    ShapedRecordingEngine,
    StubDepsFactory,
    StubMcpClient,
    make_ai_config,
    make_mcp_capability,
    make_plan,
    make_sql_capability,
    make_sql_config,
    marker_use_case_executor,
    mcp_client_factory,
)

_ALICE = Identity(subject="alice", roles=("analyst",), mechanism="test")
_MCP_SERVER = "tools"
_SQL_CONNECTION = "reporting"


class ThreeModesUseCase(UseCase[object, dict[str, object]]):
    """One artefact, run through its three modes (T304)."""

    async def execute(
        self,
        triage: AgentHandle[dict[str, Any]] = Agent(MARKER_AGENT_NAME),
    ) -> dict[str, object]:
        declared = await triage.run("assess this")
        overridden = await triage.run("assess this", expect=dict)
        prose = await triage.run_text("assess this")
        return {
            "declared": declared.output,
            "overridden": overridden.output,
            "prose": prose.output,
        }


class GrantedToolUseCase(UseCase[object, dict[str, object]]):
    """Calls a tool the artefact's own grant admits (T301)."""

    async def execute(
        self,
        triage: AgentHandle[dict[str, Any]] = Agent(MARKER_AGENT_NAME),
    ) -> dict[str, object]:
        result = await triage.mcp(_MCP_SERVER).call_untyped("read_orders", {"customer": "acme"})
        return dict(result)


class RejectedToolUseCase(UseCase[object, None]):
    """Calls a tool the artefact's own grant excludes (T301)."""

    async def execute(
        self,
        triage: AgentHandle[dict[str, Any]] = Agent(MARKER_AGENT_NAME),
    ) -> None:
        await triage.mcp(_MCP_SERVER).call_untyped("write_orders", {"customer": "acme"})


class GrantedQueryUseCase(UseCase[object, Sequence[Mapping[str, Any]]]):
    """Queries the artefact's own bounded ``sql`` grant (T303)."""

    async def execute(
        self,
        triage: AgentHandle[dict[str, Any]] = Agent(MARKER_AGENT_NAME),
    ) -> Sequence[Mapping[str, Any]]:
        return await triage.sql(_SQL_CONNECTION).query("select * from orders")


class RecordingSqlQueryService(SqlQueryService):
    """A minimal ``SqlQueryService`` double recording the roles it was called with."""

    def __init__(self, result: SqlQueryResult) -> None:
        super().__init__(executors={}, config=SqlConfig(connections={}))
        self._result = result
        self.calls: list[tuple[str, ...]] = []

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
        self.calls.append(tuple(roles or ()))
        return self._result


def _sql_result() -> SqlQueryResult:
    return SqlQueryResult(
        columns=(SqlColumn(name="id", type="Int64"),),
        rows=((1,), (2,)),
        row_count=2,
        limit=1000,
        offset=0,
        has_more=False,
        elapsed_ms=1.0,
    )


def _granted_mcp_capability() -> CompiledCapability:
    return make_mcp_capability(_MCP_SERVER, exclude=("write_*",))


def _granted_sql_capability() -> CompiledCapability:
    capability = make_sql_capability(_SQL_CONNECTION)
    return msgspec.structs.replace(
        capability,
        config=SqlConnectionConfig(
            backend="clickhouse",
            url=capability.config.url,
            allowed_roles=("analyst",),
            readonly=True,
        ),
    )


def _ai_config(*, granted_mcp: bool) -> AiConfig:
    mcp_servers = (
        {_MCP_SERVER: McpServerConfig(url="https://tools.internal/mcp")} if granted_mcp else {}
    )
    return make_ai_config(mcp_servers=mcp_servers)


def _wired(
    *,
    engine: ShapedRecordingEngine,
    compiler_and_executor: tuple[UseCaseCompiler, RuntimeExecutor],
    deps: StubDepsFactory,
    container: LoomContainer,
    capabilities: Sequence[CompiledCapability] = (),
    mcp_clients: dict[str, StubMcpClient] | None = None,
    granted_sql: bool = False,
) -> tuple[RuntimeExecutor, AgentRuntime]:
    """Wire a real executor to a real ``AgentRuntime``, exactly as ``create_app`` does."""
    _, executor = compiler_and_executor
    provider = CountingEngineProvider(engines={MARKER_AGENT_NAME: engine})
    runtime = AgentRuntime(
        plans=[make_plan(MARKER_AGENT_NAME, capabilities=capabilities)],
        config=_ai_config(granted_mcp=bool(mcp_clients)),
        engine_provider=provider,  # type: ignore[arg-type]
        deps=deps,
        container=container,
        mcp_client_factory=mcp_client_factory(mcp_clients or {}),  # type: ignore[arg-type]
        sql_config=make_sql_config(_SQL_CONNECTION) if granted_sql else None,
    )
    executor.bind_agent_resolver(agent_marker_resolver(runtime, observability=None))
    return executor, runtime


class TestLosTresModosSobreUnMismoArtefacto:
    """Standing requirement 3: la forma declarada, la sobrescrita y el texto abierto."""

    async def test_las_tres_corridas_llegan_al_motor_real(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        engine = ShapedRecordingEngine()
        executor, runtime = _wired(
            engine=engine,
            compiler_and_executor=marker_use_case_executor(ThreeModesUseCase),
            deps=deps,
            container=container,
        )

        async with runtime:
            result = await executor.execute(ThreeModesUseCase(), identity=_ALICE)

        assert result["declared"] == {"answer": "42"}
        assert result["overridden"] == {"answer": "42"}
        assert result["prose"] == {"answer": "42"}
        assert engine.shaped_calls == [dict, str]
        assert len(engine.identities) == 3


class TestUnaLlamadaFiltradaPorElPermiso:
    """Standing requirement 4: el filtro del grant, y el rechazo antes de la red."""

    async def test_una_tool_admitida_llega_al_servidor(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        session = RecordingMcpSession(
            tools=("read_orders", "write_orders"),
            results={"read_orders": {"orders": [1, 2, 3]}},
        )
        engine = ShapedRecordingEngine()
        executor, runtime = _wired(
            engine=engine,
            compiler_and_executor=marker_use_case_executor(GrantedToolUseCase),
            deps=deps,
            container=container,
            capabilities=(_granted_mcp_capability(),),
            mcp_clients={_MCP_SERVER: StubMcpClient(label=_MCP_SERVER, session=session, log=[])},
        )

        async with runtime:
            result = await executor.execute(GrantedToolUseCase(), identity=_ALICE)

        assert result == {"orders": [1, 2, 3]}
        assert session.calls == [("read_orders", {"customer": "acme"})]

    async def test_una_tool_excluida_se_rechaza_sin_llamar_al_servidor(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        session = RecordingMcpSession(tools=("read_orders", "write_orders"))
        engine = ShapedRecordingEngine()
        executor, runtime = _wired(
            engine=engine,
            compiler_and_executor=marker_use_case_executor(RejectedToolUseCase),
            deps=deps,
            container=container,
            capabilities=(_granted_mcp_capability(),),
            mcp_clients={_MCP_SERVER: StubMcpClient(label=_MCP_SERVER, session=session, log=[])},
        )

        async with runtime:
            with pytest.raises(AgentRunError) as excinfo:
                await executor.execute(RejectedToolUseCase(), identity=_ALICE)

        assert excinfo.value.code is AgentRunErrorCode.TOOL_UNKNOWN
        assert session.calls == []


class TestUnaConsultaConLosRolesDelLlamante:
    """Standing requirement 5: la consulta corre con los roles del llamante verificado."""

    async def test_la_consulta_corre_con_el_rol_del_llamante(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        service = RecordingSqlQueryService(_sql_result())
        container.register_instance(SqlQueryService, service)
        engine = ShapedRecordingEngine()
        executor, runtime = _wired(
            engine=engine,
            compiler_and_executor=marker_use_case_executor(GrantedQueryUseCase),
            deps=deps,
            container=container,
            capabilities=(_granted_sql_capability(),),
            granted_sql=True,
        )

        async with runtime:
            rows = await executor.execute(GrantedQueryUseCase(), identity=_ALICE)

        assert list(rows) == [{"id": 1}, {"id": 2}]
        assert service.calls == [("analyst",)]
