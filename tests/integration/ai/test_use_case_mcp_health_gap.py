"""The documented health-probe gap for a use-case-only MCP server (spec 015, T605).

Pins the operational consequence ``docs/ai/mcp.md`` now states in those words:
under ``ai.remote_clients: optional``, a deployment whose only MCP server is
reached through ``Mcp()`` alone, never through a compiled agent's own ``mcp``
capability, boots successfully and reports ``ok`` even when that server never
connected — ``_probe_forever`` walks compiled agent plans only
(``_lifecycle.py``), so a use-case-only server's key never reaches
``AgentHealth.checks``. The outage surfaces only on the first call, as
``TOOL_UNAVAILABLE``.
"""

from __future__ import annotations

import pytest

from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.runtime import AgentRuntime
from loom.ai.runtime._handle import mcp_marker_resolver
from loom.ai.runtime._lifecycle import UseCaseMcpGrant
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from tests.integration.ai.conftest import (
    CountingEngineProvider,
    StubDepsFactory,
    StubMcpClient,
    _until,
    make_ai_config,
    make_mcp_capability,
    make_mcp_servers,
    make_plan,
    mcp_client_factory,
)

_AGENT = "triage"
_SERVER = "alpha-tools"


class TestUnServidorSoloDeCasoDeUsoQuedaFueraDeLaSonda:
    """T605: the probe reports what an agent declared, not what a use case reached."""

    async def test_el_arranque_reporta_ok_y_el_servidor_no_aparece_en_los_checks(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        client = StubMcpClient(label="a", session=None, log=lifecycle_log, connect_error="refused")
        grant = UseCaseMcpGrant(
            capability=make_mcp_capability(_SERVER, include=("search",)),
            usecase="orders.get_order_status",
            parameter="gateway",
        )
        runtime = AgentRuntime(
            plans=[make_plan(_AGENT)],  # declares no 'mcp' capability at all
            config=make_ai_config(
                mcp_servers=make_mcp_servers(_SERVER),
                remote_clients="optional",
                health_cache_ttl_ms=5,
            ),
            engine_provider=CountingEngineProvider(),  # type: ignore[arg-type]
            deps=deps,
            container=container,
            mcp_client_factory=mcp_client_factory({_SERVER: client}),  # type: ignore[arg-type]
            use_case_mcp=(grant,),
        )

        async with runtime:
            await _until(lambda: runtime._health.get(_AGENT) is not None)
            health = await runtime.health(_AGENT)

            assert health.status == "ok"
            assert _SERVER not in health.checks

            resolve = mcp_marker_resolver(runtime, observability=None)
            handle = resolve(_SERVER, ("search",), identity)
            with pytest.raises(AgentRunError) as excinfo:
                await handle.call_untyped("search", {})
            assert excinfo.value.code is AgentRunErrorCode.TOOL_UNAVAILABLE
