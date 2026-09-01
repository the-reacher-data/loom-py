"""The background health probe fails open (:meth:`AgentRuntime.health`).

``AgentEngine.health`` is a public protocol a third party implements, so it may
raise anything. Before this the probe had no guard: the first failure ended the
task for good, and ``/health`` went on answering the last cached ``ok`` forever
over a runtime whose probe was dead. The probe must instead record the failing
agent as ``unavailable`` and keep going.
"""

from __future__ import annotations

import asyncio

import pytest

from loom.ai.abc import HealthStatus
from loom.ai.runtime import AgentRuntime
from loom.core.di import LoomContainer
from tests.integration.ai.conftest import (
    CountingEngineProvider,
    ScriptedEngine,
    StubDepsFactory,
    make_ai_config,
    make_plan,
)

_FAILING = "brittle"
_HEALTHY = "sturdy"


class ProbeFailingEngine(ScriptedEngine):
    """Engine whose ``health`` raises for the first *failures* probes.

    Models a third-party engine reaching a provider that is briefly down: the
    probe must survive it, and must recover on its own once it stops raising.

    Args:
        failures: Number of consecutive probes that raise before recovering.
    """

    def __init__(self, *, failures: int) -> None:
        super().__init__()
        self.remaining = failures
        self.probes = 0

    async def health(self) -> HealthStatus:
        """Raise while *remaining* probes are owed, then report ``ok``."""
        self.probes += 1
        if self.remaining > 0:
            self.remaining -= 1
            raise RuntimeError("the provider endpoint https://vendor.internal/v1 refused")
        return await super().health()


async def _until(predicate: object, *, timeout: float = 2.0) -> None:
    """Poll *predicate* until it holds, failing the test if it never does."""
    assert callable(predicate)
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("the probe never reached the expected state")


@pytest.fixture
def container() -> LoomContainer:
    """Empty application container: no capability is exercised here."""
    return LoomContainer()


def _runtime(engines: dict[str, ScriptedEngine], container: LoomContainer) -> AgentRuntime:
    """Build a runtime over two agents and no live client."""
    return AgentRuntime(
        plans=[make_plan(name) for name in engines],
        config=make_ai_config(health_cache_ttl_ms=5),
        engine_provider=CountingEngineProvider(engines),  # type: ignore[arg-type]
        deps=StubDepsFactory(),  # type: ignore[arg-type]
        container=container,
    )


class TestSondaAPruebaDeFallos:
    """One agent's failing probe must not silence the whole runtime."""

    async def test_marca_el_agente_como_no_disponible_cuando_su_sonda_falla(
        self, container: LoomContainer
    ) -> None:
        """A raising ``health`` becomes a health state, not a dead task."""
        engines: dict[str, ScriptedEngine] = {_FAILING: ProbeFailingEngine(failures=1000)}

        async with _runtime(engines, container) as runtime:
            await _until(lambda: runtime._health.get(_FAILING) is not None)
            health = await runtime.health(_FAILING)

        assert health.status == "unavailable"

    async def test_no_muestra_el_texto_del_fallo_cuando_su_sonda_falla(
        self, container: LoomContainer
    ) -> None:
        """The probe reaches a provider: its failure text never reaches a scrape."""
        engines: dict[str, ScriptedEngine] = {_FAILING: ProbeFailingEngine(failures=1000)}

        async with _runtime(engines, container) as runtime:
            await _until(lambda: runtime._health.get(_FAILING) is not None)
            health = await runtime.health(_FAILING)

        assert "vendor.internal" not in (health.detail or "")

    async def test_sigue_sondeando_al_resto_cuando_una_sonda_falla(
        self, container: LoomContainer
    ) -> None:
        """The loop moves on to the next agent instead of ending."""
        engines: dict[str, ScriptedEngine] = {
            _FAILING: ProbeFailingEngine(failures=1000),
            _HEALTHY: ScriptedEngine(),
        }

        async with _runtime(engines, container) as runtime:
            await _until(lambda: runtime._health.get(_HEALTHY) is not None)
            health = await runtime.health(_HEALTHY)

        assert health.status == "ok"

    async def test_se_recupera_cuando_la_sonda_vuelve_a_responder(
        self, container: LoomContainer
    ) -> None:
        """The probe is still alive after the failure: the next pass reports ``ok``."""
        engine = ProbeFailingEngine(failures=1)
        engines: dict[str, ScriptedEngine] = {_FAILING: engine}

        async with _runtime(engines, container) as runtime:
            await _until(lambda: runtime._health.get(_FAILING) is not None)
            await _until(lambda: engine.probes >= 2)
            await _until(lambda: (runtime._health[_FAILING]).status == "ok")
            health = await runtime.health(_FAILING)

        assert health.status == "ok"
