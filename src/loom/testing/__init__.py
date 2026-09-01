from typing import TYPE_CHECKING, Any

from loom.testing.agents import ContractScenario, FakeAgentEngine, agent_engine_contract_suite
from loom.testing.golden import GoldenHarness, serialize_plan
from loom.testing.in_memory import InMemoryRepository
from loom.testing.repository_harness import (
    RepositoryIntegrationHarness,
    ScenarioDict,
    build_repository_harness,
)
from loom.testing.runner import UseCaseTest

if TYPE_CHECKING:
    from loom.testing.http_harness import HttpTestHarness

__all__ = [
    "ContractScenario",
    "FakeAgentEngine",
    "GoldenHarness",
    "HttpTestHarness",
    "InMemoryRepository",
    "RepositoryIntegrationHarness",
    "ScenarioDict",
    "UseCaseTest",
    "agent_engine_contract_suite",
    "build_repository_harness",
    "serialize_plan",
]


def __getattr__(name: str) -> Any:
    if name != "HttpTestHarness":
        raise AttributeError(name)
    try:
        from loom.testing.http_harness import HttpTestHarness as _HttpTestHarness
    except ImportError as exc:  # pragma: no cover - exercised only when REST deps are absent
        raise ImportError(
            "HttpTestHarness requires the 'rest' extra (fastapi, pydantic, uvicorn). "
            "Install loom-kernel with the 'rest' extra to use HTTP test harnesses."
        ) from exc
    return _HttpTestHarness
