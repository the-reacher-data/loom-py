import importlib
from typing import TYPE_CHECKING, Any

from loom.testing.agents import ContractScenario, FakeAgentEngine, agent_engine_contract_suite
from loom.testing.golden import GoldenHarness, serialize_plan
from loom.testing.in_memory import InMemoryRepository
from loom.testing.runner import UseCaseTest

if TYPE_CHECKING:
    from loom.testing.http_harness import HttpTestHarness
    from loom.testing.repository_harness import (
        RepositoryIntegrationHarness,
        ScenarioDict,
        build_repository_harness,
    )

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

_REPOSITORY_HARNESS_EXPORTS = frozenset(
    {"RepositoryIntegrationHarness", "ScenarioDict", "build_repository_harness"}
)


def __getattr__(name: str) -> Any:
    """Resolve exports whose dependencies belong to optional extras on first access."""
    if name == "HttpTestHarness":
        return _http_test_harness()
    if name in _REPOSITORY_HARNESS_EXPORTS:
        return _repository_harness_export(name)
    raise AttributeError(name)


def _http_test_harness() -> Any:
    """Import ``HttpTestHarness``, naming the ``rest`` extra when its dependencies are missing."""
    try:
        from loom.testing.http_harness import HttpTestHarness as _HttpTestHarness
    except ImportError as exc:  # pragma: no cover - exercised only when REST deps are absent
        raise ImportError(
            "HttpTestHarness requires the 'rest' extra (fastapi, pydantic, uvicorn). "
            "Install loom-kernel with the 'rest' extra to use HTTP test harnesses."
        ) from exc
    return _HttpTestHarness


def _repository_harness_export(name: str) -> Any:
    """Import one ``repository_harness`` export, naming the ``sqlalchemy`` extra when missing."""
    try:
        repository_harness = importlib.import_module("loom.testing.repository_harness")
    except ImportError as exc:
        raise ImportError(
            f"{name} requires SQLAlchemy. "
            "Install loom-kernel[sqlalchemy] to use repository integration harnesses."
        ) from exc
    return getattr(repository_harness, name)
