from __future__ import annotations

import sys
import types

import pytest

from loom.core.discovery.manifest import ManifestDiscoveryEngine
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.sqlalchemy import RepositorySQLAlchemy


class _ManifestProduct(BaseModel):
    __tablename__ = "manifest_products"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=120)


class _ManifestProductRepository(RepositorySQLAlchemy[_ManifestProduct, int]):
    pass


def test_manifest_discovery_accepts_repositories_list() -> None:
    module_name = "tests.unit.core.discovery._manifest_with_repositories"
    module = types.ModuleType(module_name)
    module.REPOSITORIES = [_ManifestProductRepository]
    sys.modules[module_name] = module

    try:
        result = ManifestDiscoveryEngine(module_name).discover()
    finally:
        sys.modules.pop(module_name, None)

    assert result.models == ()
    assert result.use_cases == ()
    assert result.interfaces == ()


def test_manifest_discovery_rejects_invalid_repository_type() -> None:
    module_name = "tests.unit.core.discovery._manifest_with_invalid_repository"
    module = types.ModuleType(module_name)
    module.REPOSITORIES = [object]
    sys.modules[module_name] = module

    try:
        engine = ManifestDiscoveryEngine(module_name)
        with pytest.raises(TypeError, match="must implement the Repository protocol"):
            engine.discover()
    finally:
        sys.modules.pop(module_name, None)


def test_manifest_discovery_accepts_agents_only_manifest() -> None:
    """A manifest that exposes only agent artifacts is a complete manifest."""
    module_name = "tests.unit.core.discovery._manifest_with_agents"
    module = types.ModuleType(module_name)
    module.AGENTS = ["ai/agents/*/agent.yaml"]
    sys.modules[module_name] = module

    try:
        result = ManifestDiscoveryEngine(module_name).discover()
    finally:
        sys.modules.pop(module_name, None)

    assert result.agent_specs == ("ai/agents/*/agent.yaml",)


def test_manifest_discovery_rejects_empty_manifest_naming_agents() -> None:
    """The 'no components' message lists AGENTS among the expected attributes."""
    module_name = "tests.unit.core.discovery._manifest_empty"
    module = types.ModuleType(module_name)
    sys.modules[module_name] = module

    try:
        engine = ManifestDiscoveryEngine(module_name)
        with pytest.raises(ValueError, match="AGENTS"):
            engine.discover()
    finally:
        sys.modules.pop(module_name, None)


def test_manifest_discovery_rejects_non_string_agent_spec() -> None:
    """An agent entry is a path or a glob, never a type."""
    module_name = "tests.unit.core.discovery._manifest_with_invalid_agent"
    module = types.ModuleType(module_name)
    module.AGENTS = [object]
    sys.modules[module_name] = module

    try:
        engine = ManifestDiscoveryEngine(module_name)
        with pytest.raises(TypeError):
            engine.discover()
    finally:
        sys.modules.pop(module_name, None)


def test_manifest_discovery_rejects_empty_agent_spec_string() -> None:
    """An empty string matches every artifact or none: it is never a grant."""
    module_name = "tests.unit.core.discovery._manifest_with_empty_agent"
    module = types.ModuleType(module_name)
    module.AGENTS = [""]
    sys.modules[module_name] = module

    try:
        engine = ManifestDiscoveryEngine(module_name)
        with pytest.raises(TypeError):
            engine.discover()
    finally:
        sys.modules.pop(module_name, None)
