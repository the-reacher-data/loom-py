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
        with pytest.raises(TypeError, match="must inherit RepositorySQLAlchemy"):
            ManifestDiscoveryEngine(module_name).discover()
    finally:
        sys.modules.pop(module_name, None)
