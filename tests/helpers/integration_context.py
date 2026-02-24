from __future__ import annotations

import importlib
import pkgutil
from collections.abc import Mapping
from dataclasses import dataclass
from types import ModuleType
from typing import Any

import msgspec

from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager

ScenarioDict = dict[str, list[msgspec.Struct]]


@dataclass(slots=True)
class EntityContext:
    repository: Any
    service: Any | None = None


class IntegrationContext:
    def __init__(
        self,
        *,
        session_manager: SessionManager,
        entities: Mapping[str, EntityContext],
        load_order: tuple[str, ...] = (),
    ) -> None:
        self.session_manager = session_manager
        self._entities: dict[str, EntityContext] = dict(entities)
        self._load_order = load_order
        for entity_name, context in self._entities.items():
            setattr(self, entity_name, context)

    def __getattr__(self, name: str) -> EntityContext:
        entity = self._entities.get(name)
        if entity is None:
            raise AttributeError(name)
        return entity

    async def load(self, scenario: ScenarioDict) -> None:
        execution_order: list[str] = [name for name in self._load_order if name in scenario]
        execution_order.extend(name for name in scenario if name not in execution_order)

        for entity_name in execution_order:
            entity_context = self._entities.get(entity_name)
            if entity_context is None:
                raise ValueError(f"Entity '{entity_name}' is not registered in IntegrationContext")
            for contract in scenario.get(entity_name, []):
                await entity_context.repository.create(contract)


def build_integration_context(
    *,
    package_name: str,
    session_manager: SessionManager,
    load_order: tuple[str, ...] = (),
) -> IntegrationContext:
    repository_classes = _discover_repository_classes(package_name)
    repositories = {
        entity_name: repository_class(session_manager=session_manager)
        for entity_name, repository_class in repository_classes.items()
    }
    _wire_repository_dependencies(repositories)
    entities = {
        entity_name: EntityContext(repository=repository)
        for entity_name, repository in repositories.items()
    }
    return IntegrationContext(
        session_manager=session_manager,
        entities=entities,
        load_order=load_order,
    )


def _discover_repository_classes(
    package_name: str,
) -> dict[str, type[RepositorySQLAlchemy[Any, Any]]]:
    package = importlib.import_module(package_name)
    modules = _walk_modules(package)
    discovered: dict[str, type[RepositorySQLAlchemy[Any, Any]]] = {}

    for module in modules:
        for obj in vars(module).values():
            if not isinstance(obj, type):
                continue
            if getattr(obj, "__module__", None) != module.__name__:
                continue
            if obj is RepositorySQLAlchemy:
                continue
            if issubclass(obj, RepositorySQLAlchemy):
                context_key = getattr(obj, "context_key", None)
                if isinstance(context_key, str) and context_key:
                    entity_name = context_key
                elif module.__name__.endswith(".repository"):
                    entity_name = module.__name__.split(".")[-2]
                else:
                    continue
                discovered[entity_name] = obj
                break
    return discovered


def _walk_modules(package: ModuleType) -> list[ModuleType]:
    modules: list[ModuleType] = [package]
    if not hasattr(package, "__path__"):
        return modules

    for module_info in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
        modules.append(importlib.import_module(module_info.name))
    return modules


def _wire_repository_dependencies(repositories: Mapping[str, Any]) -> None:
    for repository in repositories.values():
        annotations = getattr(type(repository), "__annotations__", {})
        for attr_name in annotations:
            if not attr_name.endswith("_repo"):
                continue
            entity_name = attr_name[:-5]
            dependency = repositories.get(entity_name)
            if dependency is not None:
                setattr(repository, attr_name, dependency)
