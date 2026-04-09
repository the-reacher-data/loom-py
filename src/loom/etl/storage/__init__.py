"""Storage config, location mapping, routing, and runtime protocols."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "StorageEngine",
    "StorageConfig",
    "StorageDefaults",
    "CatalogConnection",
    "TablePathConfig",
    "TableRoute",
    "FilePathConfig",
    "FileRoute",
    "TableDiscovery",
    "SourceReader",
    "TargetWriter",
    "TableLocation",
    "TableLocator",
    "PrefixLocator",
    "MappingLocator",
    "CatalogTarget",
    "PathTarget",
    "ResolvedTarget",
    "TableRouteResolver",
    "CatalogRouteResolver",
    "PathRouteResolver",
    "FixedCatalogRouteResolver",
    "FixedPathRouteResolver",
    "CompositeRouteResolver",
    "RoutedCatalog",
    "build_table_resolver",
]

_EXPORTS: dict[str, str] = {
    # config
    "StorageEngine": "loom.etl.storage._config",
    "StorageConfig": "loom.etl.storage._config",
    "StorageDefaults": "loom.etl.storage._config",
    "CatalogConnection": "loom.etl.storage._config",
    "TablePathConfig": "loom.etl.storage._config",
    "TableRoute": "loom.etl.storage._config",
    "FilePathConfig": "loom.etl.storage._config",
    "FileRoute": "loom.etl.storage._config",
    # protocols
    "TableDiscovery": "loom.etl.storage.protocols",
    "SourceReader": "loom.etl.storage.protocols",
    "TargetWriter": "loom.etl.storage.protocols",
    # locator
    "TableLocation": "loom.etl.storage._locator",
    "TableLocator": "loom.etl.storage._locator",
    "PrefixLocator": "loom.etl.storage._locator",
    "MappingLocator": "loom.etl.storage._locator",
    # routing
    "CatalogTarget": "loom.etl.storage.routing",
    "PathTarget": "loom.etl.storage.routing",
    "ResolvedTarget": "loom.etl.storage.routing",
    "TableRouteResolver": "loom.etl.storage.routing",
    "CatalogRouteResolver": "loom.etl.storage.routing",
    "PathRouteResolver": "loom.etl.storage.routing",
    "FixedCatalogRouteResolver": "loom.etl.storage.routing",
    "FixedPathRouteResolver": "loom.etl.storage.routing",
    "CompositeRouteResolver": "loom.etl.storage.routing",
    "RoutedCatalog": "loom.etl.storage.routing",
    "build_table_resolver": "loom.etl.storage.routing",
}


def __getattr__(name: str) -> Any:
    module_path = _EXPORTS.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_path)
    value = getattr(module, name)
    globals()[name] = value
    return value
