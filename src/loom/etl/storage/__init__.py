"""Storage configs, location mapping, and runtime I/O protocols."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS: dict[str, str] = {
    "StorageBackend": "loom.etl.storage._config",
    "StorageConfig": "loom.etl.storage._config",
    "DeltaConfig": "loom.etl.storage._config",
    "UnityCatalogConfig": "loom.etl.storage._config",
    "TableDiscovery": "loom.etl.storage._io",
    "SourceReader": "loom.etl.storage._io",
    "TargetWriter": "loom.etl.storage._io",
    "TableLocation": "loom.etl.storage._locator",
    "TableLocator": "loom.etl.storage._locator",
    "PrefixLocator": "loom.etl.storage._locator",
    "MappingLocator": "loom.etl.storage._locator",
    "ObservabilityConfig": "loom.etl.storage._observability",
    "RunSinkConfig": "loom.etl.storage._observability",
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    module_path = _EXPORTS.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_path)
    value = getattr(module, name)
    globals()[name] = value
    return value
