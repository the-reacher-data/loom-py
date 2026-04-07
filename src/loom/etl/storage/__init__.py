"""Storage config, location mapping, and runtime I/O protocols."""

from __future__ import annotations

from ._config import (
    CatalogConnection,
    FilePathConfig,
    FileRoute,
    StorageConfig,
    StorageDefaults,
    StorageEngine,
    TablePathConfig,
    TableRoute,
)
from ._io import SourceReader, TableDiscovery, TargetWriter
from ._locator import MappingLocator, PrefixLocator, TableLocation, TableLocator

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
]
