"""Persistence backend plugins: contract, registry and the ``none`` backend.

This package imports no concrete backend; SQLAlchemy, DynamoDB and the rest
live under ``loom.core.repository`` and are reached only through their entry
points.
"""

from loom.core.persistence.abc import PersistenceBackend, PersistenceWiring
from loom.core.persistence.none import NoneBackend
from loom.core.persistence.registry import BACKEND_ENTRY_POINT_GROUP, resolve_backend

__all__ = [
    "BACKEND_ENTRY_POINT_GROUP",
    "NoneBackend",
    "PersistenceBackend",
    "PersistenceWiring",
    "resolve_backend",
]
