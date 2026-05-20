"""Public table sink package for streaming nodes.

The user-facing API stays on ``loom.streaming.nodes._table`` while the
implementation details live in :mod:`loom.streaming.nodes._table.common`.
"""

from loom.streaming.nodes._table.common import (
    Backend,
    DeltaSinkConfig,
    IntoTable,
    SqlAlchemyDatabaseConfig,
    SqlAlchemySinkConfig,
)

__all__ = [
    "Backend",
    "DeltaSinkConfig",
    "IntoTable",
    "SqlAlchemyDatabaseConfig",
    "SqlAlchemySinkConfig",
]
