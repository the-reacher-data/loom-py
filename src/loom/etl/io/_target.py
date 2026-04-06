"""Backward-compatibility shim — canonical location is :mod:`loom.etl.io.target`.

This module is kept for reload-contract tests.  New code should import from
:mod:`loom.etl.io.target` or :mod:`loom.etl.io` directly.
"""

from loom.etl.io._format import Format  # noqa: F401 — preserved for attribute access compat
from loom.etl.io.target import IntoFile, IntoTable, IntoTemp, SchemaMode

__all__ = [
    "IntoTable",
    "IntoFile",
    "IntoTemp",
    "SchemaMode",
]
