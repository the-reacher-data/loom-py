"""Resolved table targets used by runtime readers/writers."""

from __future__ import annotations

from dataclasses import dataclass

from loom.etl.schema._table import TableRef
from loom.etl.storage._locator import TableLocation


@dataclass(frozen=True)
class CatalogTarget:
    """Resolved catalog destination for one logical table."""

    logical_ref: TableRef
    catalog_ref: TableRef


@dataclass(frozen=True)
class PathTarget:
    """Resolved physical destination for one logical table."""

    logical_ref: TableRef
    location: TableLocation


ResolvedTarget = CatalogTarget | PathTarget
