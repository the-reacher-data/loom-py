"""Route resolvers for logical table references."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from loom.etl.schema._table import TableRef
from loom.etl.storage._locator import TableLocation, TableLocator
from loom.etl.storage.route.model import CatalogTarget, PathTarget, ResolvedTarget


@runtime_checkable
class TableRouteResolver(Protocol):
    """Resolve a logical table reference to a runtime target."""

    def resolve(self, logical_ref: TableRef) -> ResolvedTarget:
        """Resolve *logical_ref* to a catalog or path target."""
        ...


class CatalogRouteResolver:
    """Resolve all tables as catalog references.

    Args:
        default_catalog: Optional default catalog prefix for 2-part refs.
    """

    def __init__(self, default_catalog: str = "") -> None:
        self._default_catalog = default_catalog.strip()

    def resolve(self, logical_ref: TableRef) -> CatalogTarget:
        """Resolve *logical_ref* to a catalog target."""
        return CatalogTarget(
            logical_ref=logical_ref,
            catalog_ref=_qualify_catalog_ref(logical_ref, self._default_catalog),
        )


class PathRouteResolver:
    """Resolve all tables via a :class:`TableLocator`."""

    def __init__(self, locator: TableLocator) -> None:
        self._locator = locator

    def resolve(self, logical_ref: TableRef) -> PathTarget:
        """Resolve *logical_ref* to a path target."""
        return PathTarget(logical_ref=logical_ref, location=self._locator.locate(logical_ref))


class FixedCatalogRouteResolver:
    """Resolve one logical table to one explicit catalog reference."""

    def __init__(self, catalog_ref: TableRef) -> None:
        self._catalog_ref = catalog_ref

    def resolve(self, logical_ref: TableRef) -> CatalogTarget:
        """Resolve *logical_ref* to the configured catalog table."""
        return CatalogTarget(logical_ref=logical_ref, catalog_ref=self._catalog_ref)


class FixedPathRouteResolver:
    """Resolve one logical table to one explicit physical location."""

    def __init__(self, location: TableLocation) -> None:
        self._location = location

    def resolve(self, logical_ref: TableRef) -> PathTarget:
        """Resolve *logical_ref* to the configured physical path."""
        return PathTarget(logical_ref=logical_ref, location=self._location)


class CompositeRouteResolver:
    """Resolve using per-table overrides over a default resolver.

    Args:
        default: Resolver used when no table-specific override exists.
        overrides: Optional mapping ``logical_ref -> resolver``.
    """

    def __init__(
        self,
        *,
        default: TableRouteResolver,
        overrides: dict[str, TableRouteResolver] | None = None,
    ) -> None:
        self._default = default
        self._overrides = overrides or {}

    def resolve(self, logical_ref: TableRef) -> ResolvedTarget:
        """Resolve *logical_ref* using override first, then default."""
        resolver = self._overrides.get(logical_ref.ref, self._default)
        return resolver.resolve(logical_ref)


def _qualify_catalog_ref(ref: TableRef, default_catalog: str) -> TableRef:
    """Return a catalog-qualified table ref when needed."""
    parts = ref.ref.split(".")
    if len(parts) == 2 and default_catalog:
        return TableRef(f"{default_catalog}.{ref.ref}")
    return ref
