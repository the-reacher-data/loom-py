"""Route-aware table discovery adapter."""

from __future__ import annotations

from loom.etl.schema._schema import ColumnSchema
from loom.etl.schema._table import TableRef
from loom.etl.storage._io import TableDiscovery
from loom.etl.storage.route.model import CatalogTarget
from loom.etl.storage.route.resolver import TableRouteResolver


class RoutedCatalog:
    """Dispatch catalog calls to path or catalog discovery based on route."""

    def __init__(
        self,
        resolver: TableRouteResolver,
        *,
        catalog: TableDiscovery,
        path: TableDiscovery | None = None,
    ) -> None:
        self._resolver = resolver
        self._catalog = catalog
        self._path = path

    def exists(self, ref: TableRef) -> bool:
        """Return whether *ref* exists in its resolved backend."""
        target = self._resolver.resolve(ref)
        if isinstance(target, CatalogTarget):
            return self._catalog.exists(target.catalog_ref)
        self._require_path_catalog(ref)
        return self._path.exists(ref)  # type: ignore[union-attr]

    def columns(self, ref: TableRef) -> tuple[str, ...]:
        """Return resolved table column names, or empty when unknown."""
        target = self._resolver.resolve(ref)
        if isinstance(target, CatalogTarget):
            return self._catalog.columns(target.catalog_ref)
        self._require_path_catalog(ref)
        return self._path.columns(ref)  # type: ignore[union-attr]

    def schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        """Return resolved table schema, or ``None`` when table is absent."""
        target = self._resolver.resolve(ref)
        if isinstance(target, CatalogTarget):
            return self._catalog.schema(target.catalog_ref)
        self._require_path_catalog(ref)
        return self._path.schema(ref)  # type: ignore[union-attr]

    def update_schema(self, ref: TableRef, schema: tuple[ColumnSchema, ...]) -> None:
        """Persist schema update in the resolved backend."""
        target = self._resolver.resolve(ref)
        if isinstance(target, CatalogTarget):
            self._catalog.update_schema(target.catalog_ref, schema)
            return
        self._require_path_catalog(ref)
        self._path.update_schema(ref, schema)  # type: ignore[union-attr]

    def _require_path_catalog(self, ref: TableRef) -> None:
        if self._path is None:
            raise RuntimeError(
                f"Table {ref.ref!r} resolved to path mode but no path catalog is configured."
            )
