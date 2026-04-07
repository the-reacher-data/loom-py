"""Build route resolvers from ``StorageConfig``."""

from __future__ import annotations

from dataclasses import dataclass

from loom.etl.schema._table import TableRef
from loom.etl.storage._config import StorageConfig
from loom.etl.storage._locator import PrefixLocator
from loom.etl.storage.route.model import ResolvedTarget
from loom.etl.storage.route.resolver import (
    CatalogRouteResolver,
    CompositeRouteResolver,
    FixedCatalogRouteResolver,
    FixedPathRouteResolver,
    PathRouteResolver,
    TableRouteResolver,
)


@dataclass(frozen=True)
class _MissingRouteResolver:
    """Resolver that always raises for unconfigured logical refs."""

    def resolve(self, logical_ref: TableRef) -> ResolvedTarget:
        raise KeyError(
            f"No storage route configured for logical table {logical_ref.ref!r}. "
            "Define storage.defaults.table_path or add an explicit storage.tables entry."
        )


def build_table_resolver(config: StorageConfig) -> TableRouteResolver:
    """Build composite table resolver from unified storage config."""
    default_resolver = _default_resolver(config)
    overrides: dict[str, TableRouteResolver] = {}
    for route in config.tables:
        if route.path is not None:
            overrides[route.name] = FixedPathRouteResolver(route.path.to_location())
        elif route.ref.strip():
            catalog_ref = _qualify_route_ref(route.ref, route.catalog)
            overrides[route.name] = FixedCatalogRouteResolver(TableRef(catalog_ref))

    if not overrides:
        return default_resolver
    return CompositeRouteResolver(default=default_resolver, overrides=overrides)


def _default_resolver(config: StorageConfig) -> TableRouteResolver:
    default_path = config.defaults.table_path
    if default_path is not None:
        return PathRouteResolver(
            PrefixLocator(
                root=default_path.uri,
                storage_options=default_path.storage_options or None,
                writer=default_path.writer or None,
                delta_config=default_path.delta_config or None,
                commit=default_path.commit or None,
            )
        )
    if config.engine == "spark":
        return CatalogRouteResolver(default_catalog=_default_catalog_key(config))
    return _MissingRouteResolver()


def _default_catalog_key(config: StorageConfig) -> str:
    return "default" if "default" in config.catalogs else ""


def _qualify_route_ref(ref: str, catalog_key: str) -> str:
    if not catalog_key:
        return ref
    parts = ref.split(".")
    if len(parts) == 2:
        return f"{catalog_key}.{ref}"
    return ref
