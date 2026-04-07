"""Route resolution components for ETL storage."""

from loom.etl.storage.route.build import build_table_resolver
from loom.etl.storage.route.catalog import RoutedCatalog
from loom.etl.storage.route.model import CatalogTarget, PathTarget, ResolvedTarget
from loom.etl.storage.route.resolver import (
    CatalogRouteResolver,
    CompositeRouteResolver,
    FixedCatalogRouteResolver,
    FixedPathRouteResolver,
    PathRouteResolver,
    TableRouteResolver,
)

__all__ = [
    "CatalogTarget",
    "PathTarget",
    "ResolvedTarget",
    "RoutedCatalog",
    "build_table_resolver",
    "TableRouteResolver",
    "CatalogRouteResolver",
    "PathRouteResolver",
    "FixedCatalogRouteResolver",
    "FixedPathRouteResolver",
    "CompositeRouteResolver",
]
