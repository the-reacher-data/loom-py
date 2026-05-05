"""Common logical-reference routing primitives."""

from loom.core.routing.ref import LogicalRef, as_logical_ref
from loom.core.routing.resolver import DefaultingRouteResolver, RouteResolver

__all__ = [
    "DefaultingRouteResolver",
    "LogicalRef",
    "RouteResolver",
    "as_logical_ref",
]
