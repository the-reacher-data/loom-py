"""Params proxy and expressions for the ETL declarative DSL."""

from __future__ import annotations

from typing import Any

from loom.etl.declarative.expr._predicate import _ColOps


class ParamExpr(_ColOps):
    """Lazy path expression capturing params field access."""

    __slots__ = ("_path",)

    def __init__(self, path: tuple[str, ...]) -> None:
        self._path = path

    @property
    def path(self) -> tuple[str, ...]:
        """Captured attribute path from params root."""
        return self._path

    def __getattr__(self, name: str) -> ParamExpr:
        if name.startswith("_"):
            raise AttributeError(name)
        return ParamExpr(self._path + (name,))

    def __hash__(self) -> int:
        return hash(self._path)

    def __repr__(self) -> str:
        return f"params.{'.'.join(self._path)}"


class _ParamProxy:
    """Module-level singleton that creates :class:`ParamExpr` on access."""

    __slots__ = ()

    def __getattr__(self, name: str) -> ParamExpr:
        if name.startswith("_"):
            raise AttributeError(name)
        return ParamExpr((name,))

    def __repr__(self) -> str:
        return "params"


params: _ParamProxy = _ParamProxy()
"""Declarative params proxy used in source/target DSL definitions."""


def resolve_param_expr(expr: ParamExpr, params_instance: Any) -> Any:
    """Resolve a :class:`ParamExpr` against concrete params instance."""
    value: Any = params_instance
    for segment in expr.path:
        value = getattr(value, segment)
    return value


__all__ = ["ParamExpr", "params", "resolve_param_expr"]
