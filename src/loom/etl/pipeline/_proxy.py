"""Params proxy — declarative param reference at class-definition time.

``params`` is a module-level singleton imported from ``loom.etl``.
Attribute access on it produces :class:`ParamExpr` nodes that the ETL
compiler validates against the step's ``ParamsT`` and the executor
resolves against the concrete params instance at runtime.

Usage::

    from loom.etl import col, params

    sources = Sources(
        orders=FromTable("raw.orders").where(
            (col("year")  == params.run_date.year)
            & (col("month") == params.run_date.month),
        ),
    )
    target = IntoTable("staging.orders").replace_partitions(
        values={"year": params.run_date.year, "month": params.run_date.month}
    )

``params`` in ``execute(self, params: DailyOrdersParams, *, ...)`` is the
real instance — Python scope rules separate the two without ambiguity.
"""

from __future__ import annotations

from typing import Any

from loom.etl.io.source._predicate import (
    _ColOps,
)


class ParamExpr(_ColOps):
    """Lazy path expression capturing param field access at definition time.

    Chains of attribute access build a path tuple that the compiler
    resolves against the declared ``ParamsT``.

    Args:
        path: Tuple of attribute names from the root params object.

    Example::

        params.run_date        # ParamExpr(("run_date",))
        params.run_date.year   # ParamExpr(("run_date", "year"))
    """

    __slots__ = ("_path",)

    def __init__(self, path: tuple[str, ...]) -> None:
        self._path = path

    @property
    def path(self) -> tuple[str, ...]:
        """Captured attribute path from the params root."""
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
    """Module-level singleton; attribute access yields :class:`ParamExpr` nodes.

    Never instantiated by user code — import ``params`` from ``loom.etl``.
    """

    __slots__ = ()

    def __getattr__(self, name: str) -> ParamExpr:
        if name.startswith("_"):
            raise AttributeError(name)
        return ParamExpr((name,))

    def __repr__(self) -> str:
        return "params"


#: Declarative params proxy.  Import from ``loom.etl`` and use in source /
#: target declarations to reference params fields symbolically.
params: _ParamProxy = _ParamProxy()


def resolve_param_expr(expr: ParamExpr, params_instance: Any) -> Any:
    """Resolve a :class:`ParamExpr` against a concrete params instance.

    Walks the stored path tuple using ``getattr`` at each step.

    Args:
        expr: The lazy param expression to evaluate.
        params_instance: The concrete params object for the current run.

    Returns:
        The resolved value at the end of the attribute path.

    Raises:
        AttributeError: If any segment of the path does not exist on the
            resolved intermediate value.

    Example::

        p = DailyOrdersParams(run_date=date(2024, 1, 5), countries=("ES",))
        resolve_param_expr(ParamExpr(("run_date", "year")), p)  # → 2024
    """
    value: Any = params_instance
    for segment in expr.path:
        value = getattr(value, segment)
    return value
