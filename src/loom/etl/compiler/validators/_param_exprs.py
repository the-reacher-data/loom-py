"""Compile-time validation of ParamExpr field references.

Verifies that the first segment of every :class:`~loom.etl.pipeline._proxy.ParamExpr`
path used in source predicates or the target predicate names an actual field on the
declared ``ParamsT``.  Nested segments (e.g. ``.year`` on a ``datetime.date`` field)
are Python attribute access on the field's type — they are **not** validated here.

Internal module — consumed only by :mod:`loom.etl.compiler.validators._step`.
"""

from __future__ import annotations

from typing import Any

from loom.etl.compiler._errors import ETLCompilationError
from loom.etl.compiler._plan import SourceBinding, TargetBinding
from loom.etl.io.target._table import ReplaceWhereSpec
from loom.etl.pipeline._proxy import ParamExpr
from loom.etl.sql._predicate import (
    AndPred,
    EqPred,
    GePred,
    GtPred,
    InPred,
    LePred,
    LtPred,
    NePred,
    NotPred,
    OrPred,
    PredicateNode,
)


def validate_param_exprs(
    step_type: type[Any],
    params_type: type[Any],
    source_bindings: tuple[SourceBinding, ...],
    target_binding: TargetBinding,
) -> None:
    """Raise :class:`~loom.etl.compiler._errors.ETLCompilationError` if any
    ``ParamExpr`` references an undeclared params field.

    Validates only the first path segment — nested segments such as
    ``params.run_date.year`` are attribute access on the field's resolved
    type (e.g. ``datetime.date``) and cannot be verified at compile time
    without executing user code.

    Skips validation when *params_type* is not a ``msgspec.Struct`` subclass
    (custom params types that cannot be introspected via ``msgspec``).

    Args:
        step_type:       ETLStep subclass being compiled.
        params_type:     Generic ``ParamsT`` argument.
        source_bindings: Resolved source bindings carrying predicate nodes.
        target_binding:  Resolved target binding.

    Raises:
        ETLCompilationError: When ``path[0]`` is absent from *params_type*.
    """
    known = _known_fields(params_type)
    if known is None:
        return

    exprs: list[ParamExpr] = []
    for binding in source_bindings:
        for pred in binding.spec.predicates:
            _collect_exprs(pred, exprs)

    if isinstance(target_binding.spec, ReplaceWhereSpec):
        _collect_exprs(target_binding.spec.replace_predicate, exprs)

    for expr in exprs:
        field_name = expr.path[0]
        if field_name not in known:
            raise ETLCompilationError.unknown_param_field(step_type, field_name, params_type)


def _known_fields(params_type: type[Any]) -> frozenset[str] | None:
    """Return the declared field names of *params_type*, or ``None`` if not introspectable."""
    try:
        import msgspec

        if not (isinstance(params_type, type) and issubclass(params_type, msgspec.Struct)):
            return None
        return frozenset(f.name for f in msgspec.structs.fields(params_type))
    except Exception:
        return None


def _collect_exprs(node: PredicateNode, out: list[ParamExpr]) -> None:
    """Recursively collect all :class:`ParamExpr` leaves from *node* into *out*."""
    match node:
        case EqPred() | NePred() | GtPred() | GePred() | LtPred() | LePred():
            _collect_value(node.left, out)
            _collect_value(node.right, out)
        case InPred():
            _collect_value(node.ref, out)
            _collect_value(node.values, out)
        case AndPred() | OrPred():
            _collect_exprs(node.left, out)
            _collect_exprs(node.right, out)
        case NotPred():
            _collect_exprs(node.operand, out)


def _collect_value(value: Any, out: list[ParamExpr]) -> None:
    """Append *value* to *out* if it is a :class:`ParamExpr`."""
    if isinstance(value, ParamExpr):
        out.append(value)
