"""Compile-time validation of ParamExpr field references.

Verifies that the first segment of every :class:`~loom.etl.pipeline._proxy.ParamExpr`
path used in source predicates or the target predicate names an actual field on the
declared ``ParamsT``.  Nested segments (e.g. ``.year`` on a ``datetime.date`` field)
are Python attribute access on the field's type — they are **not** validated here.

Internal module — consumed only by :mod:`loom.etl.compiler.validators._step`.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Protocol, cast

from loom.etl.compiler._errors import ETLCompilationError
from loom.etl.compiler._plan import SourceBinding, TargetBinding
from loom.etl.io.target._table import ReplaceWhereSpec
from loom.etl.pipeline._proxy import ParamExpr
from loom.etl.sql._predicate import PredicateNode


class _PredicateShape(Enum):
    BINARY = "binary"  # left/right
    IN = "in"  # ref/values
    UNARY = "unary"  # operand
    UNKNOWN = "unknown"


class _BinaryPredicateLike(Protocol):
    left: Any
    right: Any


class _InPredicateLike(Protocol):
    ref: Any
    values: Any


class _UnaryPredicateLike(Protocol):
    operand: Any


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
    shape = _predicate_shape(node)
    if shape is _PredicateShape.BINARY:
        binary = cast(_BinaryPredicateLike, node)
        left = binary.left
        right = binary.right
        if _is_predicate_node_like(left) or _is_predicate_node_like(right):
            _collect_exprs(left, out)
            _collect_exprs(right, out)
            return
        _collect_value(left, out)
        _collect_value(right, out)
        return
    if shape is _PredicateShape.IN:
        in_pred = cast(_InPredicateLike, node)
        _collect_value(in_pred.ref, out)
        _collect_value(in_pred.values, out)
        return
    if shape is _PredicateShape.UNARY:
        unary = cast(_UnaryPredicateLike, node)
        _collect_exprs(unary.operand, out)


def _collect_value(value: Any, out: list[ParamExpr]) -> None:
    """Append *value* to *out* if it is a :class:`ParamExpr`."""
    if _is_param_expr(value):
        out.append(value)


def _is_param_expr(value: Any) -> bool:
    """Return True for ParamExpr values across module reload boundaries."""
    if isinstance(value, ParamExpr):
        return True
    path = getattr(value, "path", None)
    return isinstance(path, tuple) and all(isinstance(part, str) for part in path)


def _predicate_shape(node: Any) -> _PredicateShape:
    """Classify predicate node by dataclass field shape (reload-safe)."""
    fields = getattr(type(node), "__dataclass_fields__", None)
    if not isinstance(fields, dict):
        return _PredicateShape.UNKNOWN
    names = frozenset(fields.keys())
    if names == {"left", "right"}:
        return _PredicateShape.BINARY
    if names == {"ref", "values"}:
        return _PredicateShape.IN
    if names == {"operand"}:
        return _PredicateShape.UNARY
    return _PredicateShape.UNKNOWN


def _is_predicate_node_like(value: Any) -> bool:
    """Return True for supported predicate node shapes."""
    return _predicate_shape(value) is not _PredicateShape.UNKNOWN
