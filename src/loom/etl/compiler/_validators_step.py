"""Step-level compile-time validators."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol, cast

import msgspec

from loom.etl.compiler._errors import ETLCompilationError, ETLErrorCode
from loom.etl.compiler._plan import SourceBinding, TargetBinding
from loom.etl.declarative.expr._params import ParamExpr
from loom.etl.declarative.expr._predicate import PredicateNode
from loom.etl.declarative.target import TargetSpec
from loom.etl.pipeline._step_sql import StepSQL


class _BinaryPredicateLike(Protocol):
    left: Any
    right: Any


class _InPredicateLike(Protocol):
    ref: Any
    values: Any


class _UnaryPredicateLike(Protocol):
    operand: Any


class _ReplaceWhereSpecLike(Protocol):
    replace_predicate: PredicateNode


class _UpsertSpecLike(Protocol):
    upsert_keys: tuple[str, ...]
    upsert_exclude: tuple[str, ...]
    upsert_include: tuple[str, ...]


class _HistorifySpecLike(Protocol):
    keys: tuple[str, ...]
    effective_date: Any


class _PredicateShape(Enum):
    BINARY = "binary"
    IN = "in"
    UNARY = "unary"
    UNKNOWN = "unknown"


class _PredicateField(Enum):
    LEFT = "left"
    RIGHT = "right"
    REF = "ref"
    VALUES = "values"
    OPERAND = "operand"


@dataclass(frozen=True)
class StepCompilationContext:
    """Carries resolved step data required by per-step validators."""

    step_type: type[Any]
    params_type: type[Any]
    source_bindings: tuple[SourceBinding, ...]
    target_binding: TargetBinding


def validate_step(ctx: StepCompilationContext) -> None:
    """Run all per-step compile-time validators against *ctx*."""
    validate_execute_signature(ctx.step_type, ctx.params_type, ctx.source_bindings)
    validate_upsert_spec(ctx.step_type, ctx.target_binding.spec)
    validate_param_exprs(ctx.step_type, ctx.params_type, ctx.source_bindings, ctx.target_binding)


def validate_execute_signature(
    step_type: type[Any],
    params_type: type[Any],
    source_bindings: tuple[SourceBinding, ...],
) -> None:
    """Validate execute() params and keyword-only source frame bindings."""
    sig = inspect.signature(step_type.execute)
    params = list(sig.parameters.values())
    _validate_params_arg(step_type, params, params_type)
    if _is_sql_step_type(step_type):
        return
    kw_only = _collect_kw_only_frames(params)
    source_aliases = {b.alias for b in source_bindings}
    _check_missing_frames(step_type, source_aliases, kw_only)
    _check_extra_frames(step_type, source_aliases, kw_only)


def validate_params_compat(
    component_type: type[Any],
    component_params: type[Any],
    context_params: type[Any],
) -> None:
    """Raise if component_params requires fields absent from context_params."""
    if component_params is context_params:
        return
    if issubclass(context_params, component_params):
        return
    component_fields = {f.name for f in msgspec.structs.fields(component_params)}
    context_fields = {f.name for f in msgspec.structs.fields(context_params)}
    missing = component_fields - context_fields
    if missing:
        raise ETLCompilationError.missing_params_fields(
            component_type, frozenset(missing), context_params
        )


def _validate_params_arg(
    step_type: type[Any],
    params: list[inspect.Parameter],
    params_type: type[Any],
) -> None:
    positional_kinds = {
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.POSITIONAL_ONLY,
    }
    positional = [p for p in params if p.kind in positional_kinds and p.name != "self"]
    if not positional:
        raise ETLCompilationError.invalid_params_type(step_type, params_type)
    first = positional[0]
    if first.name != "params":
        raise ETLCompilationError.invalid_params_name(step_type, first.name)


def _collect_kw_only_frames(params: list[inspect.Parameter]) -> dict[str, inspect.Parameter]:
    return {p.name: p for p in params if p.kind is inspect.Parameter.KEYWORD_ONLY}


def _is_sql_step_type(step_type: type[Any]) -> bool:
    return issubclass(step_type, StepSQL)


def _check_missing_frames(
    step_type: type[Any],
    source_aliases: set[str],
    kw_only: dict[str, inspect.Parameter],
) -> None:
    missing = source_aliases - set(kw_only)
    if missing:
        raise ETLCompilationError.missing_source_params(step_type, frozenset(missing))


def _check_extra_frames(
    step_type: type[Any],
    source_aliases: set[str],
    kw_only: dict[str, inspect.Parameter],
) -> None:
    extra = set(kw_only) - source_aliases
    if extra:
        raise ETLCompilationError.extra_source_params(step_type, frozenset(extra))


def validate_param_exprs(
    step_type: type[Any],
    params_type: type[Any],
    source_bindings: tuple[SourceBinding, ...],
    target_binding: TargetBinding,
) -> None:
    """Raise when any ParamExpr references an undeclared params field."""
    known = _known_fields(params_type)
    if known is None:
        return

    exprs: list[ParamExpr] = []
    for binding in source_bindings:
        for pred in getattr(binding.spec, "predicates", ()):
            _collect_exprs(pred, exprs)

    if _is_replace_where_like(target_binding.spec):
        target_spec = cast(_ReplaceWhereSpecLike, target_binding.spec)
        _collect_exprs(target_spec.replace_predicate, exprs)

    if _is_historify_like(target_binding.spec):
        historify_spec = cast(_HistorifySpecLike, target_binding.spec)
        if _is_param_expr(historify_spec.effective_date):
            exprs.append(cast(ParamExpr, historify_spec.effective_date))

    for expr in exprs:
        field_name = expr.path[0]
        if field_name not in known:
            raise ETLCompilationError.unknown_param_field(step_type, field_name, params_type)


def _known_fields(params_type: type[Any]) -> frozenset[str] | None:
    try:
        if not (isinstance(params_type, type) and issubclass(params_type, msgspec.Struct)):
            return None
        return frozenset(f.name for f in msgspec.structs.fields(params_type))
    except Exception:
        return None


def _collect_exprs(node: PredicateNode, out: list[ParamExpr]) -> None:
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
    if _is_param_expr(value):
        out.append(value)


def _is_param_expr(value: Any) -> bool:
    if isinstance(value, ParamExpr):
        return True
    path = getattr(value, "path", None)
    return isinstance(path, tuple) and all(isinstance(part, str) for part in path)


def _predicate_shape(node: Any) -> _PredicateShape:
    fields = getattr(type(node), "__dataclass_fields__", None)
    if not isinstance(fields, dict):
        return _PredicateShape.UNKNOWN
    if _has_fields(fields, _PredicateField.LEFT, _PredicateField.RIGHT):
        return _PredicateShape.BINARY
    if _has_fields(fields, _PredicateField.REF, _PredicateField.VALUES):
        return _PredicateShape.IN
    if _has_fields(fields, _PredicateField.OPERAND):
        return _PredicateShape.UNARY
    return _PredicateShape.UNKNOWN


def _is_predicate_node_like(value: Any) -> bool:
    return _predicate_shape(value) is not _PredicateShape.UNKNOWN


def _has_fields(fields: dict[str, Any], *expected: _PredicateField) -> bool:
    if len(fields) != len(expected):
        return False
    return all(field.value in fields for field in expected)


def _is_replace_where_like(spec: object) -> bool:
    return hasattr(spec, "replace_predicate") and hasattr(spec, "table_ref")


def _is_historify_like(spec: object) -> bool:
    return (
        isinstance(getattr(spec, "keys", None), tuple)
        and hasattr(spec, "effective_date")
        and hasattr(spec, "table_ref")
    )


def validate_upsert_spec(step_type: type[Any], spec: TargetSpec) -> None:
    """Validate UPSERT-specific constraints at compile time."""
    if not _is_upsert_like(spec):
        return
    upsert_spec = cast(_UpsertSpecLike, spec)
    _check_upsert_keys_non_empty(step_type, upsert_spec)
    _check_upsert_exclude_include_exclusive(step_type, upsert_spec)
    _check_upsert_exclude_keys_disjoint(step_type, upsert_spec)


def validate_historify_spec(step_type: type[Any], spec: TargetSpec) -> None:
    """Validate IntoHistory-specific constraints at compile time.

    Belt-and-suspenders check: :class:`~loom.etl.IntoHistory` already
    validates these constraints at construction time, but this validator
    runs again at compile time to catch any directly-constructed
    :class:`~loom.etl.HistorifySpec` instances that bypassed the builder.

    Currently validates:
    * ``keys`` is non-empty.
    * ``track`` does not overlap with ``keys``.

    ``effective_date`` :class:`~loom.etl.ParamExpr` references are validated
    separately by :func:`validate_param_exprs`.

    Args:
        step_type: The step class being compiled (used in error messages).
        spec:      Compiled target spec to inspect.
    """
    if not _is_historify_like(spec):
        return
    historify = cast(_HistorifySpecLike, spec)
    if not historify.keys:
        raise ETLCompilationError(
            code=ETLErrorCode.INVALID_TARGET_TYPE,
            component=step_type.__qualname__,
            field="keys",
            message=(
                f"{step_type.__qualname__}: IntoHistory 'keys' must contain "
                "at least one column name."
            ),
        )
    track = getattr(historify, "track", None)
    if track is not None and isinstance(track, tuple):
        overlap = frozenset(historify.keys) & frozenset(track)
        if overlap:
            raise ETLCompilationError(
                code=ETLErrorCode.INVALID_TARGET_TYPE,
                component=step_type.__qualname__,
                field="track",
                message=(
                    f"{step_type.__qualname__}: IntoHistory 'keys' and 'track' "
                    f"must not overlap. Conflicting columns: {sorted(overlap)}"
                ),
            )


def _check_upsert_keys_non_empty(step_type: type[Any], spec: _UpsertSpecLike) -> None:
    if not spec.upsert_keys:
        raise ETLCompilationError.upsert_no_keys(step_type)


def _check_upsert_exclude_include_exclusive(step_type: type[Any], spec: _UpsertSpecLike) -> None:
    if spec.upsert_exclude and spec.upsert_include:
        raise ETLCompilationError.upsert_exclude_include_conflict(step_type)


def _check_upsert_exclude_keys_disjoint(step_type: type[Any], spec: _UpsertSpecLike) -> None:
    overlap = frozenset(spec.upsert_exclude) & frozenset(spec.upsert_keys)
    if overlap:
        raise ETLCompilationError.upsert_key_in_exclude(step_type, overlap)


def _is_upsert_like(spec: object) -> bool:
    keys = getattr(spec, "upsert_keys", None)
    exclude = getattr(spec, "upsert_exclude", None)
    include = getattr(spec, "upsert_include", None)
    return isinstance(keys, tuple) and isinstance(exclude, tuple) and isinstance(include, tuple)


__all__ = [
    "StepCompilationContext",
    "validate_execute_signature",
    "validate_historify_spec",
    "validate_param_exprs",
    "validate_params_compat",
    "validate_step",
    "validate_upsert_spec",
]
