"""Common helpers shared by all Historify backends."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from loom.etl.declarative.expr._params import ParamExpr
from loom.etl.declarative.target._history import HistorifyInputMode, HistorifySpec, HistoryDateType

_DATE_DELTA = timedelta(days=1)
_TS_DELTA = timedelta(microseconds=1)


def resolve_effective_date(spec: HistorifySpec, params_instance: object) -> Any:
    """Resolve effective date to a scalar (SNAPSHOT) or column name (LOG)."""
    if spec.mode is HistorifyInputMode.LOG:
        return spec.effective_date
    if isinstance(spec.effective_date, ParamExpr):
        return eval_param_expr(spec.effective_date, params_instance)
    return spec.effective_date


def eval_param_expr(expr: ParamExpr, params_instance: object) -> Any:
    """Walk the ParamExpr attribute path and return the resolved value."""
    value: Any = params_instance
    for attr in expr.path:
        value = getattr(value, attr)
    return value


def resolve_track_cols(spec: HistorifySpec, frame_cols: list[str]) -> tuple[str, ...]:
    """Return tracked columns — explicit or all non-key, non-history columns."""
    if spec.track is not None:
        return spec.track
    excluded = set(spec.keys) | {spec.valid_from, spec.valid_to}
    return tuple(c for c in frame_cols if c not in excluded)


def prev_period_value(eff_date: Any, spec: HistorifySpec) -> Any:
    """Return one unit before eff_date (one day or one microsecond)."""
    delta = _DATE_DELTA if spec.date_type is HistoryDateType.DATE else _TS_DELTA
    return eff_date - delta
