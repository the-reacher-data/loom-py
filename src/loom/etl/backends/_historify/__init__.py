"""Backend-agnostic SCD Type 2 transform and HistorifyBackend Protocol."""

from loom.etl.backends._historify._common import (
    eval_param_expr,
    prev_period_value,
    resolve_effective_date,
    resolve_track_cols,
)
from loom.etl.backends._historify._engine import scd2_transform
from loom.etl.backends._historify._ops import HistorifyBackend

__all__ = [
    "eval_param_expr",
    "HistorifyBackend",
    "scd2_transform",
    "prev_period_value",
    "resolve_effective_date",
    "resolve_track_cols",
]
