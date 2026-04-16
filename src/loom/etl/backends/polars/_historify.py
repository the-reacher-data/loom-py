"""Polars SCD Type 2 historify engine (legacy wrapper).

This module is preserved for backward compatibility with existing tests.
New code should use :class:`loom.etl.backends._historify.HistorifyEngine`
with :class:`PolarsFrameOps`.
"""

from __future__ import annotations

import contextlib
from typing import Any

import polars as pl
from deltalake import write_deltalake

from loom.etl.backends._historify._common import (
    eval_param_expr as _eval_param_expr,
)
from loom.etl.backends._historify._common import (
    resolve_track_cols as _resolve_track_cols,
)
from loom.etl.backends._historify._engine import HistorifyEngine
from loom.etl.backends._merge import _build_partition_predicate
from loom.etl.backends.polars._ops import PolarsFrameOps, _vf_dtype
from loom.etl.declarative.target._history import (
    HistorifyRepairReport,
    HistorifySpec,
)
from loom.etl.storage.routing import PathTarget


class PolarsHistorifyEngine:
    """Legacy wrapper around :class:`HistorifyEngine` for Polars."""

    def __init__(self) -> None:
        self._engine = HistorifyEngine(PolarsFrameOps())

    def apply(
        self,
        frame: pl.DataFrame,
        path_target: str | PathTarget,
        storage_options: dict[str, str] | None,
        spec: HistorifySpec,
        params_instance: object,
    ) -> HistorifyRepairReport | None:
        """Apply SCD2 and write result to Delta (legacy API)."""
        if isinstance(path_target, str):
            uri = path_target
            so = storage_options
        else:
            uri = path_target.location.uri
            so = path_target.location.storage_options or None

        existing = None
        with contextlib.suppress(Exception):
            existing = pl.read_delta(uri, storage_options=so)

        result = self._engine.transform(frame, existing, spec, params_instance)
        if spec.partition_scope:
            combos = list(result.select(list(spec.partition_scope)).unique().iter_rows(named=True))
            predicate = _build_partition_predicate(combos, spec.partition_scope)
            write_deltalake(uri, result, mode="overwrite", predicate=predicate, storage_options=so)
        else:
            write_deltalake(uri, result, mode="overwrite", storage_options=so)
        return None


# Re-export helpers for backward compatibility with tests.
def _stamp_new_rows(frame: pl.DataFrame, spec: HistorifySpec, eff_date: Any) -> pl.DataFrame:
    vf_dtype = _vf_dtype(spec)
    return frame.with_columns(
        pl.lit(eff_date).cast(vf_dtype).alias(spec.valid_from),
        pl.lit(None, dtype=vf_dtype).alias(spec.valid_to),
    )


_ops = PolarsFrameOps()
_assert_unique_entity_state = _ops.assert_unique_keys
_assert_no_date_collisions = _ops.assert_no_date_collisions
_rollback_same_day_run = _ops.rollback_same_day_run
_idempotency_strip = _ops.rollback_same_day_run
resolve_track_cols = _resolve_track_cols
_resolve_track_cols = resolve_track_cols
eval_param_expr = _eval_param_expr

stamp_new_rows = _stamp_new_rows

__all__ = [
    "PolarsHistorifyEngine",
    "_assert_no_date_collisions",
    "_assert_unique_entity_state",
    "_eval_param_expr",
    "_resolve_track_cols",
    "_rollback_same_day_run",
    "_stamp_new_rows",
    "eval_param_expr",
    "resolve_track_cols",
    "stamp_new_rows",
]
