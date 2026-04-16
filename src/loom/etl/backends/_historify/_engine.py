"""Backend-agnostic HistorifyEngine."""

from __future__ import annotations

from typing import Any, Generic, TypeVar

from loom.etl.backends._historify._common import (
    resolve_effective_date,
    resolve_track_cols,
)
from loom.etl.backends._historify._log import apply_log
from loom.etl.backends._historify._ops import FrameOps
from loom.etl.backends._historify._snapshot import apply_snapshot
from loom.etl.declarative.target._history import (
    HistorifyInputMode,
    HistorifySpec,
    HistorifyTemporalConflictError,
)

F = TypeVar("F")


class HistorifyEngine(Generic[F]):
    """SCD Type 2 engine that delegates frame operations to a FrameOps implementation."""

    def __init__(self, ops: FrameOps[F]) -> None:
        self._ops = ops

    def transform(
        self,
        frame: F,
        existing: F | None,
        spec: HistorifySpec,
        params_instance: object,
    ) -> F:
        """Apply SCD2 logic to ``frame`` and return the transformed frame.

        Args:
            frame:           Incoming data frame.
            existing:        Current target frame, or ``None`` for first run.
            spec:            Compiled HistorifySpec.
            params_instance: Runtime params for ParamExpr resolution.

        Returns:
            Transformed frame ready to be written to Delta.

        Raises:
            HistorifyKeyConflictError:      Duplicate entity state vectors.
            HistorifyDateCollisionError:    Same-date collisions in LOG mode.
            HistorifyTemporalConflictError: Future-open records, re-weave off.
        """
        ops = self._ops
        track_cols = resolve_track_cols(spec, ops.columns(frame))
        join_key = list(spec.keys) + list(track_cols)
        eff_date = resolve_effective_date(spec, params_instance)

        ops.assert_unique_keys(frame, join_key)
        if spec.mode is HistorifyInputMode.LOG:
            ops.assert_no_date_collisions(frame, join_key, str(spec.effective_date), spec)

        if existing is None:
            return _first_run(ops, frame, spec, join_key, eff_date)

        _temporal_guard(ops, existing, spec, eff_date)

        if spec.mode is HistorifyInputMode.SNAPSHOT:
            return apply_snapshot(ops, frame, existing, spec, join_key, eff_date)
        return apply_log(ops, frame, existing, spec, join_key)


def _first_run(
    ops: FrameOps[F],
    frame: F,
    spec: HistorifySpec,
    join_key: list[str],
    eff_date: Any,
) -> F:
    """Build the initial history frame when the target table is empty."""
    if spec.mode is HistorifyInputMode.SNAPSHOT:
        dtype = ops.history_dtype(spec)
        result = ops.stamp_col(
            ops.stamp_col(frame, spec.valid_from, eff_date, dtype),
            spec.valid_to,
            None,
            dtype,
        )
        return ops.ensure_soft_delete_col(result, spec)
    return ops.build_log_boundaries(frame, spec)


def _temporal_guard(
    ops: FrameOps[F],
    existing: F,
    spec: HistorifySpec,
    eff_date: Any,
) -> None:
    """Raise if future-open records exist and re-weave is disabled."""
    if spec.allow_temporal_rerun or spec.mode is HistorifyInputMode.LOG:
        return
    min_conflict = ops.temporal_conflict_min_date(existing, spec, eff_date)
    if min_conflict is None:
        return
    raise HistorifyTemporalConflictError(min_conflict, eff_date)
