"""SNAPSHOT mode SCD Type 2 algorithm."""

from __future__ import annotations

from typing import Any, TypeVar

from loom.etl.backends._historify._ops import FrameOps
from loom.etl.declarative.target._history import HistorifySpec

F = TypeVar("F")


def apply_snapshot(
    ops: FrameOps[F],
    incoming: F,
    existing: F,
    spec: HistorifySpec,
    join_key: list[str],
    eff_date: Any,
) -> F:
    """Apply SNAPSHOT SCD2 classification.

    * New       — in incoming, not in open existing -> stamp + insert.
    * Unchanged — in both -> keep open existing row as-is.
    * Deleted   — in open existing, not in incoming -> apply delete policy.

    Closed historical rows are always preserved unchanged.
    """
    existing = ops.rollback_same_day_run(existing, spec, eff_date, join_key)

    open_existing = ops.filter_null(existing, spec.valid_to)
    closed_existing = ops.filter_not_null(existing, spec.valid_to)

    new_raw = ops.anti_join(incoming, open_existing, join_key)
    new_rows = ops.stamp_col(
        ops.stamp_col(new_raw, spec.valid_from, eff_date, None),
        spec.valid_to,
        None,
        None,
    )

    deleted = ops.anti_join(open_existing, incoming, join_key)
    closed_deleted = ops.apply_delete_policy(deleted, spec, eff_date)

    unchanged = ops.semi_join(open_existing, incoming, join_key)

    result = ops.union([closed_existing, unchanged, closed_deleted, new_rows])
    return ops.ensure_soft_delete_col(result, spec)
