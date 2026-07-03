"""LOG mode SCD Type 2 algorithm."""

from __future__ import annotations

from typing import TypeVar

from loom.etl.backends._historify._ops import HistorifyBackend
from loom.etl.declarative.target._history import HistorifySpec

F = TypeVar("F")

_ORIGIN_COL = "__origin__"


def apply_log(
    ops: HistorifyBackend[F],
    frame: F,
    existing: F,
    spec: HistorifySpec,
) -> F:
    """Apply LOG mode — rebuild history for entities present in incoming.

    Entities not in ``frame`` are preserved untouched. For affected entities,
    history is rebuilt from the union of existing events and incoming events.
    Incoming events win on same ``(entity key, effective date)`` conflicts, so
    a correction with different tracked values replaces the stored event.
    """
    eff_col = str(spec.effective_date)
    entity_keys = list(spec.keys)

    affected_keys = ops.semi_join(existing, frame, entity_keys)
    untouched = ops.anti_join(existing, frame, entity_keys)

    existing_events = ops.rename(affected_keys, {spec.valid_from: eff_col})
    boundary_cols = [
        c for c in [spec.valid_to, spec.deleted_at] if c in ops.columns(existing_events)
    ]
    existing_events = ops.drop(existing_events, boundary_cols)

    all_events = ops.union(
        [
            ops.stamp_col(existing_events, _ORIGIN_COL, 0, None),
            ops.stamp_col(frame, _ORIGIN_COL, 1, None),
        ]
    )
    deduped = ops.dedup_priority(all_events, entity_keys + [eff_col], _ORIGIN_COL)
    deduped = ops.drop(deduped, [_ORIGIN_COL])

    rebuilt = ops.build_log_boundaries(deduped, spec)
    return ops.union([untouched, rebuilt])
