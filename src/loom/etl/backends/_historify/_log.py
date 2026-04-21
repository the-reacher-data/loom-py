"""LOG mode SCD Type 2 algorithm."""

from __future__ import annotations

from typing import TypeVar

from loom.etl.backends._historify._ops import HistorifyBackend
from loom.etl.declarative.target._history import HistorifySpec

F = TypeVar("F")


def apply_log(
    ops: HistorifyBackend[F],
    frame: F,
    existing: F,
    spec: HistorifySpec,
    join_key: list[str],
) -> F:
    """Apply LOG mode — rebuild history for entities present in incoming.

    Entities not in ``frame`` are preserved untouched. For affected entities,
    history is rebuilt from the union of existing events and incoming events.
    Incoming events win on same-date conflicts.
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

    all_events = ops.union([existing_events, frame])
    deduped = ops.dedup_last(all_events, join_key + [eff_col])

    rebuilt = ops.build_log_boundaries(deduped, spec)
    return ops.union([untouched, rebuilt])
