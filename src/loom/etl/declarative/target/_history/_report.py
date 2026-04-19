"""Historify repair report types."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class HistorifyRepairReport:
    """Structured report produced by a successful re-weave operation.

    Returned by the backend engine when ``allow_temporal_rerun=True`` and past-date
    data was corrected. Consumers may use this to schedule downstream re-runs
    for the affected date range.

    Attributes:
        affected_keys: Frozenset of entity key tuples that were modified.
        dates_requiring_rerun: Sorted tuple of dates where history was repaired.
        warnings: Human-readable description of each repair action.
    """

    affected_keys: frozenset[tuple[object, ...]]
    dates_requiring_rerun: tuple[object, ...]
    warnings: tuple[str, ...]


__all__ = ["HistorifyRepairReport"]
