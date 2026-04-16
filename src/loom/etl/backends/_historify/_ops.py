"""HistorifyBackend Protocol — backend-specific primitives for SCD Type 2 algorithms."""

from __future__ import annotations

from typing import Any, Protocol, TypeVar

from loom.etl.declarative.target._history import HistorifySpec

F = TypeVar("F")


class HistorifyBackend(Protocol[F]):
    """Backend-specific frame operations used by SCD2Transform."""

    def columns(self, frame: F) -> list[str]: ...

    def history_dtype(self, spec: HistorifySpec) -> Any:
        """Return the native dtype for valid_from / valid_to columns."""
        ...

    def filter_null(self, frame: F, col: str) -> F: ...

    def filter_not_null(self, frame: F, col: str) -> F: ...

    def filter_eq(self, frame: F, col: str, value: Any, dtype: Any) -> F: ...

    def filter_ne(self, frame: F, col: str, value: Any, dtype: Any) -> F: ...

    def anti_join(self, left: F, right: F, on: list[str]) -> F: ...

    def semi_join(self, left: F, right: F, on: list[str]) -> F: ...

    def union(self, frames: list[F]) -> F: ...

    def stamp_col(self, frame: F, name: str, value: Any, dtype: Any) -> F:
        """Add or overwrite column with literal value."""
        ...

    def null_col(self, frame: F, name: str, dtype: Any) -> F:
        """Add null column with given dtype."""
        ...

    def rename(self, frame: F, rename_map: dict[str, str]) -> F:
        """Rename columns according to mapping."""
        ...

    def drop(self, frame: F, cols: list[str]) -> F:
        """Drop columns."""
        ...

    def dedup_last(self, frame: F, subset: list[str]) -> F:
        """Deduplicate keeping last occurrence."""
        ...

    def rollback_same_day_run(
        self, frame: F, spec: HistorifySpec, eff_date: Any, join_key: list[str]
    ) -> F:
        """Undo a previous run on the same eff_date (SNAPSHOT idempotency)."""
        ...

    def build_log_boundaries(self, frame: F, spec: HistorifySpec) -> F:
        """Compute valid_from / valid_to from sorted event frame (LOG mode)."""
        ...

    def apply_delete_policy(self, deleted: F, spec: HistorifySpec, eff_date: Any) -> F: ...

    def ensure_soft_delete_col(self, result: F, spec: HistorifySpec) -> F: ...

    def assert_unique_keys(self, frame: F, keys: list[str]) -> None: ...

    def assert_no_date_collisions(
        self, frame: F, keys: list[str], eff_col: str, spec: HistorifySpec
    ) -> None: ...

    def temporal_conflict_min_date(
        self, existing: F, spec: HistorifySpec, eff_date: Any
    ) -> Any | None:
        """Return min conflicting valid_from or None if no conflict."""
        ...
