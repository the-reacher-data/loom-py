"""Polars FrameOps implementation."""

from __future__ import annotations

from typing import Any

import polars as pl

from loom.etl.backends._historify._common import prev_period_value
from loom.etl.declarative.target._history import (
    DeletePolicy,
    HistorifyDateCollisionError,
    HistorifyKeyConflictError,
    HistorifySpec,
    HistoryDateType,
)


def _vf_dtype(spec: HistorifySpec) -> type[pl.Date] | type[pl.Datetime]:
    return pl.Date if spec.date_type is HistoryDateType.DATE else pl.Datetime


class PolarsFrameOps:
    """Polars implementation of FrameOps."""

    def columns(self, frame: pl.DataFrame) -> list[str]:
        return list(frame.columns)

    def history_dtype(self, spec: HistorifySpec) -> type[pl.Date] | type[pl.Datetime]:
        return _vf_dtype(spec)

    def filter_null(self, frame: pl.DataFrame, col: str) -> pl.DataFrame:
        return frame.filter(pl.col(col).is_null())

    def filter_not_null(self, frame: pl.DataFrame, col: str) -> pl.DataFrame:
        return frame.filter(pl.col(col).is_not_null())

    def filter_eq(self, frame: pl.DataFrame, col: str, value: Any, dtype: Any) -> pl.DataFrame:
        expr = pl.lit(value).cast(dtype) if dtype is not None else pl.lit(value)
        return frame.filter(pl.col(col) == expr)

    def filter_ne(self, frame: pl.DataFrame, col: str, value: Any, dtype: Any) -> pl.DataFrame:
        expr = pl.lit(value).cast(dtype) if dtype is not None else pl.lit(value)
        return frame.filter(pl.col(col) != expr)

    def anti_join(self, left: pl.DataFrame, right: pl.DataFrame, on: list[str]) -> pl.DataFrame:
        return left.join(right, on=on, how="anti")

    def semi_join(self, left: pl.DataFrame, right: pl.DataFrame, on: list[str]) -> pl.DataFrame:
        return left.join(right, on=on, how="semi")

    def union(self, frames: list[pl.DataFrame]) -> pl.DataFrame:
        return pl.concat(frames, how="diagonal_relaxed")

    def stamp_col(self, frame: pl.DataFrame, name: str, value: Any, dtype: Any) -> pl.DataFrame:
        if dtype is not None:
            return frame.with_columns(pl.lit(value).cast(dtype).alias(name))
        return frame.with_columns(pl.lit(value).alias(name))

    def null_col(self, frame: pl.DataFrame, name: str, dtype: Any) -> pl.DataFrame:
        return frame.with_columns(pl.lit(None, dtype=dtype).alias(name))

    def rename(self, frame: pl.DataFrame, rename_map: dict[str, str]) -> pl.DataFrame:
        return frame.rename(rename_map)

    def drop(self, frame: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
        return frame.drop(cols)

    def dedup_last(self, frame: pl.DataFrame, subset: list[str]) -> pl.DataFrame:
        return frame.unique(subset=subset, keep="last")

    def rollback_same_day_run(
        self,
        frame: pl.DataFrame,
        spec: HistorifySpec,
        eff_date: Any,
        join_key: list[str],
    ) -> pl.DataFrame:
        vf_dtype = _vf_dtype(spec)
        prev = prev_period_value(eff_date, spec)

        stripped = frame.filter(pl.col(spec.valid_from) != pl.lit(eff_date).cast(vf_dtype))
        if stripped.is_empty():
            return stripped

        max_vf = stripped.group_by(join_key).agg(pl.col(spec.valid_from).max().alias("__max_vf__"))
        with_max = stripped.join(max_vf, on=join_key, how="left")

        is_last_closed_by_run = (pl.col(spec.valid_from) == pl.col("__max_vf__")) & (
            pl.col(spec.valid_to) == pl.lit(prev).cast(vf_dtype)
        )
        return with_max.with_columns(
            pl.when(is_last_closed_by_run)
            .then(pl.lit(None, dtype=vf_dtype))
            .otherwise(pl.col(spec.valid_to))
            .alias(spec.valid_to)
        ).drop("__max_vf__")

    def build_log_boundaries(
        self,
        frame: pl.DataFrame,
        spec: HistorifySpec,
        join_key: list[str],
    ) -> pl.DataFrame:
        eff_col = str(spec.effective_date)
        entity_key = list(spec.keys)
        vf_dtype = _vf_dtype(spec)
        offset = (
            pl.duration(days=1)
            if spec.date_type is HistoryDateType.DATE
            else pl.duration(microseconds=1)
        )

        sorted_events = frame.sort(entity_key + [eff_col])
        next_date = pl.col(eff_col).shift(-1).over(entity_key)

        return sorted_events.with_columns(
            pl.col(eff_col).cast(vf_dtype).alias(spec.valid_from),
            (next_date - offset).cast(vf_dtype).alias(spec.valid_to),
        ).drop(eff_col)

    def apply_delete_policy(
        self,
        deleted: pl.DataFrame,
        spec: HistorifySpec,
        eff_date: Any,
    ) -> pl.DataFrame:
        if spec.delete_policy is DeletePolicy.IGNORE:
            return deleted

        vf_dtype = _vf_dtype(spec)
        prev = prev_period_value(eff_date, spec)
        result = deleted.with_columns(pl.lit(prev).cast(vf_dtype).alias(spec.valid_to))

        if spec.delete_policy is DeletePolicy.SOFT_DELETE:
            result = result.with_columns(pl.lit(eff_date).cast(vf_dtype).alias("deleted_at"))

        return result

    def ensure_soft_delete_col(self, result: pl.DataFrame, spec: HistorifySpec) -> pl.DataFrame:
        if spec.delete_policy is not DeletePolicy.SOFT_DELETE:
            return result
        if "deleted_at" in result.columns:
            return result
        vf_dtype = _vf_dtype(spec)
        return result.with_columns(pl.lit(None, dtype=vf_dtype).alias("deleted_at"))

    def assert_unique_keys(self, frame: pl.DataFrame, keys: list[str]) -> None:
        if len(frame) == frame.select(keys).n_unique():
            return
        dupes = frame.filter(frame.select(keys).is_duplicated()).select(keys).head(5)
        raise HistorifyKeyConflictError(str(dupes.to_dicts()))

    def assert_no_date_collisions(
        self,
        frame: pl.DataFrame,
        keys: list[str],
        eff_col: str,
        spec: HistorifySpec,
    ) -> None:
        if spec.date_type is not HistoryDateType.DATE:
            return
        combo_key = keys + [eff_col]
        if len(frame) == frame.select(combo_key).n_unique():
            return
        dupes = frame.filter(frame.select(combo_key).is_duplicated()).select(combo_key).head(5)
        raise HistorifyDateCollisionError(str(dupes.to_dicts()))

    def temporal_conflict_min_date(
        self,
        existing: pl.DataFrame,
        spec: HistorifySpec,
        eff_date: Any,
    ) -> Any | None:
        vf_dtype = _vf_dtype(spec)
        conflict = existing.filter(pl.col(spec.valid_from) > pl.lit(eff_date).cast(vf_dtype))
        if conflict.is_empty():
            return None
        return conflict[spec.valid_from].min()
