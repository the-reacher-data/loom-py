"""Spark HistorifyBackend implementation."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from loom.etl.backends._historify._common import prev_period_value
from loom.etl.declarative.target._history import (
    DeletePolicy,
    HistorifyDateCollisionError,
    HistorifyKeyConflictError,
    HistorifySpec,
    HistoryDateType,
)


def _history_boundary_dtype_sql(spec: HistorifySpec) -> str:
    return "date" if spec.date_type is HistoryDateType.DATE else "timestamp"


class SparkHistorifyBackend:
    """Spark implementation of HistorifyBackend."""

    def columns(self, frame: DataFrame) -> list[str]:
        return list(frame.columns)

    def history_dtype(self, spec: HistorifySpec) -> str:
        return _history_boundary_dtype_sql(spec)

    def filter_null(self, frame: DataFrame, col: str) -> DataFrame:
        return frame.filter(F.col(col).isNull())

    def filter_not_null(self, frame: DataFrame, col: str) -> DataFrame:
        return frame.filter(F.col(col).isNotNull())

    def filter_eq(self, frame: DataFrame, col: str, value: Any, dtype: Any) -> DataFrame:
        expr = F.lit(value).cast(dtype) if dtype is not None else F.lit(value)
        return frame.filter(F.col(col) == expr)

    def filter_ne(self, frame: DataFrame, col: str, value: Any, dtype: Any) -> DataFrame:
        expr = F.lit(value).cast(dtype) if dtype is not None else F.lit(value)
        return frame.filter(F.col(col) != expr)

    def anti_join(self, left: DataFrame, right: DataFrame, on: list[str]) -> DataFrame:
        return left.join(right, on=on, how="left_anti")

    def semi_join(self, left: DataFrame, right: DataFrame, on: list[str]) -> DataFrame:
        return left.join(right, on=on, how="left_semi")

    def union(self, frames: list[DataFrame]) -> DataFrame:
        result = frames[0]
        for f in frames[1:]:
            result = result.unionByName(f, allowMissingColumns=True)
        return result

    def stamp_col(self, frame: DataFrame, name: str, value: Any, dtype: Any) -> DataFrame:
        if dtype is not None:
            return frame.withColumn(name, F.lit(value).cast(dtype))
        return frame.withColumn(name, F.lit(value))

    def null_col(self, frame: DataFrame, name: str, dtype: Any) -> DataFrame:
        return frame.withColumn(name, F.lit(None).cast(dtype))

    def rename(self, frame: DataFrame, rename_map: dict[str, str]) -> DataFrame:
        result = frame
        for old, new in rename_map.items():
            result = result.withColumnRenamed(old, new)
        return result

    def drop(self, frame: DataFrame, cols: list[str]) -> DataFrame:
        return frame.drop(*cols)

    def dedup_last(self, frame: DataFrame, subset: list[str]) -> DataFrame:
        window = (
            Window.partitionBy(subset)
            .orderBy(F.lit(1).desc())
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        return (
            frame.withColumn("__rn__", F.row_number().over(window))
            .filter(F.col("__rn__") == 1)
            .drop("__rn__")
        )

    def apply_overwrite_cols(
        self,
        unchanged: DataFrame,
        incoming: DataFrame,
        join_key: list[str],
        overwrite: tuple[str, ...],
    ) -> DataFrame:
        overwrite_vals = incoming.select(join_key + list(overwrite))
        return unchanged.drop(*overwrite).join(overwrite_vals, on=join_key, how="left")

    def rollback_same_day_run(
        self,
        frame: DataFrame,
        spec: HistorifySpec,
        eff_date: Any,
        join_key: list[str],
    ) -> DataFrame:
        boundary_dtype = _history_boundary_dtype_sql(spec)
        prev = prev_period_value(eff_date, spec)

        stripped = frame.filter(F.col(spec.valid_from) != F.lit(eff_date).cast(boundary_dtype))

        window = (
            Window.partitionBy(join_key)
            .orderBy(F.col(spec.valid_from).desc())
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        with_rank = stripped.withColumn("__rank__", F.rank().over(window))

        is_last_closed_by_run = (F.col("__rank__") == 1) & (
            F.col(spec.valid_to) == F.lit(prev).cast(boundary_dtype)
        )
        return with_rank.withColumn(
            spec.valid_to,
            F.when(is_last_closed_by_run, F.lit(None).cast(boundary_dtype)).otherwise(
                F.col(spec.valid_to)
            ),
        ).drop("__rank__")

    def build_log_boundaries(
        self,
        frame: DataFrame,
        spec: HistorifySpec,
    ) -> DataFrame:
        eff_col = str(spec.effective_date)
        entity_key = list(spec.keys)
        boundary_dtype = _history_boundary_dtype_sql(spec)

        # Lead/Lag in Spark do not accept any explicit window frame.
        window = Window.partitionBy(entity_key).orderBy(eff_col)
        next_date = F.lead(eff_col).over(window)

        if spec.date_type is HistoryDateType.DATE:
            valid_to_expr = F.date_sub(next_date, 1)
        else:
            valid_to_expr = next_date - F.expr("INTERVAL 1 MICROSECOND")

        return (
            frame.withColumn(spec.valid_from, F.col(eff_col).cast(boundary_dtype))
            .withColumn(spec.valid_to, valid_to_expr.cast(boundary_dtype))
            .drop(eff_col)
        )

    def apply_delete_policy(
        self,
        deleted: DataFrame,
        spec: HistorifySpec,
        eff_date: Any,
    ) -> DataFrame:
        if spec.delete_policy is DeletePolicy.IGNORE:
            return deleted

        boundary_dtype = _history_boundary_dtype_sql(spec)
        prev = prev_period_value(eff_date, spec)
        result = deleted.withColumn(spec.valid_to, F.lit(prev).cast(boundary_dtype))

        if spec.delete_policy is DeletePolicy.SOFT_DELETE:
            result = result.withColumn(spec.deleted_at, F.lit(eff_date).cast(boundary_dtype))

        return result

    def ensure_soft_delete_col(self, result: DataFrame, spec: HistorifySpec) -> DataFrame:
        if spec.delete_policy is not DeletePolicy.SOFT_DELETE:
            return result
        if spec.deleted_at in result.columns:
            return result
        boundary_dtype = _history_boundary_dtype_sql(spec)
        return result.withColumn(spec.deleted_at, F.lit(None).cast(boundary_dtype))

    def assert_unique_keys(self, frame: DataFrame, keys: list[str]) -> None:
        n_rows = frame.count()
        n_unique = frame.select(keys).distinct().count()
        if n_rows == n_unique:
            return
        dupes = (
            frame.groupBy(keys).count().filter(F.col("count") > 1).drop("count").limit(5).collect()
        )
        raise HistorifyKeyConflictError(str([row.asDict() for row in dupes]))

    def assert_no_date_collisions(
        self,
        frame: DataFrame,
        keys: list[str],
        eff_col: str,
        spec: HistorifySpec,
    ) -> None:
        if spec.date_type is not HistoryDateType.DATE:
            return
        combo_key = keys + [eff_col]
        n_rows = frame.count()
        n_unique = frame.select(combo_key).distinct().count()
        if n_rows == n_unique:
            return
        dupes = (
            frame.groupBy(combo_key)
            .count()
            .filter(F.col("count") > 1)
            .drop("count")
            .limit(5)
            .collect()
        )
        raise HistorifyDateCollisionError(str([row.asDict() for row in dupes]))

    def temporal_conflict_min_date(
        self,
        existing: DataFrame,
        spec: HistorifySpec,
        eff_date: Any,
    ) -> Any | None:
        boundary_dtype = _history_boundary_dtype_sql(spec)
        conflict_agg = (
            existing.filter(F.col(spec.valid_from) > F.lit(eff_date).cast(boundary_dtype))
            .agg(F.min(spec.valid_from).alias("min_valid_from"))
            .collect()
        )
        min_conflict = conflict_agg[0]["min_valid_from"]
        if min_conflict is None:
            return None
        return min_conflict
