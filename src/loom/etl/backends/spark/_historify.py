"""Spark SCD Type 2 historify engine.

Implements the partition-replace strategy for Delta tables using PySpark:
read relevant partitions -> apply SCD2 logic as Spark transformations ->
write back with ``replaceWhere`` to atomically replace only those partitions.

All transformations are lazy (Spark plan) until the final write action.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from functools import reduce
from typing import Any

from pyspark.errors.exceptions.base import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from loom.etl.backends._merge import _build_partition_predicate
from loom.etl.declarative.expr._params import ParamExpr
from loom.etl.declarative.target._history import (
    DeletePolicy,
    HistorifyDateCollisionError,
    HistorifyInputMode,
    HistorifyKeyConflictError,
    HistorifyRepairReport,
    HistorifySpec,
    HistorifyTemporalConflictError,
    HistoryDateType,
)
from loom.etl.storage.routing import CatalogTarget, ResolvedTarget

_log = logging.getLogger(__name__)

_DATE_DELTA = timedelta(days=1)
_TS_DELTA = timedelta(microseconds=1)


class SparkHistorifyEngine:
    """SCD Type 2 engine for Spark + Delta tables.

    Implements the same partition-replace strategy as
    :class:`~loom.etl.backends.polars._historify.PolarsHistorifyEngine`
    using PySpark transformations and Delta Lake.

    All SCD2 logic runs as lazy Spark plan until the final write action.

    Example::

        engine = SparkHistorifyEngine()
        engine.apply(
            spark=spark,
            frame=df,
            target=resolved_target,
            spec=spec,
            params_instance=params,
        )
    """

    def apply(
        self,
        spark: SparkSession,
        frame: DataFrame,
        target: ResolvedTarget,
        spec: HistorifySpec,
        params_instance: object,
    ) -> HistorifyRepairReport | None:
        """Apply SCD2 and commit result to the Delta target.

        Args:
            spark:           Active SparkSession.
            frame:           Incoming materialized DataFrame.
            target:          Resolved catalog or path target.
            spec:            Compiled :class:`~loom.etl.HistorifySpec`.
            params_instance: Runtime params for :class:`~loom.etl.ParamExpr`
                             resolution.

        Returns:
            ``None`` for normal forward runs.

        Raises:
            HistorifyKeyConflictError:      Duplicate entity state vectors.
            HistorifyDateCollisionError:    Same-date collisions in LOG mode.
            HistorifyTemporalConflictError: Future-open records, re-weave off.
        """
        track_cols = _resolve_track_cols(spec, frame.columns)
        join_key = list(spec.keys) + list(track_cols)
        eff_date = _resolve_effective_date(spec, params_instance)

        _assert_unique_entity_state(frame, join_key)
        if spec.mode is HistorifyInputMode.LOG:
            _assert_no_date_collisions(frame, join_key, str(spec.effective_date), spec)

        existing = _read_existing(spark, frame, target, spec)

        if existing is None:
            result = _first_run(frame, spec, join_key, eff_date)
            _write_result(frame, result, target, spec)
            return None

        _temporal_guard(existing, spec, eff_date)

        if spec.mode is HistorifyInputMode.SNAPSHOT:
            stripped = _idempotency_strip(existing, spec, eff_date, join_key)
            result = _apply_snapshot(frame, stripped, spec, join_key, eff_date)
        else:
            result = _apply_log(frame, existing, spec, join_key)

        _write_result(frame, result, target, spec)
        return None


# ---------------------------------------------------------------------------
# Resolution helpers
# ---------------------------------------------------------------------------


def _resolve_effective_date(spec: HistorifySpec, params_instance: object) -> Any:
    """Resolve effective date to a scalar (SNAPSHOT) or column name (LOG)."""
    if spec.mode is HistorifyInputMode.LOG:
        return spec.effective_date
    if isinstance(spec.effective_date, ParamExpr):
        return _eval_param_expr(spec.effective_date, params_instance)
    return spec.effective_date


def _eval_param_expr(expr: ParamExpr, params_instance: object) -> Any:
    """Walk the ParamExpr attribute path and return the resolved value."""
    value: Any = params_instance
    for attr in expr.path:
        value = getattr(value, attr)
    return value


def _resolve_track_cols(spec: HistorifySpec, frame_cols: list[str]) -> tuple[str, ...]:
    """Return tracked columns — explicit or all non-key, non-history columns."""
    if spec.track is not None:
        return spec.track
    excluded = set(spec.keys) | {spec.valid_from, spec.valid_to}
    return tuple(c for c in frame_cols if c not in excluded)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _assert_unique_entity_state(frame: DataFrame, join_key: list[str]) -> None:
    """Raise HistorifyKeyConflictError on duplicate (keys + track) combos."""
    n_rows = frame.count()
    n_unique = frame.select(join_key).distinct().count()
    if n_rows == n_unique:
        return
    dupes = (
        frame.groupBy(join_key).count().filter(F.col("count") > 1).drop("count").limit(5).collect()
    )
    raise HistorifyKeyConflictError(str([row.asDict() for row in dupes]))


def _assert_no_date_collisions(
    frame: DataFrame,
    join_key: list[str],
    eff_col: str,
    spec: HistorifySpec,
) -> None:
    """Raise HistorifyDateCollisionError on same (join_key, date) duplicates."""
    if spec.date_type is not HistoryDateType.DATE:
        return
    combo_key = join_key + [eff_col]
    n_rows = frame.count()
    n_unique = frame.select(combo_key).distinct().count()
    if n_rows == n_unique:
        return
    dupes = (
        frame.groupBy(combo_key).count().filter(F.col("count") > 1).drop("count").limit(5).collect()
    )
    raise HistorifyDateCollisionError(str([row.asDict() for row in dupes]))


# ---------------------------------------------------------------------------
# Delta I/O
# ---------------------------------------------------------------------------


def _read_existing(
    spark: SparkSession,
    frame: DataFrame,
    target: ResolvedTarget,
    spec: HistorifySpec,
) -> DataFrame | None:
    """Read existing Delta data, partition-filtered when scope is declared.

    Returns:
        Existing DataFrame, or ``None`` when the table does not exist yet.
    """
    df = _load_target(spark, target)
    if df is None:
        return None

    if spec.partition_scope:
        combos = _collect_partition_combos(frame, spec.partition_scope)
        df = df.filter(_spark_partition_filter(combos, spec.partition_scope))

    return df


def _load_target(spark: SparkSession, target: ResolvedTarget) -> DataFrame | None:
    """Load target DataFrame from catalog or path. Returns None if not found."""
    if isinstance(target, CatalogTarget):
        if not spark.catalog.tableExists(target.catalog_ref.ref):
            _log.debug("Catalog table %s not found — first run", target.catalog_ref.ref)
            return None
        return spark.table(target.catalog_ref.ref)

    try:
        return spark.read.format("delta").load(target.location.uri)
    except AnalysisException:
        _log.debug("Delta table not found at %s — first run", target.location.uri)
        return None


def _collect_partition_combos(
    frame: DataFrame,
    partition_scope: tuple[str, ...],
) -> list[dict[str, Any]]:
    """Collect distinct partition value combinations from the incoming frame."""
    rows = frame.select(list(partition_scope)).distinct().collect()
    return [{c: row[c] for c in partition_scope} for row in rows]


def _spark_partition_filter(
    combos: list[dict[str, Any]],
    partition_cols: tuple[str, ...],
) -> Any:
    """Build a Spark Column filter expression from partition value combinations."""
    _ = partition_cols
    if not combos:
        return F.lit(False)
    and_exprs = [
        reduce(lambda a, b: a & b, [F.col(k) == v for k, v in combo.items()]) for combo in combos
    ]
    return reduce(lambda a, b: a | b, and_exprs)


def _write_result(
    frame: DataFrame,
    result: DataFrame,
    target: ResolvedTarget,
    spec: HistorifySpec,
) -> None:
    """Write SCD2 result — replaceWhere on partition scope or full overwrite."""
    writer = result.write.format("delta").mode("overwrite")

    if spec.partition_scope:
        combos = _collect_partition_combos(frame, spec.partition_scope)
        predicate = _build_partition_predicate(combos, spec.partition_scope)
        writer = writer.option("replaceWhere", predicate)

    if isinstance(target, CatalogTarget):
        writer.saveAsTable(target.catalog_ref.ref)
    else:
        writer.save(target.location.uri)


# ---------------------------------------------------------------------------
# Bootstrap (first run)
# ---------------------------------------------------------------------------


def _first_run(
    frame: DataFrame,
    spec: HistorifySpec,
    join_key: list[str],
    eff_date: Any,
) -> DataFrame:
    """Build the initial history frame when the target table is empty."""
    if spec.mode is HistorifyInputMode.SNAPSHOT:
        result = _stamp_new_rows(frame, spec, eff_date)
        return _ensure_soft_delete_col(result, spec)
    return _build_log_history(frame, spec, join_key)


# ---------------------------------------------------------------------------
# Temporal guard
# ---------------------------------------------------------------------------


def _temporal_guard(
    existing: DataFrame,
    spec: HistorifySpec,
    eff_date: Any,
) -> None:
    """Raise if future-open records exist and re-weave is disabled."""
    if spec.allow_temporal_rerun or spec.mode is HistorifyInputMode.LOG:
        return
    vf_dtype = _vf_dtype_sql(spec)
    conflict_agg = (
        existing.filter(F.col(spec.valid_from) > F.lit(eff_date).cast(vf_dtype))
        .agg(F.min(spec.valid_from).alias("min_vf"))
        .collect()
    )
    min_conflict = conflict_agg[0]["min_vf"]
    if min_conflict is None:
        return
    raise HistorifyTemporalConflictError(min_conflict, eff_date)


# ---------------------------------------------------------------------------
# Idempotency strip (SNAPSHOT only)
# ---------------------------------------------------------------------------


def _idempotency_strip(
    existing: DataFrame,
    spec: HistorifySpec,
    eff_date: Any,
    join_key: list[str],
) -> DataFrame:
    """Undo a previous run on the same eff_date.

    1. Remove rows with ``valid_from == eff_date`` (created by the prior run).
    2. Re-open the last row per ``join_key`` whose ``valid_to`` equals
       ``eff_date - 1`` — it was closed by the previous run.

    Args:
        existing:  Current Delta content for the relevant partitions.
        spec:      Historify spec.
        eff_date:  Effective date of the current run.
        join_key:  Resolved ``keys + track`` column list.

    Returns:
        Stripped DataFrame ready for SCD2 classification.
    """
    vf_dtype = _vf_dtype_sql(spec)
    prev = _prev_period_value(eff_date, spec)

    stripped = existing.filter(F.col(spec.valid_from) != F.lit(eff_date).cast(vf_dtype))

    window = Window.partitionBy(join_key).orderBy(F.col(spec.valid_from).desc())
    with_rank = stripped.withColumn("__rank__", F.rank().over(window))

    is_last_closed_by_run = (F.col("__rank__") == 1) & (
        F.col(spec.valid_to) == F.lit(prev).cast(vf_dtype)
    )
    return with_rank.withColumn(
        spec.valid_to,
        F.when(is_last_closed_by_run, F.lit(None).cast(vf_dtype)).otherwise(F.col(spec.valid_to)),
    ).drop("__rank__")


# ---------------------------------------------------------------------------
# SNAPSHOT mode
# ---------------------------------------------------------------------------


def _apply_snapshot(
    incoming: DataFrame,
    existing: DataFrame,
    spec: HistorifySpec,
    join_key: list[str],
    eff_date: Any,
) -> DataFrame:
    """Apply SNAPSHOT SCD2 classification via anti/semi-joins on join_key.

    * New       — in incoming, not in open existing -> stamp + insert.
    * Unchanged — in both -> keep open existing row as-is.
    * Deleted   — in open existing, not in incoming -> apply delete policy.

    Closed historical rows are always preserved unchanged.
    """
    open_existing = existing.filter(F.col(spec.valid_to).isNull())
    closed_existing = existing.filter(F.col(spec.valid_to).isNotNull())

    open_keys = open_existing.select(join_key)
    incoming_keys = incoming.select(join_key)

    new_raw = incoming.join(open_keys, on=join_key, how="left_anti")
    new_rows = _stamp_new_rows(new_raw, spec, eff_date)

    deleted = open_existing.join(incoming_keys, on=join_key, how="left_anti")
    closed_deleted = _apply_delete_policy(deleted, spec, eff_date)

    unchanged = open_existing.join(incoming_keys, on=join_key, how="left_semi")

    result = (
        closed_existing.unionByName(unchanged, allowMissingColumns=True)
        .unionByName(closed_deleted, allowMissingColumns=True)
        .unionByName(new_rows, allowMissingColumns=True)
    )
    return _ensure_soft_delete_col(result, spec)


def _stamp_new_rows(rows: DataFrame, spec: HistorifySpec, eff_date: Any) -> DataFrame:
    """Add ``valid_from = eff_date`` and ``valid_to = NULL`` to new rows."""
    vf_dtype = _vf_dtype_sql(spec)
    return rows.withColumn(spec.valid_from, F.lit(eff_date).cast(vf_dtype)).withColumn(
        spec.valid_to, F.lit(None).cast(vf_dtype)
    )


def _apply_delete_policy(
    deleted: DataFrame,
    spec: HistorifySpec,
    eff_date: Any,
) -> DataFrame:
    """Close or annotate rows absent from the incoming snapshot."""
    if spec.delete_policy is DeletePolicy.IGNORE:
        return deleted

    vf_dtype = _vf_dtype_sql(spec)
    prev = _prev_period_value(eff_date, spec)
    result = deleted.withColumn(spec.valid_to, F.lit(prev).cast(vf_dtype))

    if spec.delete_policy is DeletePolicy.SOFT_DELETE:
        result = result.withColumn("deleted_at", F.lit(eff_date).cast(vf_dtype))

    return result


def _ensure_soft_delete_col(df: DataFrame, spec: HistorifySpec) -> DataFrame:
    """Add ``deleted_at = NULL`` to all rows when SOFT_DELETE and col absent.

    Keeps Delta schema stable across runs — avoids SchemaMismatchError when
    some runs produce deletions and others do not.
    """
    if spec.delete_policy is not DeletePolicy.SOFT_DELETE:
        return df
    if "deleted_at" in df.columns:
        return df
    vf_dtype = _vf_dtype_sql(spec)
    return df.withColumn("deleted_at", F.lit(None).cast(vf_dtype))


# ---------------------------------------------------------------------------
# LOG mode
# ---------------------------------------------------------------------------


def _apply_log(
    frame: DataFrame,
    existing: DataFrame,
    spec: HistorifySpec,
    join_key: list[str],
) -> DataFrame:
    """Apply LOG mode — rebuild history for entities present in incoming.

    Entities not in ``frame`` are preserved untouched.  For affected
    entities, history is rebuilt from the union of existing events
    (extracted from ``valid_from``) and incoming events.  Incoming events
    win on same-date conflicts.
    """
    eff_col = str(spec.effective_date)
    entity_keys = list(spec.keys)
    affected_keys = frame.select(entity_keys).distinct()

    untouched = existing.join(affected_keys, on=entity_keys, how="left_anti")

    affected_existing = existing.join(affected_keys, on=entity_keys, how="left_semi")
    existing_events = affected_existing.withColumnRenamed(spec.valid_from, eff_col).drop(
        spec.valid_to
    )

    all_events = existing_events.unionByName(frame, allowMissingColumns=True)

    combo_key = join_key + [eff_col]
    window_dedup = Window.partitionBy(combo_key).orderBy(F.lit(1).desc())
    deduped = (
        all_events.withColumn("__rn__", F.row_number().over(window_dedup))
        .filter(F.col("__rn__") == 1)
        .drop("__rn__")
    )

    rebuilt = _build_log_history(deduped, spec, join_key)
    return untouched.unionByName(rebuilt, allowMissingColumns=True)


def _build_log_history(
    frame: DataFrame,
    spec: HistorifySpec,
    join_key: list[str],
) -> DataFrame:
    """Compute ``valid_from``/``valid_to`` from a sorted event frame.

    Window is over ``spec.keys`` (entity key, not join_key): LOG mode models
    sequential state transitions per entity — the close date is determined by
    the next event for that entity regardless of track values.
    """
    eff_col = str(spec.effective_date)
    entity_key = list(spec.keys)
    vf_dtype = _vf_dtype_sql(spec)

    window = Window.partitionBy(entity_key).orderBy(eff_col)
    next_date = F.lead(eff_col).over(window)

    if spec.date_type is HistoryDateType.DATE:
        vto_expr = F.date_sub(next_date, 1)
    else:
        vto_expr = next_date - F.expr("INTERVAL 1 MICROSECOND")

    return (
        frame.withColumn(spec.valid_from, F.col(eff_col).cast(vf_dtype))
        .withColumn(spec.valid_to, vto_expr.cast(vf_dtype))
        .drop(eff_col)
    )


# ---------------------------------------------------------------------------
# Type / date helpers
# ---------------------------------------------------------------------------


def _vf_dtype_sql(spec: HistorifySpec) -> str:
    """Return the Spark SQL type string for valid_from / valid_to columns."""
    return "date" if spec.date_type is HistoryDateType.DATE else "timestamp"


def _prev_period_value(eff_date: Any, spec: HistorifySpec) -> Any:
    """Return one unit before eff_date (one day or one microsecond)."""
    delta = _DATE_DELTA if spec.date_type is HistoryDateType.DATE else _TS_DELTA
    return eff_date - delta


__all__ = ["SparkHistorifyEngine"]
