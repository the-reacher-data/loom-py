"""Polars SCD Type 2 historify engine.

Implements a partition-replace strategy for Delta tables:
read relevant partitions -> apply SCD2 logic in memory -> write back
with ``replaceWhere`` to atomically replace only those partitions.

Supported features:
- SNAPSHOT and LOG input modes.
- Delete policies: IGNORE, CLOSE, SOFT_DELETE.
- Same-day re-run idempotency (forward runs).
- Temporal conflict guard for backfill detection.
- Partition-scoped reads and writes.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

import polars as pl
from deltalake import DeltaTable, write_deltalake

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

_log = logging.getLogger(__name__)

_DATE_DELTA = timedelta(days=1)
_TS_DELTA = timedelta(microseconds=1)

_LOG_CLOSE_OFFSET: dict[HistoryDateType, pl.Expr] = {
    HistoryDateType.DATE: pl.duration(days=1),
    HistoryDateType.TIMESTAMP: pl.duration(microseconds=1),
}


class PolarsHistorifyEngine:
    """SCD Type 2 engine for Polars + Delta tables.

    Partition-replace strategy: reads only the relevant partitions,
    applies the SCD2 algorithm in memory, then commits via Delta
    ``replaceWhere``.  When ``partition_scope`` is ``None`` the engine
    reads and overwrites the entire table.

    Example::

        engine = PolarsHistorifyEngine()
        engine.apply(frame=df, uri="s3://bucket/wh/dim_players",
                     storage_options=None, spec=spec, params_instance=params)
    """

    def apply(
        self,
        frame: pl.DataFrame,
        uri: str,
        storage_options: dict[str, str] | None,
        spec: HistorifySpec,
        params_instance: object,
    ) -> HistorifyRepairReport | None:
        """Apply SCD2 and commit result to the Delta target.

        Args:
            frame:           Materialized incoming DataFrame.
            uri:             Delta table URI.
            storage_options: Object-store credentials, or ``None`` for local.
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

        existing = _read_existing(frame, uri, storage_options, spec)

        if existing is None or existing.is_empty():
            result = _first_run(frame, spec, join_key, eff_date)
            _write_result(frame, result, uri, storage_options, spec)
            return None

        _temporal_guard(existing, spec, eff_date)

        if spec.mode is HistorifyInputMode.SNAPSHOT:
            stripped = _idempotency_strip(existing, spec, eff_date, join_key)
            result = _apply_snapshot(frame, stripped, spec, join_key, eff_date)
        else:
            result = _apply_log(frame, existing, spec, join_key)

        _write_result(frame, result, uri, storage_options, spec)
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


def _assert_unique_entity_state(frame: pl.DataFrame, join_key: list[str]) -> None:
    """Raise HistorifyKeyConflictError on duplicate (keys + track) combos."""
    if len(frame) == frame.select(join_key).n_unique():
        return
    dupes = frame.filter(frame.select(join_key).is_duplicated()).select(join_key).head(5)
    raise HistorifyKeyConflictError(str(dupes.to_dicts()))


def _assert_no_date_collisions(
    frame: pl.DataFrame,
    join_key: list[str],
    eff_col: str,
    spec: HistorifySpec,
) -> None:
    """Raise HistorifyDateCollisionError on same (join_key, date) duplicates."""
    if spec.date_type is not HistoryDateType.DATE:
        return
    combo_key = join_key + [eff_col]
    if len(frame) == frame.select(combo_key).n_unique():
        return
    dupes = frame.filter(frame.select(combo_key).is_duplicated()).select(combo_key).head(5)
    raise HistorifyDateCollisionError(str(dupes.to_dicts()))


# ---------------------------------------------------------------------------
# Delta I/O
# ---------------------------------------------------------------------------


def _read_existing(
    frame: pl.DataFrame,
    uri: str,
    storage_options: dict[str, str] | None,
    spec: HistorifySpec,
) -> pl.DataFrame | None:
    """Read existing Delta data, partition-filtered when scope is declared.

    Returns:
        Existing DataFrame, or ``None`` when the table does not exist yet.
    """
    try:
        DeltaTable(uri, storage_options=storage_options or {})
    except Exception:
        _log.debug("Delta table not found at %s — first run", uri)
        return None

    lf = pl.scan_delta(uri, storage_options=storage_options or {})

    if spec.partition_scope:
        combos = frame.select(list(spec.partition_scope)).unique().to_dicts()
        lf = lf.filter(_polars_partition_filter(combos, spec.partition_scope))

    return lf.collect()


def _polars_partition_filter(
    combos: list[dict[str, Any]],
    partition_cols: tuple[str, ...],
) -> pl.Expr:
    """Build a Polars filter expression from partition value combinations."""
    if not combos:
        return pl.lit(False)
    or_parts = [pl.all_horizontal([pl.col(k) == v for k, v in combo.items()]) for combo in combos]
    return pl.any_horizontal(or_parts)


def _write_result(
    frame: pl.DataFrame,
    result: pl.DataFrame,
    uri: str,
    storage_options: dict[str, str] | None,
    spec: HistorifySpec,
) -> None:
    """Write SCD2 result — replaceWhere on partition scope or full overwrite."""
    kwargs: dict[str, Any] = {"storage_options": storage_options or None}

    if spec.partition_scope:
        combos = list(frame.select(list(spec.partition_scope)).unique().iter_rows(named=True))
        predicate = _build_partition_predicate(combos, spec.partition_scope)
        write_deltalake(uri, result, mode="overwrite", predicate=predicate, **kwargs)
    else:
        write_deltalake(uri, result, mode="overwrite", **kwargs)


# ---------------------------------------------------------------------------
# Bootstrap (first run)
# ---------------------------------------------------------------------------


def _first_run(
    frame: pl.DataFrame,
    spec: HistorifySpec,
    join_key: list[str],
    eff_date: Any,
) -> pl.DataFrame:
    """Build the initial history frame when the target table is empty."""
    if spec.mode is HistorifyInputMode.SNAPSHOT:
        result = _stamp_new_rows(frame, spec, eff_date)
        return _ensure_soft_delete_col(result, spec)
    return _build_log_history(frame, spec, join_key)


# ---------------------------------------------------------------------------
# Temporal guard
# ---------------------------------------------------------------------------


def _temporal_guard(
    existing: pl.DataFrame,
    spec: HistorifySpec,
    eff_date: Any,
) -> None:
    """Raise if future-open records exist and re-weave is disabled."""
    if spec.allow_temporal_rerun or spec.mode is HistorifyInputMode.LOG:
        return
    vf_dtype = _vf_dtype(spec)
    conflict = existing.filter(pl.col(spec.valid_from) > pl.lit(eff_date).cast(vf_dtype))
    if conflict.is_empty():
        return
    raise HistorifyTemporalConflictError(conflict[spec.valid_from].min(), eff_date)


# ---------------------------------------------------------------------------
# Idempotency strip (SNAPSHOT only)
# ---------------------------------------------------------------------------


def _idempotency_strip(
    existing: pl.DataFrame,
    spec: HistorifySpec,
    eff_date: Any,
    join_key: list[str],
) -> pl.DataFrame:
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
    vf_dtype = _vf_dtype(spec)
    prev = _prev_period(eff_date, spec)

    stripped = existing.filter(pl.col(spec.valid_from) != pl.lit(eff_date).cast(vf_dtype))
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


# ---------------------------------------------------------------------------
# SNAPSHOT mode
# ---------------------------------------------------------------------------


def _apply_snapshot(
    incoming: pl.DataFrame,
    existing: pl.DataFrame,
    spec: HistorifySpec,
    join_key: list[str],
    eff_date: Any,
) -> pl.DataFrame:
    """Apply SNAPSHOT SCD2 classification via anti-joins on join_key.

    * New       — in incoming, not in open existing -> stamp + insert.
    * Unchanged — in both -> keep open existing row as-is.
    * Deleted   — in open existing, not in incoming -> apply delete policy.

    Closed historical rows are always preserved unchanged.
    """
    open_existing = existing.filter(pl.col(spec.valid_to).is_null())
    closed_existing = existing.filter(pl.col(spec.valid_to).is_not_null())

    open_keys = open_existing.select(join_key)
    incoming_keys = incoming.select(join_key)

    new_raw = incoming.join(open_keys, on=join_key, how="anti")
    new_rows = _stamp_new_rows(new_raw, spec, eff_date)

    deleted = open_existing.join(incoming_keys, on=join_key, how="anti")
    closed_deleted = _apply_delete_policy(deleted, spec, eff_date)

    unchanged = open_existing.join(incoming_keys, on=join_key, how="semi")

    result = pl.concat(
        [closed_existing, unchanged, closed_deleted, new_rows], how="diagonal_relaxed"
    )
    return _ensure_soft_delete_col(result, spec)


def _ensure_soft_delete_col(result: pl.DataFrame, spec: HistorifySpec) -> pl.DataFrame:
    """Add ``deleted_at = NULL`` to all rows if policy is SOFT_DELETE and col absent.

    Keeps schema stable across runs so Delta does not reject the write when
    some run produces deleted rows (adding the column) and others do not.
    """
    if spec.delete_policy is not DeletePolicy.SOFT_DELETE:
        return result
    if "deleted_at" in result.columns:
        return result
    vf_dtype = _vf_dtype(spec)
    return result.with_columns(pl.lit(None, dtype=vf_dtype).alias("deleted_at"))


def _stamp_new_rows(rows: pl.DataFrame, spec: HistorifySpec, eff_date: Any) -> pl.DataFrame:
    """Add ``valid_from = eff_date`` and ``valid_to = NULL`` to new rows."""
    vf_dtype = _vf_dtype(spec)
    return rows.with_columns(
        pl.lit(eff_date).cast(vf_dtype).alias(spec.valid_from),
        pl.lit(None, dtype=vf_dtype).alias(spec.valid_to),
    )


def _apply_delete_policy(
    deleted: pl.DataFrame,
    spec: HistorifySpec,
    eff_date: Any,
) -> pl.DataFrame:
    """Close or annotate rows absent from the incoming snapshot."""
    if spec.delete_policy is DeletePolicy.IGNORE:
        return deleted

    vf_dtype = _vf_dtype(spec)
    prev = _prev_period(eff_date, spec)
    result = deleted.with_columns(pl.lit(prev).cast(vf_dtype).alias(spec.valid_to))

    if spec.delete_policy is DeletePolicy.SOFT_DELETE:
        result = result.with_columns(pl.lit(eff_date).cast(vf_dtype).alias("deleted_at"))

    return result


# ---------------------------------------------------------------------------
# LOG mode
# ---------------------------------------------------------------------------


def _apply_log(
    frame: pl.DataFrame,
    existing: pl.DataFrame,
    spec: HistorifySpec,
    join_key: list[str],
) -> pl.DataFrame:
    """Apply LOG mode — rebuild history for entities present in incoming.

    Entities not in ``frame`` are preserved untouched.  For affected
    entities, history is rebuilt from the union of existing events
    (extracted from ``valid_from``) and incoming events.  Incoming events
    win on same-date conflicts.
    """
    eff_col = str(spec.effective_date)
    entity_keys = list(spec.keys)
    affected_keys = frame.select(entity_keys).unique()

    untouched = existing.join(affected_keys, on=entity_keys, how="anti")

    affected_existing = existing.join(affected_keys, on=entity_keys, how="semi")
    existing_events = affected_existing.rename({spec.valid_from: eff_col}).drop(spec.valid_to)

    all_events = pl.concat([existing_events, frame], how="diagonal_relaxed")
    deduped = all_events.unique(subset=join_key + [eff_col], keep="last")

    rebuilt = _build_log_history(deduped, spec, join_key)
    return pl.concat([untouched, rebuilt], how="diagonal_relaxed")


def _build_log_history(
    frame: pl.DataFrame,
    spec: HistorifySpec,
    join_key: list[str],
) -> pl.DataFrame:
    """Compute ``valid_from``/``valid_to`` from a sorted event frame.

    Sorts events by ``entity_key + eff_col``, then assigns:
    ``valid_from = eff_col``, ``valid_to = next_event - 1`` (NULL for last).

    The temporal window is computed per ``spec.keys`` (entity key), NOT per
    ``join_key``.  LOG mode models sequential state transitions for an entity:
    the close date of one state is the day before the next event for that
    entity, regardless of which track column value the next event carries.
    """
    eff_col = str(spec.effective_date)
    entity_key = list(spec.keys)
    vf_dtype = _vf_dtype(spec)
    offset = _LOG_CLOSE_OFFSET[spec.date_type]

    sorted_events = frame.sort(entity_key + [eff_col])
    next_date = pl.col(eff_col).shift(-1).over(entity_key)

    return sorted_events.with_columns(
        pl.col(eff_col).cast(vf_dtype).alias(spec.valid_from),
        (next_date - offset).cast(vf_dtype).alias(spec.valid_to),
    ).drop(eff_col)


# ---------------------------------------------------------------------------
# Type / date helpers
# ---------------------------------------------------------------------------


def _vf_dtype(spec: HistorifySpec) -> type[pl.Date] | type[pl.Datetime]:
    """Return the Polars dtype for valid_from / valid_to columns."""
    return pl.Date if spec.date_type is HistoryDateType.DATE else pl.Datetime


def _prev_period(eff_date: Any, spec: HistorifySpec) -> Any:
    """Return one unit before eff_date (one day or one microsecond)."""
    delta = _DATE_DELTA if spec.date_type is HistoryDateType.DATE else _TS_DELTA
    return eff_date - delta


__all__ = ["PolarsHistorifyEngine"]
