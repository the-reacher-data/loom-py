"""Shared helpers for UPSERT/MERGE predicate and column-set construction.

Pure functions — no I/O, no backend dependency.  Used by both the Polars
and Spark writers to build the Delta MERGE ``ON`` predicate and the update /
insert column maps.

Pre-filter strategy
-------------------
Without a partition pre-filter the MERGE engine must scan the entire target
table to evaluate the join condition.  By prepending a literal partition
pre-filter we give Delta the information it needs to prune files at the log
level before the join runs::

    ((t.year=2023 AND t.month=1) OR (t.year=2024 AND t.month=3))
    AND (t.id = s.id AND t.year = s.year AND t.month = s.month)

The partition combos are collected from the source frame *before* the MERGE
call (Polars: ``df.unique(subset=…).to_dicts()``; Spark:
``df.select(…).distinct().collect()``).  Partition columns always have low
cardinality so the collect is cheap.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from typing import Any

from loom.etl.io._target import TargetSpec
from loom.etl.sql.literals import sql_literal

_log = logging.getLogger(__name__)

TARGET_ALIAS = "t"
SOURCE_ALIAS = "s"
_SQL_AND = " AND "


# ---------------------------------------------------------------------------
# Predicate construction
# ---------------------------------------------------------------------------


def _build_join_clause(
    upsert_keys: tuple[str, ...],
    partition_cols: tuple[str, ...],
    target_alias: str,
    source_alias: str,
) -> str:
    """Build the equality join clause from upsert keys and partition columns.

    Args:
        upsert_keys:    Columns that uniquely identify a row.
        partition_cols: Partition columns (joined on as well).
        target_alias:   Alias for the target table in the MERGE statement.
        source_alias:   Alias for the source frame in the MERGE statement.

    Returns:
        SQL string, e.g. ``t.id = s.id AND t.year = s.year``
    """
    all_cols = (*upsert_keys, *partition_cols)
    return _SQL_AND.join(f"{target_alias}.{col} = {source_alias}.{col}" for col in all_cols)


def _build_single_partition_combo_clause(
    combo: dict[str, Any],
    partition_cols: tuple[str, ...],
    target_alias: str,
) -> str:
    """Build a literal equality clause for one partition value combination.

    Args:
        combo:          Dict of partition column → literal value for one row.
        partition_cols: Ordered partition column names.
        target_alias:   Target table alias.

    Returns:
        SQL string, e.g. ``(t.year = 2023 AND t.month = 1)``
    """
    parts = _SQL_AND.join(
        f"{target_alias}.{col} = {sql_literal(combo[col])}" for col in partition_cols
    )
    return f"({parts})"


def _build_partition_literal_filter(
    combos: list[dict[str, Any]],
    partition_cols: tuple[str, ...],
    target_alias: str,
) -> str:
    """Build a partition pre-filter from distinct partition value combinations.

    Args:
        combos:         List of dicts, one per distinct partition combination.
        partition_cols: Ordered partition column names.
        target_alias:   Target table alias.

    Returns:
        SQL string, e.g.
        ``(t.year=2023 AND t.month=1) OR (t.year=2024 AND t.month=3)``
    """
    clauses = [
        _build_single_partition_combo_clause(combo, partition_cols, target_alias)
        for combo in combos
    ]
    return " OR ".join(clauses)


def _build_upsert_predicate(
    combos: list[dict[str, Any]],
    spec: TargetSpec,
    target_alias: str,
    source_alias: str,
) -> str:
    """Build the full MERGE ON predicate.

    When partition combos are present, prepends a literal partition pre-filter
    so Delta can prune files at the log level before evaluating the join.

    Args:
        combos:       Distinct partition value combinations from the source frame.
                      Pass an empty list when no partition columns are declared.
        spec:         Target spec carrying ``upsert_keys`` and ``partition_cols``.
        target_alias: Target table alias.
        source_alias: Source frame alias.

    Returns:
        SQL predicate string for the MERGE ON clause.
    """
    join_clause = _build_join_clause(
        spec.upsert_keys, spec.partition_cols, target_alias, source_alias
    )
    if not combos or not spec.partition_cols:
        return join_clause
    partition_filter = _build_partition_literal_filter(combos, spec.partition_cols, target_alias)
    return f"({partition_filter}) AND ({join_clause})"


# ---------------------------------------------------------------------------
# Column-set construction
# ---------------------------------------------------------------------------


def _build_upsert_update_cols(
    df_columns: tuple[str, ...],
    spec: TargetSpec,
) -> tuple[str, ...]:
    """Compute the columns to update on MATCH, respecting exclude/include.

    Keys and partition columns are always excluded from UPDATE SET — updating
    them would invalidate the match condition.

    If ``upsert_include`` is set, only those columns are updated (minus the
    always-excluded set).  If ``upsert_exclude`` is set, those additional
    columns are skipped.  The two options are mutually exclusive.

    Args:
        df_columns: Column names from the source frame, in original order.
        spec:       Target spec carrying ``upsert_keys``, ``partition_cols``,
                    ``upsert_exclude``, and ``upsert_include``.

    Returns:
        Ordered tuple of column names to appear in UPDATE SET.
    """
    always_excluded = frozenset(spec.upsert_keys) | frozenset(spec.partition_cols)
    if spec.upsert_include:
        return tuple(c for c in spec.upsert_include if c not in always_excluded)
    candidates = tuple(c for c in df_columns if c not in always_excluded)
    if spec.upsert_exclude:
        extra_excluded = frozenset(spec.upsert_exclude)
        return tuple(c for c in candidates if c not in extra_excluded)
    return candidates


def _build_update_set(
    update_cols: tuple[str, ...],
    source_alias: str,
) -> dict[str, str]:
    """Map each update column to its source-aliased expression.

    Args:
        update_cols:  Columns to include in UPDATE SET.
        source_alias: Source frame alias (e.g. ``"s"``).

    Returns:
        Dict mapping column name → ``"s.column"`` expression,
        e.g. ``{"name": "s.name", "value": "s.value"}``.
    """
    return {col: f"{source_alias}.{col}" for col in update_cols}


def _build_insert_values(
    all_columns: tuple[str, ...],
    source_alias: str,
) -> dict[str, str]:
    """Map every source column to its alias expression for NOT MATCHED INSERT.

    Args:
        all_columns:  All column names from the source frame.
        source_alias: Source frame alias (e.g. ``"s"``).

    Returns:
        Dict mapping column name → ``"s.column"`` expression.
    """
    return {col: f"{source_alias}.{col}" for col in all_columns}


# ---------------------------------------------------------------------------
# Observability helpers
# ---------------------------------------------------------------------------


def _warn_no_partition_cols(table_ref: str) -> None:
    """Emit a WARNING when no partition columns are declared on an UPSERT.

    Without partition columns the MERGE predicate cannot prune Delta log files,
    forcing a full table scan.  For large tables this can be very expensive.

    Args:
        table_ref: Logical table reference string (for the log message).
    """
    _log.warning(
        "upsert table=%s has no partition_cols — full table scan on MERGE. "
        "Declare partition_cols= to enable Delta log file pruning.",
        table_ref,
    )


def _log_partition_combos(
    combos: list[dict[str, Any]],
    table_ref: str,
) -> None:
    """Log the number of distinct partition combinations that will be merged.

    Args:
        combos:    List of partition combination dicts collected from the frame.
        table_ref: Logical table reference string.
    """
    _log.debug("upsert table=%s partition_combos=%d", table_ref, len(combos))


def _build_partition_predicate(
    rows: Iterable[Mapping[str, Any]],
    partition_cols: tuple[str, ...],
) -> str:
    """Build a ``replaceWhere`` SQL predicate from distinct partition rows.

    Backend-agnostic: callers extract the distinct rows from their DataFrame
    and pass them as an ``Iterable[Mapping[str, Any]]``.

    Args:
        rows:           Distinct partition-column rows (each a mapping of
                        column name → value).
        partition_cols: Ordered partition column names.

    Returns:
        SQL predicate string, e.g.
        ``(year = 2024 AND month = 1) OR (year = 2024 AND month = 2)``.
    """
    clauses = [
        _SQL_AND.join(f"{col} = {sql_literal(row[col])}" for col in partition_cols) for row in rows
    ]
    return " OR ".join(f"({c})" for c in clauses)
