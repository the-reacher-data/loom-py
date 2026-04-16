"""Spark SCD Type 2 historify engine (legacy wrapper).

This module is preserved for backward compatibility with existing tests.
New code should use :class:`loom.etl.backends._historify.HistorifyEngine`
with :class:`SparkFrameOps`.
"""

from __future__ import annotations

import contextlib
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from loom.etl.backends._historify._common import (
    eval_param_expr as _eval_param_expr,
)
from loom.etl.backends._historify._common import (
    resolve_track_cols as _resolve_track_cols,
)
from loom.etl.backends._historify._engine import HistorifyEngine
from loom.etl.backends._merge import _build_partition_predicate
from loom.etl.backends.spark._ops import SparkFrameOps, _vf_dtype_sql
from loom.etl.backends.spark._writer import _collect_partition_combos
from loom.etl.declarative.target._history import (
    HistorifyRepairReport,
    HistorifySpec,
)
from loom.etl.storage.routing import PathTarget, ResolvedTarget


class SparkHistorifyEngine:
    """Legacy wrapper around :class:`HistorifyEngine` for Spark."""

    def __init__(self) -> None:
        self._engine = HistorifyEngine(SparkFrameOps())

    def apply(
        self,
        spark: SparkSession,
        frame: DataFrame,
        target: ResolvedTarget,
        spec: HistorifySpec,
        params_instance: object,
    ) -> HistorifyRepairReport | None:
        """Apply SCD2 and write result to Delta (legacy API)."""
        existing = None
        if isinstance(target, PathTarget):
            with contextlib.suppress(Exception):
                existing = spark.read.format("delta").load(target.location.uri)
        elif hasattr(target, "catalog_ref") and spark.catalog.tableExists(target.catalog_ref.ref):
            existing = spark.table(target.catalog_ref.ref)

        result = self._engine.transform(frame, existing, spec, params_instance)

        writer = result.write.format("delta").option("optimizeWrite", "true").mode("overwrite")
        if spec.partition_scope:
            combos = _collect_partition_combos(frame, spec.partition_scope, target.logical_ref.ref)
            predicate = _build_partition_predicate(combos, spec.partition_scope)
            writer = writer.option("replaceWhere", predicate)

        if isinstance(target, PathTarget):
            writer.save(target.location.uri)
        else:
            writer.saveAsTable(target.catalog_ref.ref)
        return None


# Re-export helpers for backward compatibility with tests.
def _stamp_new_rows(frame: DataFrame, spec: HistorifySpec, eff_date: Any) -> DataFrame:
    vf_dtype = _vf_dtype_sql(spec)
    return frame.withColumn(spec.valid_from, F.lit(eff_date).cast(vf_dtype)).withColumn(
        spec.valid_to, F.lit(None).cast(vf_dtype)
    )


_ops = SparkFrameOps()
_assert_unique_entity_state = _ops.assert_unique_keys
_assert_no_date_collisions = _ops.assert_no_date_collisions
_idempotency_strip = _ops.rollback_same_day_run
_resolve_track_cols = _resolve_track_cols
_eval_param_expr = _eval_param_expr

__all__ = [
    "SparkHistorifyEngine",
    "_assert_no_date_collisions",
    "_assert_unique_entity_state",
    "_eval_param_expr",
    "_idempotency_strip",
    "_resolve_track_cols",
    "_stamp_new_rows",
]
