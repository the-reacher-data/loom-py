"""Spark Parquet backend for IntermediateStore.

Internal module — not exported from the Spark backend package.
"""

from __future__ import annotations

import logging
from typing import Any

from loom.etl.temp._store import _join_path

_log = logging.getLogger(__name__)


class _SparkTempBackend:
    """PySpark Parquet backend for :class:`~loom.etl.temp._store.IntermediateStore`.

    Writes Spark DataFrames as Parquet directories via ``df.write.parquet()``
    and reads them back via ``spark.read.parquet()``.  Cuts the lineage DAG;
    Photon-optimised.  Avoids competing with the shuffle memory pool that
    ``df.cache()`` would enter.

    Args:
        spark: Active :class:`pyspark.sql.SparkSession`.
    """

    def __init__(self, spark: Any) -> None:
        self._spark = spark

    def probe(self, name: str, base: str) -> Any | None:
        """Return a ``DataFrame`` reading *name* under *base*, or ``None`` if absent.

        Args:
            name: Logical intermediate name.
            base: Scope-level base directory path.

        Returns:
            Spark DataFrame when found; ``None`` when the path does not exist.
        """
        return _probe_spark(self._spark, _join_path(base, name))

    def write(self, name: str, base: str, data: Any, *, append: bool) -> None:
        """Write *data* as a Parquet directory under *base*.

        Args:
            name:   Logical intermediate name.
            base:   Scope-level base directory path.
            data:   ``pyspark.sql.DataFrame`` to materialise.
            append: When ``True``, appends to any existing Parquet data for
                    this name (fan-in).  When ``False``, overwrites.

        Raises:
            TypeError: When *data* is not a ``pyspark.sql.DataFrame``.
        """
        if not _is_spark_dataframe(data):
            raise TypeError(
                f"Spark backend expects pyspark.sql.DataFrame, got {type(data).__qualname__!r}. "
                "Verify that IntermediateStore was constructed with a SparkSession."
            )
        path = _join_path(base, name)
        _log.debug("temp write parquet path=%s append=%s", path, append)
        data.write.mode("append" if append else "overwrite").parquet(path)


# ---------------------------------------------------------------------------
# Module-level helpers — no state, only used by _SparkTempBackend
# ---------------------------------------------------------------------------


def _is_spark_dataframe(obj: Any) -> bool:
    """Return ``True`` when *obj* is a ``pyspark.sql.DataFrame``."""
    t = type(obj)
    return "pyspark" in t.__module__ and t.__name__ == "DataFrame"


def _probe_spark(spark: Any, path: str) -> Any | None:
    """Read a Parquet path via *spark*; return ``None`` when the path does not exist.

    Catches ``pyspark.sql.utils.AnalysisException`` only — the specific Spark
    exception for missing paths and unresolvable table references.  Checked by
    module prefix and class name to avoid importing pyspark at module level
    (optional dependency).  Any other exception is re-raised.

    Args:
        spark: Active :class:`pyspark.sql.SparkSession`.
        path:  Parquet directory path to read.

    Returns:
        Spark DataFrame when the path exists; ``None`` on ``AnalysisException``.
    """
    try:
        return spark.read.parquet(path)
    except Exception as exc:
        t = type(exc)
        if t.__module__.startswith("pyspark") and t.__name__ == "AnalysisException":
            return None
        raise
