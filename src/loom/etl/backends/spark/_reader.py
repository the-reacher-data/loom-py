"""SparkDeltaReader — SourceReader backed by PySpark + Delta Lake.

Two resolution modes controlled by the *locator* constructor argument:

* **Path-based** (``locator`` is a URI string, :class:`pathlib.Path`, or
  :class:`~loom.etl._locator.TableLocator`) — reads via
  ``spark.read.format("delta").load(uri)``.  Works with any cloud storage
  that Spark can reach: S3, GCS, ADLS, DBFS, local.

* **Unity Catalog** (``locator=None``) — reads via
  ``spark.table(ref.ref)``, delegating all path and credential resolution
  to the active Spark catalog.  The :class:`~loom.etl._table.TableRef` is
  used as the fully-qualified table name
  (e.g. ``"main.raw.orders"`` or ``"raw.orders"``).

See https://docs.databricks.com/en/data-governance/unity-catalog/index.html
"""

from __future__ import annotations

import logging
import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from loom.etl.backends.spark._dtype import loom_type_to_spark
from loom.etl.io._source import JsonColumnSpec, SourceSpec
from loom.etl.schema._schema import ColumnSchema
from loom.etl.storage._locator import TableLocator, _as_locator

_log = logging.getLogger(__name__)


class SparkDeltaReader:
    """Read ETL sources as Spark DataFrames from Delta tables.

    Implements :class:`~loom.etl._io.SourceReader`.

    Spark DataFrames are lazy by default — the scan is not materialised
    until the step's ``execute()`` result is written by the writer.

    Args:
        spark:   Active :class:`pyspark.sql.SparkSession`.
        locator: How to resolve table references to a physical location.

                 * Pass a URI string, :class:`pathlib.Path`, or any
                   :class:`~loom.etl._locator.TableLocator` for path-based
                   reads — e.g. ``"s3://my-lake/"`` or
                   ``PrefixLocator("abfss://container@account.dfs.core.windows.net/")``.
                 * Pass ``None`` (default) for Unity Catalog — table refs are
                   resolved directly via ``spark.table()``.

    Example::

        from loom.etl.backends.spark import SparkDeltaReader

        # Unity Catalog (Databricks managed)
        reader = SparkDeltaReader(spark)

        # Cloud path (S3, GCS, ADLS, DBFS)
        reader = SparkDeltaReader(spark, "s3://my-lake/")

        # With explicit credentials
        from loom.etl.storage._locator import PrefixLocator
        reader = SparkDeltaReader(spark, PrefixLocator("s3://my-lake/", storage_options={...}))
    """

    def __init__(
        self,
        spark: SparkSession,
        locator: str | os.PathLike[str] | TableLocator | None = None,
    ) -> None:
        self._spark = spark
        self._locator = _as_locator(locator) if locator is not None else None

    def read(self, spec: SourceSpec, _params_instance: Any) -> DataFrame:
        """Return a lazy Spark DataFrame backed by the Delta table in *spec*.

        Args:
            spec:             Compiled source spec.  Must be a TABLE source.
            _params_instance: Concrete params (reserved for future predicate
                              pushdown support).

        Returns:
            Spark DataFrame over the Delta table (lazy scan).

        Raises:
            TypeError: If *spec* is a FILE source (unsupported here).
        """
        if spec.table_ref is None:
            raise TypeError(f"SparkDeltaReader only supports TABLE sources; got FILE spec: {spec}")
        if self._locator is None:
            df = self._spark.table(spec.table_ref.ref)
        else:
            loc = self._locator.locate(spec.table_ref)
            df = self._spark.read.format("delta").load(loc.uri)
        if spec.columns:
            _log.debug(
                "read spark table=%s columns=%d schema_cols=%d",
                spec.table_ref.ref,
                len(spec.columns),
                len(spec.schema),
            )
            df = df.select(list(spec.columns))
        df = _apply_source_schema_spark(df, spec.schema)
        return _apply_json_decode_spark(df, spec.json_columns)


def _apply_source_schema_spark(df: DataFrame, schema: tuple[ColumnSchema, ...]) -> DataFrame:
    """Cast declared source columns to their Spark type equivalents.

    Only columns declared in *schema* are cast; all other columns pass
    through unchanged.  The cast is applied lazily — no Spark action is
    triggered.

    Args:
        df:     Source DataFrame.
        schema: Tuple of :class:`~loom.etl.schema._schema.ColumnSchema` entries
                declared via ``.with_schema()`` on the source spec.

    Returns:
        Original DataFrame when *schema* is empty; otherwise a new DataFrame
        with ``withColumn`` cast expressions for the declared columns.
    """
    if not schema:
        return df
    for col in schema:
        df = df.withColumn(col.name, F.col(col.name).cast(loom_type_to_spark(col.dtype)))
    return df


def _apply_json_decode_spark(df: DataFrame, json_columns: tuple[JsonColumnSpec, ...]) -> DataFrame:
    """Decode JSON string columns using Spark ``from_json``.

    Each entry in *json_columns* replaces the named string column with a
    structured value decoded from its JSON content.

    Args:
        df:           Source DataFrame.
        json_columns: JSON column specs from the source spec.

    Returns:
        Original DataFrame when *json_columns* is empty; otherwise a new
        DataFrame with ``from_json`` applied for the declared columns.
    """
    if not json_columns:
        return df
    for jc in json_columns:
        schema_ddl: str = loom_type_to_spark(jc.loom_type).simpleString()
        df = df.withColumn(jc.column, F.from_json(F.col(jc.column), schema_ddl))
    return df
