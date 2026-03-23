"""SparkDeltaReader — SourceReader backed by PySpark + Delta Lake."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from loom.etl._source import SourceSpec
from loom.etl._table import TableRef


class SparkDeltaReader:
    """Read ETL sources as Spark DataFrames from Delta tables.

    Implements :class:`~loom.etl._io.SourceReader`.

    Spark DataFrames are lazy by default — the scan is not materialised
    until the step's ``execute()`` result is written by the writer.

    Args:
        spark: Active :class:`pyspark.sql.SparkSession`.
        root:  Filesystem root.  Table paths are resolved as
               ``root/<schema>/<table>/``.

    Example::

        reader = SparkDeltaReader(spark, Path("/data/delta"))
        frame = reader.read(spec, params)  # returns pyspark.sql.DataFrame
    """

    def __init__(self, spark: SparkSession, root: Path) -> None:
        self._spark = spark
        self._root = root

    def read(self, spec: SourceSpec, _params_instance: Any) -> DataFrame:
        """Return a lazy Spark DataFrame backed by the Delta table in *spec*.

        Args:
            spec:             Compiled source spec.  Must be a TABLE source.
            _params_instance: Concrete params (unused — predicate pushdown
                              not yet implemented).

        Returns:
            Spark DataFrame over the Delta table (lazy scan).

        Raises:
            AssertionError: If *spec* is a FILE source (unsupported here).
        """
        assert spec.table_ref is not None, (
            f"SparkDeltaReader only supports TABLE sources; got FILE spec: {spec}"
        )
        path = self._table_path(spec.table_ref)
        return self._spark.read.format("delta").load(str(path))

    def _table_path(self, ref: TableRef) -> Path:
        return self._root.joinpath(*ref.ref.split("."))
