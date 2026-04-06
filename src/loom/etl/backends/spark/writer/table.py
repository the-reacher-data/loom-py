"""Spark Delta table writer adapter."""

from __future__ import annotations

import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from loom.etl.backends.spark._writer import SparkDeltaWriter
from loom.etl.io.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpsertSpec,
)
from loom.etl.schema._table import TableRef
from loom.etl.storage._locator import TableLocator


class SparkDeltaTableWriter:
    """Adapter exposing table-only writes."""

    def __init__(
        self,
        spark: SparkSession,
        locator: str | os.PathLike[str] | TableLocator | None,
    ) -> None:
        self._writer = SparkDeltaWriter(spark, locator)

    def write(
        self, frame: DataFrame, spec: Any, params_instance: Any, *, streaming: bool = False
    ) -> None:
        """Write a Delta table target spec."""
        _ = streaming
        if not isinstance(
            spec, (AppendSpec, ReplaceSpec, ReplacePartitionsSpec, ReplaceWhereSpec, UpsertSpec)
        ):
            raise TypeError(
                f"SparkDeltaTableWriter only supports TABLE targets; got: {type(spec)!r}"
            )
        self._writer.write(frame, spec, params_instance)

    def append(
        self,
        frame: DataFrame,
        table_ref: TableRef,
        params_instance: Any,
        *,
        streaming: bool = False,
    ) -> None:
        """Append rows to *table_ref*, creating the table on first write."""
        self._writer.append(frame, table_ref, params_instance, streaming=streaming)
