"""SparkTargetWriter dispatching TABLE and FILE target specs."""

from __future__ import annotations

import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from loom.etl.backends.spark._writer import SparkDeltaWriter
from loom.etl.io.target import TargetSpec
from loom.etl.io.target._file import FileSpec
from loom.etl.io.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpsertSpec,
)
from loom.etl.storage._locator import TableLocator

from .file import SparkFileWriter


class SparkTargetWriter(SparkDeltaWriter):
    """Spark implementation of ``TargetWriter`` with TABLE + FILE support."""

    def __init__(
        self,
        spark: SparkSession,
        locator: str | os.PathLike[str] | TableLocator | None,
    ) -> None:
        super().__init__(spark, locator)
        self._file_writer = SparkFileWriter()

    def write(self, frame: DataFrame, spec: TargetSpec, params_instance: Any) -> None:
        """Write target spec with Spark according to its spec variant."""
        if isinstance(spec, FileSpec):
            self._file_writer.write(frame, spec)
            return
        if isinstance(
            spec, (AppendSpec, ReplaceSpec, ReplacePartitionsSpec, ReplaceWhereSpec, UpsertSpec)
        ):
            super().write(frame, spec, params_instance)
            return
        raise TypeError(f"SparkTargetWriter does not support target spec: {type(spec)!r}")
