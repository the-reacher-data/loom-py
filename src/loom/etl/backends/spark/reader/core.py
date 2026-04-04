"""SparkSourceReader dispatching TABLE and FILE source kinds."""

from __future__ import annotations

import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from loom.etl.backends.spark._reader import SparkDeltaReader
from loom.etl.io._source import SourceKind, SourceSpec
from loom.etl.storage._locator import TableLocator

from .file import SparkFileReader


class SparkSourceReader(SparkDeltaReader):
    """Spark implementation of ``SourceReader`` with TABLE + FILE support."""

    def __init__(
        self,
        spark: SparkSession,
        locator: str | os.PathLike[str] | TableLocator | None = None,
    ) -> None:
        super().__init__(spark, locator)
        self._file_reader = SparkFileReader(spark)

    def read(self, spec: SourceSpec, params_instance: Any) -> DataFrame:
        """Read source spec with Spark according to its ``SourceKind``."""
        match spec.kind:
            case SourceKind.TABLE:
                return super().read(spec, params_instance)
            case SourceKind.FILE:
                return self._file_reader.read(spec, params_instance)
            case _:
                raise TypeError(
                    f"SparkSourceReader cannot read source kind {spec.kind!r}. "
                    "TEMP sources are handled by IntermediateStore."
                )
