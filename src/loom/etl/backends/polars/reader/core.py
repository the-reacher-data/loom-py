"""PolarsSourceReader dispatching TABLE and FILE source kinds."""

from __future__ import annotations

import os
from typing import Any

import polars as pl

from loom.etl.backends.polars._reader import PolarsDeltaReader
from loom.etl.io._source import SourceKind, SourceSpec
from loom.etl.storage._locator import TableLocator

from .file import PolarsFileReader
from .table import PolarsDeltaTableReader


class PolarsSourceReader(PolarsDeltaReader):
    """Polars implementation of ``SourceReader`` with TABLE + FILE support."""

    def __init__(self, locator: str | os.PathLike[str] | TableLocator) -> None:
        super().__init__(locator)
        self._table_reader = PolarsDeltaTableReader(self)
        self._file_reader = PolarsFileReader(self)

    def read(self, spec: SourceSpec, params_instance: Any) -> pl.LazyFrame:
        """Read source spec with Polars according to its ``SourceKind``."""
        match spec.kind:
            case SourceKind.TABLE:
                return self._table_reader.read(spec, params_instance)
            case SourceKind.FILE:
                return self._file_reader.read(spec, params_instance)
            case _:
                raise TypeError(
                    f"PolarsSourceReader cannot read source kind {spec.kind!r}. "
                    "TEMP sources are handled by IntermediateStore."
                )
