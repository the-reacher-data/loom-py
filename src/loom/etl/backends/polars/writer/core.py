"""PolarsTargetWriter dispatching TABLE and FILE target specs."""

from __future__ import annotations

import os
from typing import Any

import polars as pl

from loom.etl.backends.polars._writer import PolarsDeltaWriter
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

from .file import PolarsFileWriter
from .table import PolarsDeltaTableWriter


class PolarsTargetWriter(PolarsDeltaWriter):
    """Polars implementation of ``TargetWriter`` with TABLE + FILE support."""

    def __init__(self, locator: str | os.PathLike[str] | TableLocator) -> None:
        super().__init__(locator)
        self._table_writer = PolarsDeltaTableWriter(super().write)
        self._file_writer = PolarsFileWriter(super().write)

    def write(self, frame: pl.LazyFrame, spec: TargetSpec, params_instance: Any) -> None:
        """Write target spec with Polars according to its spec variant."""
        if isinstance(spec, FileSpec):
            self._file_writer.write(frame, spec)
            return
        if isinstance(
            spec, (AppendSpec, ReplaceSpec, ReplacePartitionsSpec, ReplaceWhereSpec, UpsertSpec)
        ):
            self._table_writer.write(frame, spec, params_instance)
            return
        raise TypeError(f"PolarsTargetWriter does not support target spec: {type(spec)!r}")
