"""PolarsSourceReader dispatching TABLE and FILE source kinds."""

from __future__ import annotations

import os
from typing import Any

import polars as pl

from loom.etl.backends.polars._reader import PolarsDeltaReader
from loom.etl.io.source import FileSourceSpec, SourceSpec, TableSourceSpec
from loom.etl.storage._locator import TableLocator


class PolarsSourceReader(PolarsDeltaReader):
    """Polars implementation of ``SourceReader`` with TABLE + FILE support."""

    def __init__(self, locator: str | os.PathLike[str] | TableLocator) -> None:
        super().__init__(locator)

    def read(self, spec: SourceSpec, params_instance: Any) -> pl.LazyFrame:
        """Read source spec with Polars by dispatching on the spec type."""
        if isinstance(spec, TableSourceSpec):
            return self._read_delta(spec, params_instance)
        if isinstance(spec, FileSourceSpec):
            return self._read_file(spec)
        raise TypeError(
            f"PolarsSourceReader cannot read source kind {spec.kind!r}. "
            "TEMP sources are handled by IntermediateStore."
        )
