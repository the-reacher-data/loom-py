"""Polars Delta table reader adapter."""

from __future__ import annotations

from typing import Any

import polars as pl

from loom.etl.backends.polars._reader import PolarsDeltaReader
from loom.etl.io.source import TableSourceSpec


class PolarsDeltaTableReader:
    """Adapter exposing TABLE reads only."""

    def __init__(self, reader: PolarsDeltaReader) -> None:
        self._reader = reader

    def read(self, spec: TableSourceSpec, params_instance: Any) -> pl.LazyFrame:
        """Read TABLE source through the Polars reader implementation."""
        return self._reader._read_delta(spec, params_instance)
