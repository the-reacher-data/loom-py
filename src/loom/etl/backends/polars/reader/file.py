"""Polars file reader adapter."""

from __future__ import annotations

from typing import Any

import polars as pl

from loom.etl.backends.polars._reader import PolarsDeltaReader
from loom.etl.io._source import SourceSpec


class PolarsFileReader:
    """Adapter exposing FILE reads only."""

    def __init__(self, reader: PolarsDeltaReader) -> None:
        self._reader = reader

    def read(self, spec: SourceSpec, _params_instance: Any) -> pl.LazyFrame:
        """Read FILE source through the legacy Polars reader implementation."""
        if spec.path is None:
            raise ValueError(f"FromFile spec has no path: {spec}")
        return self._reader._read_file(spec)
