"""Polars file writer adapter."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import polars as pl

from loom.etl.io.target._file import FileSpec


class PolarsFileWriter:
    """Adapter exposing FILE writes only."""

    def __init__(self, write_fn: Callable[[pl.LazyFrame, Any, Any], None]) -> None:
        self._write_fn = write_fn

    def write(self, frame: pl.LazyFrame, spec: FileSpec) -> None:
        """Write FILE target through the legacy Polars writer implementation."""
        self._write_fn(frame, spec, None)
