"""Polars Delta table writer adapter."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import polars as pl

from loom.etl.io.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpsertSpec,
)


class PolarsDeltaTableWriter:
    """Adapter exposing TABLE writes only."""

    def __init__(self, write_fn: Callable[[pl.LazyFrame, Any, Any], None]) -> None:
        self._write_fn = write_fn

    def write(self, frame: pl.LazyFrame, spec: Any, params_instance: Any) -> None:
        """Write TABLE target through the legacy Polars writer implementation."""
        if not isinstance(
            spec, (AppendSpec, ReplaceSpec, ReplacePartitionsSpec, ReplaceWhereSpec, UpsertSpec)
        ):
            raise TypeError(
                f"PolarsDeltaTableWriter only supports TABLE targets; got: {type(spec)!r}"
            )
        self._write_fn(frame, spec, params_instance)
