"""Unit tests for Polars checkpoint append schema alignment."""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl

from loom.etl.checkpoint._backends._polars import _PolarsCheckpointBackend


def test_append_aligns_to_existing_schema(tmp_path: Path) -> None:
    backend = _PolarsCheckpointBackend(storage_options={})
    base = str(tmp_path / "checkpoints")

    # Create base directory only (cloud storage doesn't need pre-created subdirs)
    os.makedirs(base, exist_ok=True)

    first = pl.DataFrame({"id": [1], "amount": [10.5]}).lazy()
    second = pl.DataFrame({"id": ["2"], "extra": ["x"]}).lazy()

    backend.write("orders", base, first, append=True)
    backend.write("orders", base, second, append=True)

    scanned = backend.probe("orders", base)
    assert scanned is not None
    out = scanned.collect().sort("id")

    assert out.columns == ["id", "amount"]
    assert out.schema["id"] == pl.Int64
    assert out.schema["amount"] == pl.Float64
    assert out.to_dict(as_series=False) == {"id": [1, 2], "amount": [10.5, None]}
