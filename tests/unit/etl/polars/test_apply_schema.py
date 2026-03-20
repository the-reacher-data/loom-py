"""Unit tests for apply_schema — pure function, no Delta dependency."""

from __future__ import annotations

import polars as pl
import pytest

from loom.etl._schema import ColumnSchema, LoomDtype
from loom.etl._target import SchemaMode
from loom.etl.backends.polars._schema import SchemaError, SchemaNotFoundError, apply_schema

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_SCHEMA: tuple[ColumnSchema, ...] = (
    ColumnSchema("id", LoomDtype.INT64, nullable=False),
    ColumnSchema("amount", LoomDtype.FLOAT64),
    ColumnSchema("label", LoomDtype.UTF8),
)


def _frame(**cols) -> pl.LazyFrame:  # type: ignore[no-untyped-def]
    return pl.DataFrame(cols).lazy()


# ---------------------------------------------------------------------------
# schema is None — all modes must raise SchemaNotFoundError
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", list(SchemaMode))
def test_apply_schema_none_raises_schema_not_found(mode: SchemaMode) -> None:
    frame = _frame(id=[1], amount=[1.0], label=["x"])
    with pytest.raises(SchemaNotFoundError):
        apply_schema(frame, None, mode)


# ---------------------------------------------------------------------------
# OVERWRITE — always passes through unchanged
# ---------------------------------------------------------------------------


def test_overwrite_passes_through_matching_frame() -> None:
    frame = _frame(id=[1], amount=[1.0], label=["x"])
    result = apply_schema(frame, _SCHEMA, SchemaMode.OVERWRITE)
    assert result is frame


def test_overwrite_passes_through_mismatched_types() -> None:
    frame = _frame(id=["not-an-int"], amount=[1.0], label=["x"])
    result = apply_schema(frame, _SCHEMA, SchemaMode.OVERWRITE)
    assert result is frame


def test_overwrite_passes_through_extra_columns() -> None:
    frame = _frame(id=[1], amount=[1.0], label=["x"], extra=["y"])
    result = apply_schema(frame, _SCHEMA, SchemaMode.OVERWRITE)
    assert result is frame


# ---------------------------------------------------------------------------
# STRICT — exact match required
# ---------------------------------------------------------------------------


def test_strict_passes_with_matching_frame() -> None:
    frame = _frame(id=[1], amount=[1.0], label=["x"])
    result = apply_schema(frame, _SCHEMA, SchemaMode.STRICT)
    assert result is frame


def test_strict_fails_on_extra_column() -> None:
    frame = _frame(id=[1], amount=[1.0], label=["x"], extra=["y"])
    with pytest.raises(SchemaError, match="extra"):
        apply_schema(frame, _SCHEMA, SchemaMode.STRICT)


def test_strict_fails_on_missing_column() -> None:
    frame = _frame(id=[1], amount=[1.0])
    with pytest.raises(SchemaError, match="missing"):
        apply_schema(frame, _SCHEMA, SchemaMode.STRICT)


def test_strict_fails_on_type_mismatch() -> None:
    frame = _frame(id=[1], amount=[1.0], label=[99])  # label should be Utf8
    with pytest.raises(SchemaError, match="label"):
        apply_schema(frame, _SCHEMA, SchemaMode.STRICT)


# ---------------------------------------------------------------------------
# EVOLVE — extra columns allowed; missing columns filled with typed nulls
# ---------------------------------------------------------------------------


def test_evolve_passes_with_matching_frame() -> None:
    frame = _frame(id=[1], amount=[1.0], label=["x"])
    result = apply_schema(frame, _SCHEMA, SchemaMode.EVOLVE)
    assert result is frame


def test_evolve_allows_extra_columns() -> None:
    frame = _frame(id=[1], amount=[1.0], label=["x"], new_col=["z"])
    result = apply_schema(frame, _SCHEMA, SchemaMode.EVOLVE)
    df = result.collect()
    assert "new_col" in df.columns


def test_evolve_fills_missing_columns_with_typed_nulls() -> None:
    frame = _frame(id=[1], amount=[1.0])  # label missing
    result = apply_schema(frame, _SCHEMA, SchemaMode.EVOLVE)
    df = result.collect()
    assert "label" in df.columns
    assert df["label"].dtype == pl.String
    assert df["label"][0] is None


def test_evolve_fails_on_type_mismatch() -> None:
    frame = _frame(id=[1], amount=["not-a-float"], label=["x"])
    with pytest.raises(SchemaError, match="amount"):
        apply_schema(frame, _SCHEMA, SchemaMode.EVOLVE)


def test_evolve_missing_column_dtype_matches_schema() -> None:
    schema = (
        ColumnSchema("id", LoomDtype.INT64),
        ColumnSchema("score", LoomDtype.FLOAT32),
    )
    frame = _frame(id=[1])  # score missing
    result = apply_schema(frame, schema, SchemaMode.EVOLVE)
    df = result.collect()
    assert df["score"].dtype == pl.Float32
