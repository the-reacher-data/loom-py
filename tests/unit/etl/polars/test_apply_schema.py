"""Unit tests for apply_schema — pure function, no Delta dependency."""

from __future__ import annotations

import polars as pl
import pytest

from loom.etl.backends.polars._schema import SchemaError, SchemaNotFoundError, apply_schema
from loom.etl.io._target import SchemaMode
from loom.etl.schema._schema import (
    ArrayType,
    ColumnSchema,
    DatetimeType,
    DecimalType,
    EnumType,
    ListType,
    LoomDtype,
    StructField,
    StructType,
)

_SCHEMA: tuple[ColumnSchema, ...] = (
    ColumnSchema("id", LoomDtype.INT64, nullable=False),
    ColumnSchema("amount", LoomDtype.FLOAT64),
    ColumnSchema("label", LoomDtype.UTF8),
)


def _frame(**cols) -> pl.LazyFrame:  # type: ignore[no-untyped-def]
    return pl.DataFrame(cols).lazy()


@pytest.mark.parametrize("mode", list(SchemaMode))
def test_apply_schema_none_raises_schema_not_found(mode: SchemaMode) -> None:
    frame = _frame(id=[1], amount=[1.0], label=["x"])
    with pytest.raises(SchemaNotFoundError):
        apply_schema(frame, None, mode)


@pytest.mark.parametrize(
    "frame",
    [
        _frame(id=[1], amount=[1.0], label=["x"]),
        _frame(id=["not-an-int"], amount=[1.0], label=["x"]),
        _frame(id=[1], amount=[1.0], label=["x"], extra=["y"]),
    ],
)
def test_overwrite_passes_through_frame(frame: pl.LazyFrame) -> None:
    result = apply_schema(frame, _SCHEMA, SchemaMode.OVERWRITE)
    assert result is frame


@pytest.mark.parametrize("mode", [SchemaMode.STRICT, SchemaMode.EVOLVE])
def test_apply_schema_passes_with_matching_frame(mode: SchemaMode) -> None:
    frame = _frame(id=[1], amount=[1.0], label=["x"])
    result = apply_schema(frame, _SCHEMA, mode)
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


# ---------------------------------------------------------------------------
# Complex type validation — strict mode
# ---------------------------------------------------------------------------


def test_strict_passes_list_of_int() -> None:
    schema = (ColumnSchema("tags", ListType(inner=LoomDtype.INT64)),)
    frame = pl.DataFrame({"tags": [[1, 2, 3]]}).lazy()
    result = apply_schema(frame, schema, SchemaMode.STRICT)
    assert result is frame


def test_strict_fails_list_inner_type_mismatch() -> None:
    schema = (ColumnSchema("tags", ListType(inner=LoomDtype.INT64)),)
    frame = pl.DataFrame({"tags": [["a", "b"]]}).lazy()  # List[String], not List[Int64]
    with pytest.raises(SchemaError, match="tags"):
        apply_schema(frame, schema, SchemaMode.STRICT)


def test_strict_passes_struct() -> None:
    schema = (
        ColumnSchema(
            "address",
            StructType(
                fields=(
                    StructField("street", LoomDtype.UTF8),
                    StructField("zip", LoomDtype.UTF8),
                )
            ),
        ),
    )
    frame = pl.DataFrame({"address": [{"street": "Main St", "zip": "12345"}]}).lazy()
    result = apply_schema(frame, schema, SchemaMode.STRICT)
    assert result is frame


def test_strict_fails_struct_field_type_mismatch() -> None:
    schema = (
        ColumnSchema(
            "point",
            StructType(
                fields=(
                    StructField("x", LoomDtype.FLOAT64),
                    StructField("y", LoomDtype.FLOAT64),
                )
            ),
        ),
    )
    frame = pl.DataFrame(
        {"point": [{"x": 1, "y": 2.0}]}
    ).lazy()  # frame has x as Int64, schema expects Float64
    with pytest.raises(SchemaError, match="point"):
        apply_schema(frame, schema, SchemaMode.STRICT)


def test_strict_passes_list_of_struct() -> None:
    schema = (
        ColumnSchema(
            "events",
            ListType(
                inner=StructType(
                    fields=(
                        StructField("kind", LoomDtype.UTF8),
                        StructField("count", LoomDtype.INT64),
                    )
                )
            ),
        ),
    )
    frame = pl.DataFrame({"events": [[{"kind": "click", "count": 5}]]}).lazy()
    result = apply_schema(frame, schema, SchemaMode.STRICT)
    assert result is frame


def test_strict_fails_list_of_struct_inner_mismatch() -> None:
    schema = (
        ColumnSchema(
            "events",
            ListType(
                inner=StructType(
                    fields=(
                        StructField("kind", LoomDtype.UTF8),
                        StructField("count", LoomDtype.INT64),
                    )
                )
            ),
        ),
    )
    frame = pl.DataFrame(
        {"events": [[{"kind": "click", "count": 5.0}]]}
    ).lazy()  # frame has count as Float64, schema expects Int64
    with pytest.raises(SchemaError, match="events"):
        apply_schema(frame, schema, SchemaMode.STRICT)


def test_strict_passes_struct_of_list() -> None:
    schema = (
        ColumnSchema(
            "record",
            StructType(
                fields=(
                    StructField("ids", ListType(inner=LoomDtype.INT64)),
                    StructField("name", LoomDtype.UTF8),
                )
            ),
        ),
    )
    frame = pl.DataFrame({"record": [{"ids": [1, 2], "name": "alice"}]}).lazy()
    result = apply_schema(frame, schema, SchemaMode.STRICT)
    assert result is frame


def test_strict_passes_decimal_type() -> None:
    schema = (ColumnSchema("price", DecimalType(precision=10, scale=2)),)
    frame = pl.DataFrame({"price": ["1.50"]}).select(pl.col("price").cast(pl.Decimal(10, 2))).lazy()
    result = apply_schema(frame, schema, SchemaMode.STRICT)
    assert result is frame


def test_strict_fails_decimal_precision_mismatch() -> None:
    schema = (ColumnSchema("price", DecimalType(precision=10, scale=2)),)
    frame = (
        pl.DataFrame({"price": ["1.50"]}).select(pl.col("price").cast(pl.Decimal(10, 4))).lazy()
    )  # decimal scale mismatch: frame 4, schema 2
    with pytest.raises(SchemaError, match="price"):
        apply_schema(frame, schema, SchemaMode.STRICT)


def test_strict_passes_datetime_with_tz() -> None:
    schema = (ColumnSchema("ts", DatetimeType("us", "UTC")),)
    frame = (
        pl.DataFrame({"ts": [1000000]}).select(pl.col("ts").cast(pl.Datetime("us", "UTC"))).lazy()
    )
    result = apply_schema(frame, schema, SchemaMode.STRICT)
    assert result is frame


def test_strict_fails_datetime_tz_mismatch() -> None:
    schema = (ColumnSchema("ts", DatetimeType("us", "UTC")),)
    frame = (
        pl.DataFrame({"ts": [1000000]})
        .select(
            pl.col("ts").cast(pl.Datetime("us"))  # naive, no tz
        )
        .lazy()
    )
    with pytest.raises(SchemaError, match="ts"):
        apply_schema(frame, schema, SchemaMode.STRICT)


def test_strict_passes_array_type() -> None:
    schema = (ColumnSchema("vec", ArrayType(inner=LoomDtype.FLOAT64, width=3)),)
    frame = (
        pl.DataFrame({"vec": [[1.0, 2.0, 3.0]]})
        .select(pl.col("vec").cast(pl.Array(pl.Float64, 3)))
        .lazy()
    )
    result = apply_schema(frame, schema, SchemaMode.STRICT)
    assert result is frame


def test_strict_passes_enum_type() -> None:
    categories = ("low", "medium", "high")
    schema = (ColumnSchema("priority", EnumType(categories=categories)),)
    frame = (
        pl.DataFrame({"priority": ["low"]})
        .select(pl.col("priority").cast(pl.Enum(list(categories))))
        .lazy()
    )
    result = apply_schema(frame, schema, SchemaMode.STRICT)
    assert result is frame


# ---------------------------------------------------------------------------
# Coarse LoomDtype — structural aliases accept any parametrisation
# ---------------------------------------------------------------------------


def test_strict_coarse_list_accepts_any_inner_type() -> None:
    """LoomDtype.LIST is coarse — accepts List[Int64], List[String], etc."""
    schema = (ColumnSchema("tags", LoomDtype.LIST),)
    frame = pl.DataFrame({"tags": [[1, 2]]}).lazy()
    result = apply_schema(frame, schema, SchemaMode.STRICT)
    assert result is frame


def test_strict_coarse_datetime_accepts_any_parametrisation() -> None:
    """LoomDtype.DATETIME is coarse — accepts any time_unit / tz."""
    schema = (ColumnSchema("ts", LoomDtype.DATETIME),)
    frame = (
        pl.DataFrame({"ts": [1000000]}).select(pl.col("ts").cast(pl.Datetime("ns", "UTC"))).lazy()
    )
    result = apply_schema(frame, schema, SchemaMode.STRICT)
    assert result is frame


# ---------------------------------------------------------------------------
# EVOLVE — null fill for complex types
# ---------------------------------------------------------------------------


def test_evolve_fills_missing_list_column() -> None:
    schema = (
        ColumnSchema("id", LoomDtype.INT64),
        ColumnSchema("tags", ListType(inner=LoomDtype.UTF8)),
    )
    frame = _frame(id=[1])
    result = apply_schema(frame, schema, SchemaMode.EVOLVE)
    df = result.collect()
    assert "tags" in df.columns
    assert df["tags"].dtype == pl.List(pl.String)
    assert df["tags"][0] is None


def test_evolve_fills_missing_struct_column() -> None:
    schema = (
        ColumnSchema("id", LoomDtype.INT64),
        ColumnSchema(
            "point",
            StructType(
                fields=(
                    StructField("x", LoomDtype.FLOAT64),
                    StructField("y", LoomDtype.FLOAT64),
                )
            ),
        ),
    )
    frame = _frame(id=[1])
    result = apply_schema(frame, schema, SchemaMode.EVOLVE)
    df = result.collect()
    assert "point" in df.columns
    assert isinstance(df["point"].dtype, pl.Struct)


def test_evolve_fills_missing_nested_list_of_struct() -> None:
    schema = (
        ColumnSchema("id", LoomDtype.INT64),
        ColumnSchema(
            "events", ListType(inner=StructType(fields=(StructField("kind", LoomDtype.UTF8),)))
        ),
    )
    frame = _frame(id=[1])
    result = apply_schema(frame, schema, SchemaMode.EVOLVE)
    df = result.collect()
    assert "events" in df.columns
    assert isinstance(df["events"].dtype, pl.List)


def test_evolve_coarse_list_dtype_raises_schema_error_on_missing() -> None:
    """LoomDtype.LIST (coarse) cannot fill a missing column — no inner type."""
    schema = (
        ColumnSchema("id", LoomDtype.INT64),
        ColumnSchema("tags", LoomDtype.LIST),
    )
    frame = _frame(id=[1])
    with pytest.raises(SchemaError, match="tags"):
        apply_schema(frame, schema, SchemaMode.EVOLVE)
