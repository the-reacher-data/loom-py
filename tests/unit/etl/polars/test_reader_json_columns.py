"""Unit tests for JSON column decoding in PolarsDeltaReader (_apply_json_decode)."""

from __future__ import annotations

import json

import polars as pl
import pytest

from loom.etl._schema import ListType, LoomDtype, StructField, StructType
from loom.etl._source import JsonColumnSpec
from loom.etl.backends.polars._reader import _apply_json_decode

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _frame(*rows: dict) -> pl.LazyFrame:
    """Build a lazy frame from a list of row dicts with string-encoded JSON columns."""
    return pl.DataFrame(rows).lazy()


def _json_str(obj: object) -> str:
    return json.dumps(obj)


# ---------------------------------------------------------------------------
# No json_columns → passthrough
# ---------------------------------------------------------------------------


def test_apply_json_decode_no_columns_returns_same_frame() -> None:
    frame = _frame({"id": 1, "payload": '{"x": 1}'})
    result = _apply_json_decode(frame, ())
    assert result is frame


# ---------------------------------------------------------------------------
# Simple struct
# ---------------------------------------------------------------------------


def test_apply_json_decode_simple_struct() -> None:
    struct_type = StructType(
        fields=(
            StructField("order_id", LoomDtype.INT64),
            StructField("amount", LoomDtype.FLOAT64),
        )
    )
    jc = JsonColumnSpec(column="payload", loom_type=struct_type)

    rows = [
        {"id": 1, "payload": _json_str({"order_id": 10, "amount": 99.5})},
        {"id": 2, "payload": _json_str({"order_id": 20, "amount": 49.5})},
    ]
    frame = pl.DataFrame(rows).lazy()
    result = _apply_json_decode(frame, (jc,)).collect()

    assert result["payload"].dtype == pl.Struct(
        [pl.Field("order_id", pl.Int64), pl.Field("amount", pl.Float64)]
    )
    decoded = result["payload"].to_list()
    assert decoded[0]["order_id"] == 10
    assert decoded[1]["amount"] == pytest.approx(49.5)


# ---------------------------------------------------------------------------
# List of primitives
# ---------------------------------------------------------------------------


def test_apply_json_decode_list_of_primitives() -> None:
    jc = JsonColumnSpec(column="tags", loom_type=ListType(inner=LoomDtype.UTF8))

    rows = [
        {"id": 1, "tags": _json_str(["a", "b"])},
        {"id": 2, "tags": _json_str(["x"])},
    ]
    frame = pl.DataFrame(rows).lazy()
    result = _apply_json_decode(frame, (jc,)).collect()

    assert result["tags"].dtype == pl.List(pl.String)
    assert result["tags"][0].to_list() == ["a", "b"]


# ---------------------------------------------------------------------------
# List of structs
# ---------------------------------------------------------------------------


def test_apply_json_decode_list_of_structs() -> None:
    inner = StructType(
        fields=(
            StructField("kind", LoomDtype.UTF8),
            StructField("count", LoomDtype.INT64),
        )
    )
    jc = JsonColumnSpec(column="events", loom_type=ListType(inner=inner))

    rows = [
        {"id": 1, "events": _json_str([{"kind": "click", "count": 3}])},
    ]
    frame = pl.DataFrame(rows).lazy()
    result = _apply_json_decode(frame, (jc,)).collect()

    assert result["events"].dtype == pl.List(
        pl.Struct([pl.Field("kind", pl.String), pl.Field("count", pl.Int64)])
    )
    first = result["events"][0].to_list()
    assert first[0]["kind"] == "click"
    assert first[0]["count"] == 3


# ---------------------------------------------------------------------------
# Multiple json columns decoded independently
# ---------------------------------------------------------------------------


def test_apply_json_decode_multiple_columns() -> None:
    jc_payload = JsonColumnSpec(
        column="payload",
        loom_type=StructType(fields=(StructField("x", LoomDtype.INT64),)),
    )
    jc_tags = JsonColumnSpec(column="tags", loom_type=ListType(inner=LoomDtype.UTF8))

    rows = [{"payload": '{"x": 5}', "tags": '["a","b"]'}]
    frame = pl.DataFrame(rows).lazy()
    result = _apply_json_decode(frame, (jc_payload, jc_tags)).collect()

    assert result["payload"][0]["x"] == 5
    assert result["tags"][0].to_list() == ["a", "b"]


# ---------------------------------------------------------------------------
# Other columns are untouched
# ---------------------------------------------------------------------------


def test_apply_json_decode_preserves_non_json_columns() -> None:
    jc = JsonColumnSpec(
        column="meta",
        loom_type=StructType(fields=(StructField("k", LoomDtype.UTF8),)),
    )
    rows = [{"id": 42, "name": "alice", "meta": '{"k": "v"}'}]
    frame = pl.DataFrame(rows).lazy()
    result = _apply_json_decode(frame, (jc,)).collect()

    assert result["id"][0] == 42
    assert result["name"][0] == "alice"
    assert result["meta"][0]["k"] == "v"


# ---------------------------------------------------------------------------
# Nested struct
# ---------------------------------------------------------------------------


def test_apply_json_decode_nested_struct() -> None:
    inner_struct = StructType(
        fields=(
            StructField("lat", LoomDtype.FLOAT64),
            StructField("lon", LoomDtype.FLOAT64),
        )
    )
    outer_struct = StructType(
        fields=(
            StructField("name", LoomDtype.UTF8),
            StructField("point", inner_struct),
        )
    )
    jc = JsonColumnSpec(column="loc", loom_type=outer_struct)

    rows = [{"loc": _json_str({"name": "HQ", "point": {"lat": 40.7, "lon": -74.0}})}]
    frame = pl.DataFrame(rows).lazy()
    result = _apply_json_decode(frame, (jc,)).collect()

    loc = result["loc"][0]
    assert loc["name"] == "HQ"
    assert loc["point"]["lat"] == pytest.approx(40.7)
    assert loc["point"]["lon"] == pytest.approx(-74.0)


# ---------------------------------------------------------------------------
# Null JSON value is handled gracefully (Polars returns null struct)
# ---------------------------------------------------------------------------


def test_apply_json_decode_null_json_value() -> None:
    jc = JsonColumnSpec(
        column="payload",
        loom_type=StructType(fields=(StructField("x", LoomDtype.INT64),)),
    )
    rows = [{"payload": "null"}, {"payload": '{"x": 1}'}]
    frame = pl.DataFrame(rows).lazy()
    result = _apply_json_decode(frame, (jc,)).collect()

    assert result["payload"][0] is None or result["payload"][0]["x"] is None
    assert result["payload"][1]["x"] == 1
