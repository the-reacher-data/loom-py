"""Unit tests for parse_json and extended with_schema on FromTable / FromFile."""

from __future__ import annotations

import dataclasses

import msgspec
import pytest

from loom.etl._format import Format
from loom.etl._schema import (
    ColumnSchema,
    ListType,
    LoomDtype,
    StructField,
    StructType,
)
from loom.etl._source import FromFile, FromTable, JsonColumnSpec

# ---------------------------------------------------------------------------
# Sample contracts
# ---------------------------------------------------------------------------


class OrderPayload(msgspec.Struct):
    order_id: int
    amount: float


class EventPayload(msgspec.Struct):
    kind: str
    user_id: int


@dataclasses.dataclass
class CsvRow:
    id: int
    label: str


# ---------------------------------------------------------------------------
# FromTable.parse_json — contract forms
# ---------------------------------------------------------------------------


def test_from_table_parse_json_with_class_produces_struct_spec() -> None:
    src = FromTable("raw.events").parse_json("payload", OrderPayload)
    spec = src._to_spec("events")

    assert len(spec.json_columns) == 1
    jc = spec.json_columns[0]
    assert jc.column == "payload"
    assert jc.loom_type == StructType(
        fields=(
            StructField("order_id", LoomDtype.INT64),
            StructField("amount", LoomDtype.FLOAT64),
        )
    )


def test_from_table_parse_json_with_loom_type_passthrough() -> None:
    st = StructType(fields=(StructField("x", LoomDtype.INT64),))
    spec = FromTable("raw.t").parse_json("col", st)._to_spec("t")

    assert spec.json_columns[0].loom_type is st


def test_from_table_parse_json_with_list_generic() -> None:
    spec = FromTable("raw.t").parse_json("tags", list[str])._to_spec("t")

    assert spec.json_columns[0].loom_type == ListType(inner=LoomDtype.UTF8)


def test_from_table_parse_json_with_list_of_struct() -> None:
    spec = FromTable("raw.t").parse_json("events", list[EventPayload])._to_spec("t")

    expected_inner = StructType(
        fields=(
            StructField("kind", LoomDtype.UTF8),
            StructField("user_id", LoomDtype.INT64),
        )
    )
    assert spec.json_columns[0].loom_type == ListType(inner=expected_inner)


# ---------------------------------------------------------------------------
# FromTable.parse_json — immutability and accumulation
# ---------------------------------------------------------------------------


def test_from_table_parse_json_is_immutable() -> None:
    original = FromTable("raw.events")
    extended = original.parse_json("payload", OrderPayload)

    assert original._to_spec("e").json_columns == ()
    assert len(extended._to_spec("e").json_columns) == 1


def test_from_table_parse_json_accumulates_multiple_columns() -> None:
    src = FromTable("raw.events").parse_json("payload", OrderPayload).parse_json("tags", list[str])
    spec = src._to_spec("events")

    assert len(spec.json_columns) == 2
    assert spec.json_columns[0].column == "payload"
    assert spec.json_columns[1].column == "tags"


def test_from_table_parse_json_preserves_other_state() -> None:
    from loom.etl._table import col

    src = (
        FromTable("raw.events")
        .where(col("year") == 2024)
        .columns("id", "payload")
        .parse_json("payload", OrderPayload)
    )
    spec = src._to_spec("events")

    assert len(spec.predicates) == 1
    assert spec.columns == ("id", "payload")
    assert len(spec.json_columns) == 1


# ---------------------------------------------------------------------------
# FromFile.parse_json
# ---------------------------------------------------------------------------


def test_from_file_parse_json_with_class() -> None:
    src = FromFile("s3://raw/events.json", format=Format.JSON).parse_json("payload", OrderPayload)
    spec = src._to_spec("events")

    assert len(spec.json_columns) == 1
    jc = spec.json_columns[0]
    assert jc.column == "payload"
    assert isinstance(jc.loom_type, StructType)


def test_from_file_parse_json_is_immutable() -> None:
    original = FromFile("s3://raw/e.csv", format=Format.CSV)
    extended = original.parse_json("meta", OrderPayload)

    assert original._to_spec("e").json_columns == ()
    assert len(extended._to_spec("e").json_columns) == 1


def test_from_file_parse_json_accumulates() -> None:
    src = (
        FromFile("s3://raw/e.json", format=Format.JSON)
        .parse_json("payload", OrderPayload)
        .parse_json("extra", list[int])
    )
    spec = src._to_spec("e")

    assert len(spec.json_columns) == 2


def test_from_file_parse_json_preserves_other_state() -> None:
    from loom.etl._read_options import JsonReadOptions

    src = (
        FromFile("s3://raw/e.json", format=Format.JSON)
        .with_options(JsonReadOptions(infer_schema_length=None))
        .parse_json("payload", OrderPayload)
    )
    spec = src._to_spec("e")

    assert spec.read_options is not None
    assert len(spec.json_columns) == 1


# ---------------------------------------------------------------------------
# with_schema — class contract
# ---------------------------------------------------------------------------


def test_from_table_with_schema_accepts_class() -> None:
    spec = FromTable("raw.orders").with_schema(OrderPayload)._to_spec("orders")

    assert spec.schema == (
        ColumnSchema("order_id", LoomDtype.INT64),
        ColumnSchema("amount", LoomDtype.FLOAT64),
    )


def test_from_table_with_schema_accepts_class_dataclass() -> None:
    spec = FromTable("raw.t").with_schema(CsvRow)._to_spec("t")

    assert spec.schema == (
        ColumnSchema("id", LoomDtype.INT64),
        ColumnSchema("label", LoomDtype.UTF8),
    )


def test_from_file_with_schema_accepts_class() -> None:
    spec = FromFile("s3://raw/t.csv", format=Format.CSV).with_schema(CsvRow)._to_spec("t")

    assert spec.schema == (
        ColumnSchema("id", LoomDtype.INT64),
        ColumnSchema("label", LoomDtype.UTF8),
    )


def test_with_schema_class_passthrough_tuple() -> None:
    schema = (ColumnSchema("x", LoomDtype.INT32),)
    spec = FromTable("raw.t").with_schema(schema)._to_spec("t")

    assert spec.schema == schema


# ---------------------------------------------------------------------------
# Combination: with_schema + parse_json
# ---------------------------------------------------------------------------


def test_with_schema_and_parse_json_combined() -> None:
    src = FromTable("raw.events").with_schema(CsvRow).parse_json("payload", OrderPayload)
    spec = src._to_spec("events")

    assert len(spec.schema) == 2
    assert len(spec.json_columns) == 1


# ---------------------------------------------------------------------------
# JsonColumnSpec dataclass
# ---------------------------------------------------------------------------


def test_json_column_spec_is_frozen() -> None:
    jc = JsonColumnSpec(column="col", loom_type=LoomDtype.UTF8)
    with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
        jc.column = "other"  # type: ignore[misc]


def test_json_column_spec_equality() -> None:
    a = JsonColumnSpec(column="x", loom_type=LoomDtype.INT64)
    b = JsonColumnSpec(column="x", loom_type=LoomDtype.INT64)
    assert a == b
