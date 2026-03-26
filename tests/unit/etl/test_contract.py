"""Unit tests for _contract — annotation introspection to Loom schema types."""

from __future__ import annotations

import dataclasses
import datetime

import msgspec
import pytest

from loom.etl._contract import (
    _annotation_to_loom_type,
    _strip_optional,
    resolve_json_type,
    resolve_schema,
)
from loom.etl._schema import (
    ColumnSchema,
    DatetimeType,
    ListType,
    LoomDtype,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Helpers — sample contracts
# ---------------------------------------------------------------------------


class PlainRow:
    id: int
    name: str
    score: float


@dataclasses.dataclass
class DataclassRow:
    order_id: int
    amount: float
    active: bool


class MsgspecRow(msgspec.Struct):
    user_id: int
    email: str
    age: float


class NestedAddress(msgspec.Struct):
    street: str
    zip_code: str


class NestedRow(msgspec.Struct):
    id: int
    address: NestedAddress


class RowWithOptional(msgspec.Struct):
    id: int
    label: str | None


class NoAnnotations:
    pass


# ---------------------------------------------------------------------------
# resolve_schema — passthrough
# ---------------------------------------------------------------------------


def test_resolve_schema_passthrough_explicit_tuple() -> None:
    schema = (
        ColumnSchema("id", LoomDtype.INT64),
        ColumnSchema("name", LoomDtype.UTF8),
    )
    assert resolve_schema(schema) is schema


def test_resolve_schema_empty_tuple() -> None:
    assert resolve_schema(()) == ()


# ---------------------------------------------------------------------------
# resolve_schema — from annotated class
# ---------------------------------------------------------------------------


def test_resolve_schema_from_plain_class() -> None:
    result = resolve_schema(PlainRow)
    assert result == (
        ColumnSchema("id", LoomDtype.INT64),
        ColumnSchema("name", LoomDtype.UTF8),
        ColumnSchema("score", LoomDtype.FLOAT64),
    )


def test_resolve_schema_from_dataclass() -> None:
    result = resolve_schema(DataclassRow)
    assert result == (
        ColumnSchema("order_id", LoomDtype.INT64),
        ColumnSchema("amount", LoomDtype.FLOAT64),
        ColumnSchema("active", LoomDtype.BOOLEAN),
    )


def test_resolve_schema_from_msgspec_struct() -> None:
    result = resolve_schema(MsgspecRow)
    assert result == (
        ColumnSchema("user_id", LoomDtype.INT64),
        ColumnSchema("email", LoomDtype.UTF8),
        ColumnSchema("age", LoomDtype.FLOAT64),
    )


def test_resolve_schema_class_with_no_annotations_raises() -> None:
    with pytest.raises(TypeError, match="no type annotations"):
        resolve_schema(NoAnnotations)


def test_resolve_schema_non_class_non_tuple_raises() -> None:
    with pytest.raises(TypeError, match="annotated class"):
        resolve_schema("not_a_class")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# resolve_json_type — passthrough
# ---------------------------------------------------------------------------


def test_resolve_json_type_passthrough_loom_dtype() -> None:
    assert resolve_json_type(LoomDtype.UTF8) is LoomDtype.UTF8


def test_resolve_json_type_passthrough_struct_type() -> None:
    st = StructType(fields=(StructField("x", LoomDtype.FLOAT64),))
    assert resolve_json_type(st) is st


def test_resolve_json_type_passthrough_list_type() -> None:
    lt = ListType(inner=LoomDtype.INT64)
    assert resolve_json_type(lt) is lt


# ---------------------------------------------------------------------------
# resolve_json_type — from annotated class
# ---------------------------------------------------------------------------


def test_resolve_json_type_from_msgspec_struct() -> None:
    result = resolve_json_type(MsgspecRow)
    assert result == StructType(
        fields=(
            StructField("user_id", LoomDtype.INT64),
            StructField("email", LoomDtype.UTF8),
            StructField("age", LoomDtype.FLOAT64),
        )
    )


def test_resolve_json_type_nested_struct() -> None:
    result = resolve_json_type(NestedRow)
    expected = StructType(
        fields=(
            StructField("id", LoomDtype.INT64),
            StructField(
                "address",
                StructType(
                    fields=(
                        StructField("street", LoomDtype.UTF8),
                        StructField("zip_code", LoomDtype.UTF8),
                    )
                ),
            ),
        )
    )
    assert result == expected


# ---------------------------------------------------------------------------
# resolve_json_type — list[X] generic
# ---------------------------------------------------------------------------


def test_resolve_json_type_list_of_primitive() -> None:
    result = resolve_json_type(list[str])
    assert result == ListType(inner=LoomDtype.UTF8)


def test_resolve_json_type_list_of_int() -> None:
    assert resolve_json_type(list[int]) == ListType(inner=LoomDtype.INT64)


def test_resolve_json_type_list_of_struct_class() -> None:
    result = resolve_json_type(list[MsgspecRow])
    expected = ListType(
        inner=StructType(
            fields=(
                StructField("user_id", LoomDtype.INT64),
                StructField("email", LoomDtype.UTF8),
                StructField("age", LoomDtype.FLOAT64),
            )
        )
    )
    assert result == expected


def test_resolve_json_type_unknown_raises() -> None:
    with pytest.raises(TypeError, match="annotated class"):
        resolve_json_type(42)


# ---------------------------------------------------------------------------
# _annotation_to_loom_type — primitive mapping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "py_type,expected",
    [
        (int, LoomDtype.INT64),
        (float, LoomDtype.FLOAT64),
        (str, LoomDtype.UTF8),
        (bool, LoomDtype.BOOLEAN),
        (bytes, LoomDtype.BINARY),
        (datetime.datetime, LoomDtype.DATETIME),
        (datetime.date, LoomDtype.DATE),
    ],
)
def test_annotation_primitive_mapping(py_type: type, expected: LoomDtype) -> None:
    assert _annotation_to_loom_type(py_type) == expected


def test_annotation_list_of_str() -> None:
    assert _annotation_to_loom_type(list[str]) == ListType(inner=LoomDtype.UTF8)


def test_annotation_nested_class() -> None:
    result = _annotation_to_loom_type(NestedAddress)
    assert result == StructType(
        fields=(
            StructField("street", LoomDtype.UTF8),
            StructField("zip_code", LoomDtype.UTF8),
        )
    )


def test_annotation_unknown_type_raises() -> None:
    with pytest.raises(TypeError, match="Cannot convert annotation"):
        _annotation_to_loom_type(object)


# ---------------------------------------------------------------------------
# _annotation_to_loom_type — Optional / Union stripping
# ---------------------------------------------------------------------------


def test_annotation_optional_new_syntax() -> None:
    assert _annotation_to_loom_type(str | None) == LoomDtype.UTF8


def test_resolve_schema_class_with_optional_fields() -> None:
    result = resolve_schema(RowWithOptional)
    assert result == (
        ColumnSchema("id", LoomDtype.INT64),
        ColumnSchema("label", LoomDtype.UTF8),
    )


# ---------------------------------------------------------------------------
# _strip_optional
# ---------------------------------------------------------------------------


def test_strip_optional_new_syntax() -> None:
    is_opt, inner = _strip_optional(str | None)
    assert is_opt is True
    assert inner is str


def test_strip_optional_non_optional() -> None:
    is_opt, inner = _strip_optional(int)
    assert is_opt is False
    assert inner is int


def test_strip_optional_multi_union_not_stripped() -> None:
    # str | int | None — two non-None members, should NOT strip
    is_opt, _ = _strip_optional(str | int | None)
    assert is_opt is False


# ---------------------------------------------------------------------------
# resolve_json_type — existing LoomType variants passed through
# ---------------------------------------------------------------------------


def test_resolve_json_type_passthrough_datetime_type() -> None:
    dt = DatetimeType("us", "UTC")
    assert resolve_json_type(dt) is dt
