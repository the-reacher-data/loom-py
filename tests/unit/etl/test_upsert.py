"""Unit tests for shared UPSERT/MERGE helpers in loom.etl._upsert.

Pure function tests — no I/O, no backend dependency.
"""

from __future__ import annotations

import pytest

from loom.etl._format import Format
from loom.etl._table import TableRef
from loom.etl._target import TargetSpec, WriteMode
from loom.etl._upsert import (
    SOURCE_ALIAS,
    TARGET_ALIAS,
    _build_insert_values,
    _build_join_clause,
    _build_partition_literal_filter,
    _build_single_partition_combo_clause,
    _build_update_set,
    _build_upsert_predicate,
    _build_upsert_update_cols,
    _sql_literal,
)


def _upsert_spec(
    keys: tuple[str, ...] = ("id",),
    partition_cols: tuple[str, ...] = (),
    exclude: tuple[str, ...] = (),
    include: tuple[str, ...] = (),
) -> TargetSpec:
    return TargetSpec(
        mode=WriteMode.UPSERT,
        format=Format.DELTA,
        table_ref=TableRef("test.table"),
        upsert_keys=keys,
        partition_cols=partition_cols,
        upsert_exclude=exclude,
        upsert_include=include,
    )


# ---------------------------------------------------------------------------
# _sql_literal
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        ("hello", "'hello'"),
        ("it's", "'it''s'"),
        (True, "TRUE"),
        (False, "FALSE"),
        (42, "42"),
        (3.14, "3.14"),
        (None, "None"),
    ],
)
def test_sql_literal(value: object, expected: str) -> None:
    assert _sql_literal(value) == expected


# ---------------------------------------------------------------------------
# _build_join_clause
# ---------------------------------------------------------------------------


def test_build_join_clause_keys_only() -> None:
    result = _build_join_clause(("id",), (), "t", "s")
    assert result == "t.id = s.id"


def test_build_join_clause_keys_and_partitions() -> None:
    result = _build_join_clause(("id",), ("year", "month"), "t", "s")
    assert result == "t.id = s.id AND t.year = s.year AND t.month = s.month"


def test_build_join_clause_multiple_keys() -> None:
    result = _build_join_clause(("order_id", "line_id"), (), "t", "s")
    assert result == "t.order_id = s.order_id AND t.line_id = s.line_id"


# ---------------------------------------------------------------------------
# _build_single_partition_combo_clause
# ---------------------------------------------------------------------------


def test_build_single_partition_combo_clause_single_col() -> None:
    combo = {"year": 2023}
    result = _build_single_partition_combo_clause(combo, ("year",), "t")
    assert result == "(t.year = 2023)"


def test_build_single_partition_combo_clause_multi_col() -> None:
    combo = {"year": 2023, "month": 1}
    result = _build_single_partition_combo_clause(combo, ("year", "month"), "t")
    assert result == "(t.year = 2023 AND t.month = 1)"


def test_build_single_partition_combo_clause_string_value() -> None:
    combo = {"region": "eu-west"}
    result = _build_single_partition_combo_clause(combo, ("region",), "t")
    assert result == "(t.region = 'eu-west')"


# ---------------------------------------------------------------------------
# _build_partition_literal_filter
# ---------------------------------------------------------------------------


def test_build_partition_literal_filter_single_combo() -> None:
    combos = [{"year": 2023, "month": 1}]
    result = _build_partition_literal_filter(combos, ("year", "month"), "t")
    assert result == "(t.year = 2023 AND t.month = 1)"


def test_build_partition_literal_filter_multiple_combos() -> None:
    combos = [{"year": 2023, "month": 1}, {"year": 2024, "month": 3}]
    result = _build_partition_literal_filter(combos, ("year", "month"), "t")
    assert result == "(t.year = 2023 AND t.month = 1) OR (t.year = 2024 AND t.month = 3)"


# ---------------------------------------------------------------------------
# _build_upsert_predicate
# ---------------------------------------------------------------------------


def test_build_upsert_predicate_no_partitions() -> None:
    spec = _upsert_spec(keys=("id",))
    result = _build_upsert_predicate([], spec, TARGET_ALIAS, SOURCE_ALIAS)
    assert result == "t.id = s.id"


def test_build_upsert_predicate_with_partitions_and_combos() -> None:
    spec = _upsert_spec(keys=("id",), partition_cols=("year", "month"))
    combos = [{"year": 2023, "month": 1}]
    result = _build_upsert_predicate(combos, spec, TARGET_ALIAS, SOURCE_ALIAS)
    key_clause = "t.id = s.id AND t.year = s.year AND t.month = s.month"
    assert result == f"((t.year = 2023 AND t.month = 1)) AND ({key_clause})"


def test_build_upsert_predicate_empty_combos_with_partition_cols() -> None:
    """When combos list is empty (e.g. empty frame), skip the partition filter."""
    spec = _upsert_spec(keys=("id",), partition_cols=("year",))
    result = _build_upsert_predicate([], spec, TARGET_ALIAS, SOURCE_ALIAS)
    assert result == "t.id = s.id AND t.year = s.year"


# ---------------------------------------------------------------------------
# _build_upsert_update_cols
# ---------------------------------------------------------------------------


def test_build_upsert_update_cols_defaults_excludes_keys() -> None:
    spec = _upsert_spec(keys=("id",))
    cols = _build_upsert_update_cols(("id", "name", "value"), spec)
    assert cols == ("name", "value")


def test_build_upsert_update_cols_excludes_partition_cols() -> None:
    spec = _upsert_spec(keys=("id",), partition_cols=("year", "month"))
    cols = _build_upsert_update_cols(("id", "year", "month", "name"), spec)
    assert cols == ("name",)


def test_build_upsert_update_cols_with_exclude() -> None:
    spec = _upsert_spec(keys=("id",), exclude=("created_at",))
    cols = _build_upsert_update_cols(("id", "name", "created_at", "updated_at"), spec)
    assert cols == ("name", "updated_at")


def test_build_upsert_update_cols_with_include() -> None:
    spec = _upsert_spec(keys=("id",), include=("name", "value"))
    cols = _build_upsert_update_cols(("id", "name", "value", "created_at"), spec)
    assert cols == ("name", "value")


def test_build_upsert_update_cols_include_drops_keys() -> None:
    """include= should not override the always-excluded keys."""
    spec = _upsert_spec(keys=("id",), include=("id", "name"))
    cols = _build_upsert_update_cols(("id", "name", "value"), spec)
    assert cols == ("name",)


# ---------------------------------------------------------------------------
# _build_update_set and _build_insert_values
# ---------------------------------------------------------------------------


def test_build_update_set() -> None:
    result = _build_update_set(("name", "value"), "s")
    assert result == {"name": "s.name", "value": "s.value"}


def test_build_insert_values() -> None:
    result = _build_insert_values(("id", "name", "year"), "s")
    assert result == {"id": "s.id", "name": "s.name", "year": "s.year"}
