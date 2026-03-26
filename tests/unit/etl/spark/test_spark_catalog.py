"""Tests for SparkCatalog — mock-based, no JVM required."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import types as T  # noqa: E402

from loom.etl._schema import ColumnSchema, LoomDtype  # noqa: E402
from loom.etl._table import TableRef  # noqa: E402
from loom.etl.backends.spark._catalog import SparkCatalog  # noqa: E402


def _field(name: str, dtype: T.DataType, nullable: bool = True) -> MagicMock:
    field = MagicMock()
    field.name = name
    field.dataType = dtype
    field.nullable = nullable
    return field


def _mock_spark(tables: dict[str, list[MagicMock]]) -> MagicMock:
    spark = MagicMock()
    spark.catalog.tableExists.side_effect = lambda name: name in tables

    def _table(name: str) -> MagicMock:
        df = MagicMock()
        fields = tables[name]
        df.columns = [f.name for f in fields]
        schema = MagicMock()
        schema.fields = fields
        df.schema = schema
        return df

    spark.table.side_effect = _table
    return spark


_ID_AMOUNT_FIELDS = [_field("id", T.LongType()), _field("amount", T.DoubleType())]
_SCHEMA_FIELDS = [
    _field("id", T.LongType(), nullable=False),
    _field("amount", T.DoubleType()),
    _field("name", T.StringType()),
]


class TestExists:
    @pytest.mark.parametrize(
        ("tables", "ref", "expected"),
        [
            ({"raw.orders": []}, "raw.orders", True),
            ({}, "raw.orders", False),
        ],
    )
    def test_exists_returns_expected_flag(
        self,
        tables: dict[str, list[MagicMock]],
        ref: str,
        expected: bool,
    ) -> None:
        catalog = SparkCatalog(_mock_spark(tables))
        assert catalog.exists(TableRef(ref)) is expected

    def test_exists_delegates_ref_string_to_spark(self) -> None:
        spark = _mock_spark({"main.raw.orders": []})
        SparkCatalog(spark).exists(TableRef("main.raw.orders"))
        spark.catalog.tableExists.assert_called_once_with("main.raw.orders")


class TestColumns:
    @pytest.mark.parametrize(
        ("tables", "expected", "table_calls"),
        [
            ({"raw.orders": _ID_AMOUNT_FIELDS}, ("id", "amount"), 1),
            ({}, (), 0),
        ],
    )
    def test_columns_returns_expected_values(
        self,
        tables: dict[str, list[MagicMock]],
        expected: tuple[str, ...],
        table_calls: int,
    ) -> None:
        spark = _mock_spark(tables)
        catalog = SparkCatalog(spark)

        assert catalog.columns(TableRef("raw.orders")) == expected
        assert spark.table.call_count == table_calls


class TestSchema:
    @pytest.mark.parametrize(
        ("tables", "expected", "table_calls"),
        [
            ({}, None, 0),
            (
                {"raw.orders": _SCHEMA_FIELDS},
                (
                    ColumnSchema("id", LoomDtype.INT64, nullable=False),
                    ColumnSchema("amount", LoomDtype.FLOAT64, nullable=True),
                    ColumnSchema("name", LoomDtype.UTF8, nullable=True),
                ),
                1,
            ),
        ],
    )
    def test_schema_returns_expected_values(
        self,
        tables: dict[str, list[MagicMock]],
        expected: tuple[ColumnSchema, ...] | None,
        table_calls: int,
    ) -> None:
        spark = _mock_spark(tables)
        catalog = SparkCatalog(spark)

        assert catalog.schema(TableRef("raw.orders")) == expected
        assert spark.table.call_count == table_calls

    def test_schema_maps_all_common_spark_types(self) -> None:
        fields = [
            _field("a", T.ByteType()),
            _field("b", T.ShortType()),
            _field("c", T.IntegerType()),
            _field("d", T.LongType()),
            _field("e", T.FloatType()),
            _field("f", T.DoubleType()),
            _field("g", T.StringType()),
            _field("h", T.BooleanType()),
            _field("i", T.DateType()),
            _field("j", T.TimestampType()),
        ]
        catalog = SparkCatalog(_mock_spark({"t": fields}))

        result = catalog.schema(TableRef("t"))
        assert result is not None
        assert [col.dtype for col in result] == [
            LoomDtype.INT8,
            LoomDtype.INT16,
            LoomDtype.INT32,
            LoomDtype.INT64,
            LoomDtype.FLOAT32,
            LoomDtype.FLOAT64,
            LoomDtype.UTF8,
            LoomDtype.BOOLEAN,
            LoomDtype.DATE,
            LoomDtype.DATETIME,
        ]


class TestNoopAndProtocol:
    def test_update_schema_is_noop(self) -> None:
        spark = _mock_spark({})
        catalog = SparkCatalog(spark)

        catalog.update_schema(TableRef("raw.orders"), (ColumnSchema("id", LoomDtype.INT64),))

        spark.table.assert_not_called()
        spark.catalog.tableExists.assert_not_called()

    def test_spark_catalog_satisfies_table_discovery_protocol(self) -> None:
        from loom.etl._io import TableDiscovery

        catalog = SparkCatalog(_mock_spark({}))
        assert isinstance(catalog, TableDiscovery)
