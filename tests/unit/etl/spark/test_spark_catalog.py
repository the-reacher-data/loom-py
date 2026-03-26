"""Tests for SparkCatalog — mock-based, no JVM required.

SparkSession is replaced by a MagicMock so these tests run without
a local Spark installation.  Integration tests that need a real
SparkSession live in tests/integration/.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import types as T  # noqa: E402

from loom.etl._schema import ColumnSchema, LoomDtype  # noqa: E402
from loom.etl._table import TableRef  # noqa: E402
from loom.etl.backends.spark._catalog import SparkCatalog  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _field(name: str, dtype: T.DataType, nullable: bool = True) -> MagicMock:
    f = MagicMock()
    f.name = name
    f.dataType = dtype
    f.nullable = nullable
    return f


def _mock_spark(
    tables: dict[str, list[MagicMock]],
) -> MagicMock:
    """Build a mock SparkSession with the given tables.

    Args:
        tables: Mapping of table name → list of StructField mocks.
    """
    spark = MagicMock()

    spark.catalog.tableExists.side_effect = lambda name: name in tables

    def _table(name: str) -> MagicMock:
        fields = tables[name]
        df = MagicMock()
        df.columns = [f.name for f in fields]
        schema = MagicMock()
        schema.fields = fields
        df.schema = schema
        return df

    spark.table.side_effect = _table
    return spark


# ---------------------------------------------------------------------------
# exists()
# ---------------------------------------------------------------------------


def test_exists_returns_true_for_registered_table() -> None:
    spark = _mock_spark({"raw.orders": []})
    catalog = SparkCatalog(spark)
    assert catalog.exists(TableRef("raw.orders")) is True


def test_exists_returns_false_for_missing_table() -> None:
    spark = _mock_spark({})
    catalog = SparkCatalog(spark)
    assert catalog.exists(TableRef("raw.orders")) is False


def test_exists_delegates_ref_string_to_spark() -> None:
    spark = _mock_spark({"main.raw.orders": []})
    catalog = SparkCatalog(spark)
    catalog.exists(TableRef("main.raw.orders"))
    spark.catalog.tableExists.assert_called_once_with("main.raw.orders")


# ---------------------------------------------------------------------------
# columns()
# ---------------------------------------------------------------------------


def test_columns_returns_column_names_in_order() -> None:
    fields = [_field("id", T.LongType()), _field("amount", T.DoubleType())]
    spark = _mock_spark({"raw.orders": fields})
    catalog = SparkCatalog(spark)
    assert catalog.columns(TableRef("raw.orders")) == ("id", "amount")


def test_columns_returns_empty_tuple_when_table_missing() -> None:
    spark = _mock_spark({})
    catalog = SparkCatalog(spark)
    assert catalog.columns(TableRef("raw.orders")) == ()


def test_columns_does_not_call_table_when_not_exists() -> None:
    spark = _mock_spark({})
    catalog = SparkCatalog(spark)
    catalog.columns(TableRef("raw.orders"))
    spark.table.assert_not_called()


# ---------------------------------------------------------------------------
# schema()
# ---------------------------------------------------------------------------


def test_schema_returns_none_when_table_missing() -> None:
    spark = _mock_spark({})
    catalog = SparkCatalog(spark)
    assert catalog.schema(TableRef("raw.orders")) is None


def test_schema_returns_column_schema_tuple() -> None:
    fields = [
        _field("id", T.LongType(), nullable=False),
        _field("amount", T.DoubleType()),
        _field("name", T.StringType()),
    ]
    spark = _mock_spark({"raw.orders": fields})
    catalog = SparkCatalog(spark)

    result = catalog.schema(TableRef("raw.orders"))

    assert result == (
        ColumnSchema("id", LoomDtype.INT64, nullable=False),
        ColumnSchema("amount", LoomDtype.FLOAT64, nullable=True),
        ColumnSchema("name", LoomDtype.UTF8, nullable=True),
    )


def test_schema_preserves_nullable_false() -> None:
    fields = [_field("pk", T.IntegerType(), nullable=False)]
    spark = _mock_spark({"t": fields})
    catalog = SparkCatalog(spark)
    result = catalog.schema(TableRef("t"))
    assert result is not None
    assert result[0].nullable is False


def test_schema_maps_all_common_spark_types() -> None:
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
    spark = _mock_spark({"t": fields})
    catalog = SparkCatalog(spark)
    result = catalog.schema(TableRef("t"))
    assert result is not None
    dtypes = [col.dtype for col in result]
    assert dtypes == [
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


def test_schema_does_not_call_table_when_not_exists() -> None:
    spark = _mock_spark({})
    catalog = SparkCatalog(spark)
    catalog.schema(TableRef("missing"))
    spark.table.assert_not_called()


# ---------------------------------------------------------------------------
# update_schema() — no-op contract
# ---------------------------------------------------------------------------


def test_update_schema_is_noop_and_does_not_raise() -> None:
    spark = _mock_spark({})
    catalog = SparkCatalog(spark)
    schema = (ColumnSchema("id", LoomDtype.INT64),)
    catalog.update_schema(TableRef("raw.orders"), schema)
    spark.table.assert_not_called()
    spark.catalog.tableExists.assert_not_called()


# ---------------------------------------------------------------------------
# Protocol compliance
# ---------------------------------------------------------------------------


def test_spark_catalog_satisfies_table_discovery_protocol() -> None:
    from loom.etl._io import TableDiscovery

    spark = _mock_spark({})
    catalog = SparkCatalog(spark)
    assert isinstance(catalog, TableDiscovery)
