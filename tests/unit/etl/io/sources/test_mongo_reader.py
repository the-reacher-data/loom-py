"""Tests for MongoSourceReader using hand-rolled fake pymongo objects.

No live MongoDB required. Filter correctness is tested in test_mongo_predicate.py.
Here we verify that the reader passes the right args to pymongo and converts
documents correctly to Polars DataFrames.
"""

from __future__ import annotations

import logging
from datetime import UTC
from typing import Any

import msgspec
import polars as pl
import pytest
from bson import ObjectId
from bson.decimal128 import Decimal128

from loom.etl.declarative.expr import col
from loom.etl.declarative.source._specs import MongoSourceSpec
from loom.etl.io.sources._mongo import MongoSourceReader

# ---------------------------------------------------------------------------
# Fake pymongo client stack
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, docs: list[dict]) -> None:
        self._docs = list(docs)
        self.filter_used: dict = {}
        self.projection_used: dict | None = None
        self.batch_size_used: int | None = None
        self._limit: int | None = None

    def limit(self, n: int) -> _FakeCursor:
        self._limit = n
        return self

    def __iter__(self):
        docs = self._docs if self._limit is None else self._docs[: self._limit]
        return iter(docs)


class _FakeCollection:
    def __init__(self, docs: list[dict]) -> None:
        self._docs = docs
        self._cursor: _FakeCursor | None = None

    def find(
        self,
        filter: dict | None = None,
        projection: dict | None = None,
        batch_size: int = 100,
    ) -> _FakeCursor:
        cursor = _FakeCursor(self._docs)
        cursor.filter_used = filter or {}
        cursor.projection_used = projection
        cursor.batch_size_used = batch_size
        self._cursor = cursor
        return cursor


class _FakeDatabase:
    def __init__(self, collections: dict[str, list[dict]]) -> None:
        self._cols = collections
        self.collections: dict[str, _FakeCollection] = {}

    def __getitem__(self, name: str) -> _FakeCollection:
        if name not in self.collections:
            self.collections[name] = _FakeCollection(self._cols.get(name, []))
        return self.collections[name]


class _FakeClient:
    def __init__(self, dbs: dict[str, dict[str, list[dict]]]) -> None:
        self._dbs = dbs
        self.databases: dict[str, _FakeDatabase] = {}
        self.closed = False

    def __getitem__(self, db_name: str) -> _FakeDatabase:
        if db_name not in self.databases:
            self.databases[db_name] = _FakeDatabase(self._dbs.get(db_name, {}))
        return self.databases[db_name]

    def close(self) -> None:
        self.closed = True


# ---------------------------------------------------------------------------
# Schema for schema-related tests
# ---------------------------------------------------------------------------


class OrderDoc(msgspec.Struct):
    order_id: str
    status: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ORDERS = [
    {"order_id": "o1", "status": "active"},
    {"order_id": "o2", "status": "pending"},
]


def _reader(
    docs: list[dict], *, db: str = "app", collection: str = "orders"
) -> tuple[MongoSourceReader, _FakeClient]:
    client = _FakeClient({db: {collection: docs}})
    return MongoSourceReader(client, db), client


def _spec(**kwargs: Any) -> MongoSourceSpec:
    return MongoSourceSpec(alias="orders", collection="orders", **kwargs)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_returns_dataframe(self) -> None:
        reader, _ = _reader(_ORDERS)
        df = reader.read(_spec(), None)
        assert isinstance(df, pl.DataFrame)
        assert df.shape == (2, 2)
        assert set(df.columns) == {"order_id", "status"}

    def test_values_preserved(self) -> None:
        reader, _ = _reader(_ORDERS)
        df = reader.read(_spec(), None)
        assert df["order_id"].to_list() == ["o1", "o2"]
        assert df["status"].to_list() == ["active", "pending"]


# ---------------------------------------------------------------------------
# pymongo call args
# ---------------------------------------------------------------------------


class TestCallArgs:
    def test_no_filter_passes_empty_dict(self) -> None:
        reader, client = _reader(_ORDERS)
        reader.read(_spec(filter=None), None)
        cursor = client["app"]["orders"]._cursor
        assert cursor is not None
        assert cursor.filter_used == {}

    def test_filter_forwarded(self) -> None:
        reader, client = _reader(_ORDERS)
        reader.read(_spec(filter=col("status") == "active"), None)
        cursor = client["app"]["orders"]._cursor
        assert cursor is not None
        assert cursor.filter_used == {"status": "active"}

    def test_projection_forwarded(self) -> None:
        reader, client = _reader(_ORDERS)
        reader.read(_spec(projection=("order_id", "status")), None)
        cursor = client["app"]["orders"]._cursor
        assert cursor is not None
        assert cursor.projection_used == {"order_id": 1, "status": 1}

    def test_no_projection_passes_none(self) -> None:
        reader, client = _reader(_ORDERS)
        reader.read(_spec(projection=None), None)
        cursor = client["app"]["orders"]._cursor
        assert cursor is not None
        assert cursor.projection_used is None

    def test_batch_size_forwarded(self) -> None:
        reader, client = _reader(_ORDERS)
        reader.read(_spec(batch_size=50), None)
        cursor = client["app"]["orders"]._cursor
        assert cursor is not None
        assert cursor.batch_size_used == 50


# ---------------------------------------------------------------------------
# limit
# ---------------------------------------------------------------------------


class TestLimit:
    def test_limit_applied(self) -> None:
        docs = [{"n": i} for i in range(10)]
        reader, _ = _reader(docs)
        df = reader.read(_spec(limit=3), None)
        assert df.shape[0] == 3

    def test_no_limit_returns_all(self) -> None:
        docs = [{"n": i} for i in range(5)]
        reader, _ = _reader(docs)
        df = reader.read(_spec(limit=None), None)
        assert df.shape[0] == 5


# ---------------------------------------------------------------------------
# Empty collection
# ---------------------------------------------------------------------------


class TestEmptyCollection:
    def test_empty_returns_empty_dataframe(self) -> None:
        reader, _ = _reader([])
        df = reader.read(_spec(), None)
        assert isinstance(df, pl.DataFrame)
        assert df.shape[0] == 0

    def test_empty_with_schema_returns_correct_columns(self) -> None:
        reader, _ = _reader([])
        df = reader.read(_spec(schema_type=OrderDoc), None)
        assert set(df.columns) == {"order_id", "status"}
        assert df.shape[0] == 0


# ---------------------------------------------------------------------------
# BSON type normalization
# ---------------------------------------------------------------------------


class TestBsonTypes:
    def test_objectid_normalized_to_str(self) -> None:
        oid = ObjectId("507f1f77bcf86cd799439011")
        reader, _ = _reader([{"_id": oid, "name": "foo"}])
        df = reader.read(_spec(), None)
        assert df["_id"].dtype == pl.String
        assert df["_id"][0] == "507f1f77bcf86cd799439011"

    def test_decimal128_normalized_to_float(self) -> None:
        reader, _ = _reader([{"amount": Decimal128("3.14"), "n": 1}])
        df = reader.read(_spec(), None)
        assert df["amount"].dtype == pl.Float64
        assert abs(df["amount"][0] - 3.14) < 1e-10

    def test_nested_objectid_in_subdoc(self) -> None:
        oid = ObjectId("507f1f77bcf86cd799439011")
        reader, _ = _reader([{"meta": {"ref_id": oid}, "n": 1}])
        df = reader.read(_spec(), None)
        assert isinstance(df["meta"][0], dict)
        assert df["meta"][0]["ref_id"] == "507f1f77bcf86cd799439011"

    def test_mixed_types_no_crash(self) -> None:
        from datetime import datetime

        reader, _ = _reader(
            [
                {
                    "_id": ObjectId("507f1f77bcf86cd799439011"),
                    "amount": Decimal128("100.00"),
                    "created_at": datetime(2024, 1, 1, tzinfo=UTC),
                    "tags": ["a", "b"],
                }
            ]
        )
        df = reader.read(_spec(), None)
        assert df.shape[0] == 1
        assert df["_id"].dtype == pl.String
        assert df["amount"].dtype == pl.Float64


# ---------------------------------------------------------------------------
# extra_fields_mode
# ---------------------------------------------------------------------------


class TestExtraFieldsMode:
    def test_error_mode_raises_on_extra_field(self) -> None:
        docs = [{"order_id": "o1", "status": "active", "extra_col": "surprise"}]
        reader, _ = _reader(docs)
        with pytest.raises(ValueError, match="extra_col"):
            reader.read(_spec(schema_type=OrderDoc, extra_fields_mode="error"), None)

    def test_ignore_mode_drops_extra_field(self) -> None:
        docs = [{"order_id": "o1", "status": "active", "extra_col": "surprise"}]
        reader, _ = _reader(docs)
        df = reader.read(_spec(schema_type=OrderDoc, extra_fields_mode="ignore"), None)
        assert "extra_col" not in df.columns
        assert set(df.columns) == {"order_id", "status"}

    def test_warn_mode_drops_extra_field_and_logs(self, caplog: pytest.LogCaptureFixture) -> None:
        docs = [{"order_id": "o1", "status": "active", "extra_col": "surprise"}]
        reader, _ = _reader(docs)
        with caplog.at_level(logging.WARNING):
            df = reader.read(_spec(schema_type=OrderDoc, extra_fields_mode="warn"), None)
        assert "extra_col" not in df.columns
        assert any("extra_col" in msg for msg in caplog.messages)

    def test_no_extra_fields_no_error(self) -> None:
        docs = [{"order_id": "o1", "status": "active"}]
        reader, _ = _reader(docs)
        df = reader.read(_spec(schema_type=OrderDoc, extra_fields_mode="error"), None)
        assert df.shape == (1, 2)

    def test_capture_mode_stores_extra_in_column(self) -> None:
        import json

        docs = [{"order_id": "o1", "status": "active", "extra_col": "val"}]
        reader, _ = _reader(docs)
        df = reader.read(_spec(schema_type=OrderDoc, extra_fields_mode="capture"), None)
        assert "_extra" in df.columns
        assert "extra_col" not in df.columns
        extra = json.loads(df["_extra"][0])
        assert extra["extra_col"] == "val"
