"""Tests for MongoSourceReader using hand-rolled fake pymongo objects.

No live MongoDB required. Filter correctness is tested in test_mongo_predicate.py.
Here we verify that the reader passes the right args to pymongo and converts
documents correctly to Polars DataFrames.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import polars as pl
import pytest
from bson import ObjectId
from bson.decimal128 import Decimal128

from loom.etl.backends.polars._dtype import loom_type_to_polars
from loom.etl.declarative.expr import col
from loom.etl.declarative.source._specs import MongoSourceSpec
from loom.etl.io.sources._mongo import MongoSourceReader
from loom.etl.io.sources._mongo_batch import _canonicalize_batch
from loom.etl.schema._contract import resolve_schema
from loom.etl.schema._schema import ColumnSchema, ListType, LoomDtype, StructField, StructType

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

    def close(self) -> None:
        pass

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
# Schema classes — plain Python, no msgspec
# ---------------------------------------------------------------------------


class OrderDoc:
    order_id: str
    status: str


class ProductDoc:
    label: str


class DeepLeaf:
    refs: list[str] | None = None


class DeepNode:
    leaves: list[DeepLeaf] | None = None
    tags: list[str] | None = None


class DeepEnvelope:
    payload: DeepNode | None = None


class MetaDoc:
    name: str
    score: float


class ArchiveAsset:
    url: str
    name: str
    s3_url: str
    _id: str
    createdAt: datetime
    updatedAt: datetime


class ArchiveBundle:
    assets: list[ArchiveAsset] | None = None


class ArchiveEnvelope:
    bundle: ArchiveBundle | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ORDERS = [
    {"order_id": "o1", "status": "active"},
    {"order_id": "o2", "status": "pending"},
]

_ORDER_SCHEMA = (
    ColumnSchema("order_id", LoomDtype.UTF8),
    ColumnSchema("status", LoomDtype.UTF8),
)


def _reader(
    docs: list[dict], *, db: str = "app", collection: str = "orders"
) -> tuple[MongoSourceReader, _FakeClient]:
    client = _FakeClient({db: {collection: docs}})
    return MongoSourceReader(client, db), client


def _spec(*, collection: str = "orders", **kwargs: Any) -> MongoSourceSpec:
    return MongoSourceSpec(alias=collection, collection=collection, **kwargs)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_returns_lazy_frame(self) -> None:
        reader, _ = _reader(_ORDERS)
        result = reader.read(_spec(), None)
        assert isinstance(result, pl.LazyFrame)
        df = result.collect()
        assert df.shape == (2, 2)
        assert set(df.columns) == {"order_id", "status"}

    def test_values_preserved(self) -> None:
        reader, _ = _reader(_ORDERS)
        df = reader.read(_spec(), None).collect()
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
        df = reader.read(_spec(limit=3), None).collect()
        assert df.shape[0] == 3

    def test_no_limit_returns_all(self) -> None:
        docs = [{"n": i} for i in range(5)]
        reader, _ = _reader(docs)
        df = reader.read(_spec(limit=None), None).collect()
        assert df.shape[0] == 5


# ---------------------------------------------------------------------------
# Empty collection
# ---------------------------------------------------------------------------


class TestEmptyCollection:
    def test_empty_returns_empty_dataframe(self) -> None:
        reader, _ = _reader([])
        result = reader.read(_spec(), None)
        assert isinstance(result, pl.LazyFrame)
        df = result.collect()
        assert df.shape[0] == 0

    def test_empty_with_schema_returns_correct_columns(self) -> None:
        reader, _ = _reader([])
        df = reader.read(_spec(schema=_ORDER_SCHEMA), None).collect()
        assert set(df.columns) == {"order_id", "status"}
        assert df.shape[0] == 0

    def test_empty_with_recursive_schema_keeps_nested_types(self) -> None:
        reader, _ = _reader([])
        df = reader.read(_spec(schema=resolve_schema(DeepEnvelope)), None).collect()
        assert df.shape == (0, 1)
        assert df["payload"].dtype == pl.Struct(
            {
                "leaves": pl.List(pl.Struct({"refs": pl.List(pl.String)})),
                "tags": pl.List(pl.String),
            }
        )


# ---------------------------------------------------------------------------
# BSON type normalization
# ---------------------------------------------------------------------------


class TestBsonTypes:
    def test_objectid_normalized_to_str(self) -> None:
        oid = ObjectId("507f1f77bcf86cd799439011")
        reader, _ = _reader([{"_id": oid, "name": "foo"}])
        df = reader.read(_spec(), None).collect()
        assert df["_id"].dtype == pl.String
        assert df["_id"][0] == "507f1f77bcf86cd799439011"

    def test_decimal128_normalized_to_float(self) -> None:
        reader, _ = _reader([{"amount": Decimal128("3.14"), "n": 1}])
        df = reader.read(_spec(), None).collect()
        assert df["amount"].dtype == pl.Float64
        assert abs(df["amount"][0] - 3.14) < 1e-10

    def test_nested_objectid_in_subdoc(self) -> None:
        oid = ObjectId("507f1f77bcf86cd799439011")
        reader, _ = _reader([{"meta": {"ref_id": oid}, "n": 1}])
        df = reader.read(_spec(), None).collect()
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
        df = reader.read(_spec(), None).collect()
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
        with pytest.raises((ValueError, pl.exceptions.ComputeError), match="extra_col"):
            reader.read(_spec(schema=_ORDER_SCHEMA, extra_fields_mode="error"), None).collect()

    def test_ignore_mode_drops_extra_field(self) -> None:
        docs = [{"order_id": "o1", "status": "active", "extra_col": "surprise"}]
        reader, _ = _reader(docs)
        df = reader.read(_spec(schema=_ORDER_SCHEMA, extra_fields_mode="ignore"), None).collect()
        assert "extra_col" not in df.columns
        assert set(df.columns) == {"order_id", "status"}

    def test_warn_mode_drops_extra_field_and_logs(self, caplog: pytest.LogCaptureFixture) -> None:
        docs = [{"order_id": "o1", "status": "active", "extra_col": "surprise"}]
        reader, _ = _reader(docs)
        with caplog.at_level(logging.WARNING):
            df = reader.read(_spec(schema=_ORDER_SCHEMA, extra_fields_mode="warn"), None).collect()
        assert "extra_col" not in df.columns
        assert any("extra_col" in msg for msg in caplog.messages)

    def test_no_extra_fields_no_error(self) -> None:
        docs = [{"order_id": "o1", "status": "active"}]
        reader, _ = _reader(docs)
        df = reader.read(_spec(schema=_ORDER_SCHEMA, extra_fields_mode="error"), None).collect()
        assert df.shape == (1, 2)

    def test_capture_mode_stores_extra_in_column(self) -> None:
        import json

        docs = [{"order_id": "o1", "status": "active", "extra_col": "val"}]
        reader, _ = _reader(docs)
        df = reader.read(_spec(schema=_ORDER_SCHEMA, extra_fields_mode="capture"), None).collect()
        assert "_extra" in df.columns
        assert "extra_col" not in df.columns
        extra = json.loads(df["_extra"][0])
        assert extra["extra_col"] == "val"


# ---------------------------------------------------------------------------
# Heterogeneous types within a single batch
# ---------------------------------------------------------------------------


class TestHeterogeneousTypesWithinBatch:
    def test_dict_vs_string_same_field(self) -> None:
        import json

        docs = [
            {"id": 1, "attributes": {"weight": 12, "color": "red"}},
            {"id": 2, "attributes": "see label"},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df["attributes"].dtype == pl.String
        first = df.filter(pl.col("id") == 1)["attributes"][0]
        parsed = json.loads(first)
        assert parsed == {"weight": 12, "color": "red"}

    def test_int_vs_string_same_field(self, caplog: pytest.LogCaptureFixture) -> None:
        import json

        docs = [
            {"id": 1, "year": 2020},
            {"id": 2, "year": "unknown"},
        ]
        reader, _ = _reader(docs)
        with caplog.at_level(logging.WARNING):
            df = reader.read(_spec(), None).collect()
        assert df["year"].dtype == pl.String
        int_row = df.filter(pl.col("id") == 1)["year"][0]
        assert json.loads(int_row) == 2020
        assert any("year" in msg for msg in caplog.messages)

    def test_list_of_strings_vs_list_of_dicts(self) -> None:
        import json

        docs = [
            {"id": 1, "tags": ["sport"]},
            {"id": 2, "tags": [{"name": "sport"}]},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df["tags"].dtype == pl.String
        first = df.filter(pl.col("id") == 1)["tags"][0]
        parsed = json.loads(first)
        assert parsed == ["sport"]

    def test_null_does_not_create_conflict(self, caplog: pytest.LogCaptureFixture) -> None:
        docs = [
            {"id": 1, "attributes": {"weight": 12}},
            {"id": 2, "attributes": None},
            {"id": 3, "attributes": {"weight": 30}},
        ]
        reader, _ = _reader(docs)
        with caplog.at_level(logging.WARNING):
            df = reader.read(_spec(), None).collect()
        assert df["attributes"].dtype == pl.Struct
        assert df["attributes"][1] is None
        assert not any("attributes" in msg for msg in caplog.messages)

    def test_uniform_field_not_serialized(self) -> None:
        docs = [{"id": i, "price": 5000} for i in range(4)]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df["price"].dtype == pl.Int64
        assert df["price"][0] == 5000

    def test_warning_emitted_on_conflict(self, caplog: pytest.LogCaptureFixture) -> None:
        docs = [
            {"id": 1, "attributes": {"weight": 12}},
            {"id": 2, "attributes": "see label"},
        ]
        reader, _ = _reader(docs)
        with caplog.at_level(logging.WARNING):
            reader.read(_spec(), None)
        assert any("attributes" in msg for msg in caplog.messages)


# ---------------------------------------------------------------------------
# Heterogeneous types across batches
# ---------------------------------------------------------------------------


class TestHeterogeneousTypesAcrossBatches:
    def test_struct_in_batch1_string_in_batch2(self) -> None:
        import json

        docs = [
            {"id": 1, "attributes": {"weight": 10}},
            {"id": 2, "attributes": {"weight": 20}},
            {"id": 3, "attributes": "n/a"},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(batch_size=2), None).collect()
        assert df.shape[0] == 3
        assert df["attributes"].dtype == pl.String
        first = df.filter(pl.col("id") == 1)["attributes"][0]
        parsed = json.loads(first)
        assert parsed == {"weight": 10}

    def test_warning_emitted_on_cross_batch_conflict(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        docs = [
            {"id": 1, "attributes": {"weight": 10}},
            {"id": 2, "attributes": {"weight": 20}},
            {"id": 3, "attributes": "n/a"},
        ]
        reader, _ = _reader(docs)
        with caplog.at_level(logging.WARNING):
            reader.read(_spec(batch_size=2), None)
        assert any("attributes" in msg for msg in caplog.messages)


# ---------------------------------------------------------------------------
# Schema-guided coercion of fields declared as UTF8
# ---------------------------------------------------------------------------


class TestSchemaGuidedCoercion:
    def test_struct_coerced_to_str_when_schema_declares_utf8(self) -> None:
        import json

        docs = [
            {"label": {"short": "A1", "long": "Alpha One"}},
            {"label": {"short": "B2", "long": "Beta Two"}},
        ]
        reader, _ = _reader(docs)
        schema = (ColumnSchema("label", LoomDtype.UTF8),)
        df = reader.read(_spec(schema=schema), None).collect()
        assert df["label"].dtype == pl.String
        parsed = json.loads(df["label"][0])
        assert parsed == {"short": "A1", "long": "Alpha One"}

    def test_warning_emitted_on_schema_coercion(self, caplog: pytest.LogCaptureFixture) -> None:
        docs = [
            {"label": {"short": "A1", "long": "Alpha One"}},
            {"label": {"short": "B2", "long": "Beta Two"}},
        ]
        reader, _ = _reader(docs)
        schema = (ColumnSchema("label", LoomDtype.UTF8),)
        with caplog.at_level(logging.WARNING):
            reader.read(_spec(schema=schema), None).collect()
        assert any("label" in msg for msg in caplog.messages)

    def test_recursive_schema_materialises_nested_empty_lists(self) -> None:
        docs = [
            {"payload": {"leaves": [{"refs": []}], "tags": []}},
            {"payload": {"leaves": [{"refs": []}], "tags": []}},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(schema=resolve_schema(DeepEnvelope)), None).collect()
        assert df["payload"].dtype == pl.Struct(
            {
                "leaves": pl.List(pl.Struct({"refs": pl.List(pl.String)})),
                "tags": pl.List(pl.String),
            }
        )
        assert "Null" not in str(df["payload"].dtype)

    def test_recursive_schema_canonicalizes_nested_struct_key_order(self) -> None:
        docs = [
            {
                "bundle": {
                    "assets": [
                        {
                            "_id": "asset-1",
                            "name": "scan",
                            "url": "https://example.invalid/a",
                            "s3_url": "s3://bucket/a",
                            "updatedAt": datetime(2024, 5, 1, tzinfo=UTC),
                            "createdAt": datetime(2024, 5, 1, tzinfo=UTC),
                        }
                    ]
                }
            },
            {
                "bundle": {
                    "assets": [
                        {
                            "url": "https://example.invalid/b",
                            "name": "scan-b",
                            "s3_url": "s3://bucket/b",
                            "_id": "asset-2",
                            "createdAt": datetime(2024, 5, 2, tzinfo=UTC),
                            "updatedAt": datetime(2024, 5, 2, tzinfo=UTC),
                        }
                    ]
                }
            },
        ]
        reader, _ = _reader(docs, collection="archives")
        df = reader.read(
            _spec(collection="archives", schema=resolve_schema(ArchiveEnvelope)), None
        ).collect()
        assets = df["bundle"][0]["assets"]
        assert isinstance(assets, list)
        first = assets[0]
        assert list(first.keys()) == [
            "url",
            "name",
            "s3_url",
            "_id",
            "createdAt",
            "updatedAt",
        ]
        assert first["url"] == "https://example.invalid/a"

    def test_canonicalize_batch_reorders_nested_structs_without_ordered_dict(self) -> None:
        schema = resolve_schema(ArchiveEnvelope)
        declared = {"bundle": loom_type_to_polars(schema[0].dtype)}
        batch = [
            {
                "bundle": {
                    "assets": [
                        {
                            "_id": "asset-1",
                            "name": "scan",
                            "url": "https://example.invalid/a",
                            "s3_url": "s3://bucket/a",
                            "updatedAt": datetime(2024, 5, 1, tzinfo=UTC),
                            "createdAt": datetime(2024, 5, 1, tzinfo=UTC),
                        }
                    ]
                }
            }
        ]

        canonicalized = _canonicalize_batch(batch, declared)

        assets = canonicalized[0]["bundle"]["assets"]
        assert isinstance(assets, list)
        assert list(assets[0].keys()) == [
            "url",
            "name",
            "s3_url",
            "_id",
            "createdAt",
            "updatedAt",
        ]

    def test_canonicalize_batch_sorts_extra_nested_dict_fields(self) -> None:
        docs = [
            {
                "bundle": {
                    "assets": [
                        {
                            "_id": "asset-1",
                            "name": "scan",
                            "url": "https://example.invalid/a",
                            "s3_url": "s3://bucket/a",
                            "updatedAt": datetime(2024, 5, 1, tzinfo=UTC),
                            "createdAt": datetime(2024, 5, 1, tzinfo=UTC),
                        }
                    ],
                    "metadata": {"beta": 2, "alpha": 1},
                }
            },
            {
                "bundle": {
                    "assets": [
                        {
                            "url": "https://example.invalid/b",
                            "name": "scan-b",
                            "s3_url": "s3://bucket/b",
                            "_id": "asset-2",
                            "createdAt": datetime(2024, 5, 2, tzinfo=UTC),
                            "updatedAt": datetime(2024, 5, 2, tzinfo=UTC),
                        }
                    ],
                    "metadata": {"alpha": 3, "beta": 4},
                }
            },
        ]
        reader, _ = _reader(docs, collection="archives")
        df = reader.read(
            _spec(collection="archives", schema=resolve_schema(ArchiveEnvelope)), None
        ).collect()
        assert "metadata" not in df["bundle"][0]

    def test_extra_complex_fields_are_serialized_before_inference(self) -> None:
        docs = [
            {
                "bundle": {"assets": []},
                "attachments": [
                    {
                        "_id": "asset-1",
                        "name": "scan",
                        "url": "https://example.invalid/a",
                        "s3_url": "s3://bucket/a",
                        "updatedAt": datetime(2024, 5, 1, tzinfo=UTC),
                        "createdAt": datetime(2024, 5, 1, tzinfo=UTC),
                    }
                ],
            },
            {
                "bundle": {"assets": []},
                "attachments": [
                    {
                        "url": "https://example.invalid/b",
                        "name": "scan-b",
                        "s3_url": "s3://bucket/b",
                        "_id": "asset-2",
                        "createdAt": datetime(2024, 5, 2, tzinfo=UTC),
                        "updatedAt": datetime(2024, 5, 2, tzinfo=UTC),
                    }
                ],
            },
        ]
        reader, _ = _reader(docs, collection="archives")
        df = reader.read(
            _spec(collection="archives", schema=resolve_schema(ArchiveEnvelope)), None
        ).collect()
        assert "attachments" not in df.columns


# ---------------------------------------------------------------------------
# Nested / deep heterogeneous types
# ---------------------------------------------------------------------------


class TestNestedHeterogeneousTypes:
    """Conflicts inside nested dicts/lists that are invisible at root level."""

    def test_list_of_dicts_with_mixed_nested_field(self) -> None:
        import json

        docs = [
            {"id": 1, "ad": {"leads": [{"origin": {"channel": "search", "paid": True}}]}},
            {"id": 2, "ad": {"leads": [{"origin": "direct"}]}},
            {"id": 3, "ad": {"leads": [{"origin": {"channel": "email", "paid": False}}]}},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df.shape[0] == 3
        assert df["ad"].dtype == pl.String
        parsed = json.loads(df["ad"][0])
        assert parsed["leads"][0]["origin"]["channel"] == "search"

    def test_nested_dict_field_with_scalar_type_conflict(self) -> None:
        import json

        docs = [
            {"id": 1, "pricing": {"amount": 1500, "currency": "EUR"}},
            {"id": 2, "pricing": {"amount": 1499.99, "currency": "EUR"}},
            {"id": 3, "pricing": {"amount": "consult", "currency": "EUR"}},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df.shape[0] == 3
        assert df["pricing"].dtype == pl.String
        row = json.loads(df["pricing"][2])
        assert row["amount"] == "consult"

    def test_list_of_scalars_not_serialised_when_uniform(self) -> None:
        docs = [
            {"id": 1, "tags": ["news", "featured"], "ad": {"leads": [{"origin": {"ch": "a"}}]}},
            {"id": 2, "tags": ["sale"], "ad": {"leads": [{"origin": "direct"}]}},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df.shape[0] == 2
        assert df["tags"].dtype == pl.List(pl.String)
        assert df["ad"].dtype == pl.String

    def test_warning_emitted_for_nested_conflict(self, caplog: pytest.LogCaptureFixture) -> None:
        docs = [
            {"id": 1, "ad": {"leads": [{"origin": {"channel": "search"}}]}},
            {"id": 2, "ad": {"leads": [{"origin": "direct"}]}},
        ]
        reader, _ = _reader(docs)
        with caplog.at_level(logging.WARNING):
            reader.read(_spec(), None)
        assert any("ad" in msg for msg in caplog.messages)
        assert any("nested" in msg.lower() for msg in caplog.messages)

    def test_non_conflicting_nested_dict_stays_as_struct(self) -> None:
        docs = [
            {"id": 1, "location": {"city": "Madrid", "zip": "28001"}},
            {"id": 2, "location": {"city": "BCN", "zip": "08001"}},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df["location"].dtype == pl.Struct({"city": pl.String, "zip": pl.String})

    def test_int_and_float_mix_not_treated_as_conflict(self) -> None:
        """int+float in the same field is NOT a conflict — both are numeric and
        Polars resolves them as Float64 without serialisation to JSON string."""
        docs = [
            {"id": 1, "km": 50000, "price": 14999.99},
            {"id": 2, "km": 49500.5, "price": 16000},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df["km"].dtype == pl.Float64
        assert df["price"].dtype == pl.Float64
        assert df["km"].to_list() == [50000.0, 49500.5]

    def test_int_float_with_declared_float64_schema_not_serialised(self) -> None:
        """int+float field declared as Float64 — no conflict serialisation, stays numeric."""
        docs = [
            {"id": 1, "km": 50000},
            {"id": 2, "km": 49500.5},
        ]
        reader, _ = _reader(docs)
        schema = (
            ColumnSchema("id", LoomDtype.INT64),
            ColumnSchema("km", LoomDtype.FLOAT64),
        )
        df = reader.read(_spec(schema=schema), None).collect()
        assert df["km"].dtype == pl.Float64
        assert df["km"][0] == 50000.0

    def test_int_string_mix_still_serialised(self) -> None:
        """int+string IS a real conflict and must still be serialised."""
        import json

        docs = [
            {"id": 1, "status": 0},
            {"id": 2, "status": "active"},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df["status"].dtype == pl.String
        # The int value ends up serialised as a JSON number string
        assert json.loads(df["status"][0]) == 0

    def test_declared_struct_with_nested_scalar_conflict_stays_struct(self) -> None:
        """A schema-declared Struct field is not serialised even when a sub-field
        has mixed types across documents.  The conflicted sub-value is coerced or
        nulled by the canonicalization step instead of flattening the whole field."""
        docs = [
            {"id": 1, "pricing": {"amount": 1500, "currency": "EUR"}},
            {"id": 2, "pricing": {"amount": 1499.99, "currency": "EUR"}},
            {"id": 3, "pricing": {"amount": "consult", "currency": "EUR"}},
        ]
        reader, _ = _reader(docs)
        schema = (
            ColumnSchema("id", LoomDtype.INT64),
            ColumnSchema(
                "pricing",
                StructType(
                    fields=(
                        StructField("amount", LoomDtype.FLOAT64),
                        StructField("currency", LoomDtype.UTF8),
                    )
                ),
            ),
        )
        df = reader.read(_spec(schema=schema), None).collect()

        assert df["pricing"].dtype == pl.Struct({"amount": pl.Float64, "currency": pl.String})
        assert df["pricing"][0]["amount"] == 1500.0
        assert df["pricing"][1]["amount"] == 1499.99
        assert df["pricing"][2]["amount"] is None  # "consult" cannot parse as Float64

    def test_undeclared_struct_with_nested_conflict_still_serialises(self) -> None:
        """Without a declared schema the existing behaviour is preserved:
        a nested type conflict → JSON string."""
        docs = [
            {"id": 1, "pricing": {"amount": 1500, "currency": "EUR"}},
            {"id": 2, "pricing": {"amount": "consult", "currency": "EUR"}},
        ]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df["pricing"].dtype == pl.String

    def test_declared_list_of_struct_with_nested_conflict_stays_list(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Same protection extends to List[Struct] fields."""
        docs = [
            {"id": 1, "tags": [{"name": "sale", "weight": 1}]},
            {"id": 2, "tags": [{"name": "new", "weight": "high"}]},
        ]
        reader, _ = _reader(docs)
        schema = (
            ColumnSchema("id", LoomDtype.INT64),
            ColumnSchema(
                "tags",
                ListType(
                    inner=StructType(
                        fields=(
                            StructField("name", LoomDtype.UTF8),
                            StructField("weight", LoomDtype.INT64),
                        )
                    )
                ),
            ),
        )
        with caplog.at_level(logging.WARNING):
            df = reader.read(_spec(schema=schema), None).collect()

        assert df["tags"].dtype == pl.List(pl.Struct({"name": pl.String, "weight": pl.Int64}))
        assert any("deferring" in msg.lower() for msg in caplog.messages)


# ---------------------------------------------------------------------------
# UTF8 schema fields — pre-serialization to JSON string
# ---------------------------------------------------------------------------


class TestSchemaStrFields:
    """Fields declared LoomDtype.UTF8 are pre-serialized when the value is complex."""

    def test_utf8_field_dict_becomes_json_string(self) -> None:
        import json

        docs = [
            {"id": 1, "meta": {"key": "val", "count": 3}},
            {"id": 2, "meta": {"key": "other", "count": 7}},
        ]
        reader, _ = _reader(docs)
        schema = (ColumnSchema("id", LoomDtype.INT64), ColumnSchema("meta", LoomDtype.UTF8))
        df = reader.read(_spec(schema=schema), None).collect()
        assert df["meta"].dtype == pl.String
        parsed = json.loads(df["meta"][0])
        assert parsed == {"key": "val", "count": 3}

    def test_utf8_field_preserves_plain_string(self) -> None:
        docs = [{"id": 1, "meta": "already a string"}]
        reader, _ = _reader(docs)
        schema = (ColumnSchema("id", LoomDtype.INT64), ColumnSchema("meta", LoomDtype.UTF8))
        df = reader.read(_spec(schema=schema), None).collect()
        assert df["meta"].dtype == pl.String
        assert df["meta"][0] == "already a string"

    def test_utf8_field_null_stays_null(self) -> None:
        docs = [
            {"id": 1, "meta": None},
            {"id": 2, "meta": {"k": "v"}},
        ]
        reader, _ = _reader(docs)
        schema = (ColumnSchema("id", LoomDtype.INT64), ColumnSchema("meta", LoomDtype.UTF8))
        df = reader.read(_spec(schema=schema), None).collect()
        assert df["meta"].dtype == pl.String
        assert df["meta"][0] is None

    def test_non_utf8_field_unaffected(self) -> None:
        docs = [{"id": 1, "price": 42, "meta": {"k": "v"}}]
        reader, _ = _reader(docs)
        schema = (
            ColumnSchema("id", LoomDtype.INT64),
            ColumnSchema("price", LoomDtype.INT64),
            ColumnSchema("meta", LoomDtype.UTF8),
        )
        df = reader.read(_spec(schema=schema), None).collect()
        assert df["price"].dtype == pl.Int64
        assert df["price"][0] == 42

    def test_class_annotation_str_auto_pre_serialized(self) -> None:
        import json

        docs = [{"name": {"first": "Alpha", "last": "One"}, "score": 9.5}]
        reader, _ = _reader(docs)
        df = reader.read(_spec(schema=resolve_schema(MetaDoc)), None).collect()
        assert df["name"].dtype == pl.String
        parsed = json.loads(df["name"][0])
        assert parsed == {"first": "Alpha", "last": "One"}


# ---------------------------------------------------------------------------
# Schemaless null/empty-list safeguard
# ---------------------------------------------------------------------------


class TestNullTypeSafeguard:
    """Without schema, Null and List(Null) dtypes are promoted to String / List(String)."""

    def test_all_null_field_promoted_to_string(self) -> None:
        docs = [{"id": i, "notes": None} for i in range(5)]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df["notes"].dtype == pl.String

    def test_all_empty_list_promoted_to_list_string(self) -> None:
        docs = [{"id": i, "tags": []} for i in range(5)]
        reader, _ = _reader(docs)
        df = reader.read(_spec(), None).collect()
        assert df["tags"].dtype == pl.List(pl.String)

    def test_fallback_preserves_declared_struct_field_types(self) -> None:
        """When fallback fires, schema_overrides are forwarded so nested Null→declared type.

        Reproduces: declared task.user:str|None, all docs have user=None,
        _resolve_conflicts serialises 'ad' to string → pl.from_dicts fails with
        schema mismatch → fallback used → without fix, task.user ends up Null.
        """
        from loom.etl.io.sources._mongo_batch import _build_frame_fallback

        task_dtype = pl.List(
            pl.Struct({"name": pl.String, "status": pl.Boolean, "user": pl.String})
        )
        batch = [
            {"id": 1, "task": [{"name": "A", "status": False, "user": None}]},
            {"id": 2, "task": []},
            {"id": 3, "task": [{"name": "B", "status": True, "user": None}]},
        ]
        schema_overrides = {"id": pl.Int64, "task": task_dtype}

        df = _build_frame_fallback(batch, schema_overrides)

        assert df["task"].dtype == task_dtype
        assert "Null" not in str(df["task"].dtype)
