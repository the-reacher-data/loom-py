"""MongoDB CDC contract tests for streaming."""

from __future__ import annotations

import importlib
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from bson import ObjectId
from bson.binary import Binary
from bson.dbref import DBRef
from bson.decimal128 import Decimal128
from bson.timestamp import Timestamp

from loom.core.model import LoomFrozenStruct
from loom.core.routing import LogicalRef
from loom.streaming import (
    FromMongoCDC,
    MongoBsonTimestamp,
    MongoCDCEvent,
    MongoCDCNamespace,
    build_mongo_cdc_event,
    build_mongo_cdc_message,
    normalize_bson_value,
)
from loom.streaming.nodes._shape import StreamShape


class TestMongoBoundaryContracts:
    def test_public_streaming_imports_do_not_require_mongo_runtime_dependency(self) -> None:
        streaming = importlib.import_module("loom.streaming")
        nodes = importlib.import_module("loom.streaming.nodes")

        assert streaming.FromMongoCDC.__name__ == "FromMongoCDC"
        assert streaming.MongoCDCEvent.__name__ == "MongoCDCEvent"
        assert nodes.FromMongoCDC.__name__ == "FromMongoCDC"

    def test_from_mongo_cdc_holds_declared_values(self) -> None:
        source = FromMongoCDC(
            "domain_events",
            collections=("orders", "payments"),
            watch_options={"full_document": "updateLookup"},
            shape=StreamShape.BATCH,
        )

        assert source.name == "domain_events"
        assert source.collections == ("orders", "payments")
        assert source.watch_options == {"full_document": "updateLookup"}
        assert source.shape is StreamShape.BATCH

    def test_from_mongo_cdc_uses_logical_ref(self) -> None:
        source = FromMongoCDC("domain_events")

        assert source.logical_ref == LogicalRef("domain_events")

    def test_from_mongo_cdc_defaults_to_database_scope(self) -> None:
        source = FromMongoCDC("domain_events")

        assert source.collections == ()
        assert source.watch_options == {}
        assert source.shape is StreamShape.RECORD


class TestMongoPayloadContracts:
    def test_mongo_cdc_payload_contracts_are_loom_structs(self) -> None:
        event = MongoCDCEvent(
            event_id="token-1",
            operation_type="insert",
            namespace=MongoCDCNamespace(db="app", coll="orders"),
            resume_token={"_data": "825E"},
            document_id="507f1f77bcf86cd799439011",
            cluster_time=MongoBsonTimestamp(seconds=1716400000, increment=7),
            wall_time_ms=1716400000123,
            full_document={"_id": "507f1f77bcf86cd799439011", "status": "open"},
            update_description=None,
            raw_json='{"operationType":"insert"}',
        )

        assert isinstance(event, LoomFrozenStruct)
        assert isinstance(event.namespace, LoomFrozenStruct)
        assert isinstance(event.cluster_time, LoomFrozenStruct)
        assert event.namespace.db == "app"
        assert event.cluster_time is not None
        assert event.cluster_time.increment == 7
        assert event.resume_token == {"_data": "825E"}

    def test_mongo_cdc_value_contracts_are_immutable(self) -> None:
        event = MongoCDCEvent(
            event_id="token-1",
            operation_type="insert",
            namespace=MongoCDCNamespace(db="app"),
            resume_token={"_data": "825E"},
            raw_json="{}",
        )

        with pytest.raises(AttributeError):
            cast(Any, event).event_id = "token-2"


class TestMongoNormalizationContracts:
    def test_normalize_bson_value_converts_core_scalar_types(self) -> None:
        normalized = {
            "object_id": normalize_bson_value(ObjectId("507f1f77bcf86cd799439011")),
            "timestamp": normalize_bson_value(Timestamp(1716400000, 7)),
            "decimal": normalize_bson_value(Decimal128("428.79")),
            "when": normalize_bson_value(datetime(2024, 5, 22, 12, 0, tzinfo=UTC)),
        }

        assert normalized == {
            "object_id": "507f1f77bcf86cd799439011",
            "timestamp": {"seconds": 1716400000, "increment": 7},
            "decimal": "428.79",
            "when": 1716379200000,
        }

    def test_normalize_bson_value_converts_binary_and_dbref(self) -> None:
        normalized = {
            "binary": normalize_bson_value(Binary(b"mongo")),
            "dbref": normalize_bson_value(
                DBRef("orders", ObjectId("507f1f77bcf86cd799439011"), database="app")
            ),
        }

        assert normalized == {
            "binary": "bW9uZ28=",
            "dbref": {
                "collection": "orders",
                "database": "app",
                "id": "507f1f77bcf86cd799439011",
            },
        }

    def test_build_mongo_cdc_event_preserves_core_metadata(self) -> None:
        change = {
            "_id": {"_data": "825E"},
            "operationType": "update",
            "ns": {"db": "app", "coll": "orders"},
            "documentKey": {"_id": ObjectId("507f1f77bcf86cd799439011")},
            "clusterTime": Timestamp(1716400000, 7),
            "wallTime": datetime(2024, 5, 22, 12, 0, tzinfo=UTC),
            "fullDocument": {
                "_id": ObjectId("507f1f77bcf86cd799439011"),
                "amount": Decimal128("428.79"),
            },
            "updateDescription": {"updatedFields": {"status": "paid"}},
        }

        event = build_mongo_cdc_event(change)

        assert event.event_id == "825E"
        assert event.operation_type == "update"
        assert event.namespace == MongoCDCNamespace(db="app", coll="orders")
        assert event.document_id == "507f1f77bcf86cd799439011"
        assert event.cluster_time == MongoBsonTimestamp(seconds=1716400000, increment=7)
        assert event.wall_time_ms == 1716379200000
        assert event.resume_token == {"_data": "825E"}
        assert event.full_document == {"_id": "507f1f77bcf86cd799439011", "amount": "428.79"}
        assert event.raw_json

    def test_build_mongo_cdc_message_maps_neutral_message_meta_fields(self) -> None:
        message = build_mongo_cdc_message(
            {
                "_id": {"_data": "825E"},
                "operationType": "insert",
                "ns": {"db": "app", "coll": "orders"},
                "documentKey": {"_id": ObjectId("507f1f77bcf86cd799439011")},
                "clusterTime": Timestamp(1716400000, 7),
            }
        )

        assert message.meta.message_id == "825E"
        assert message.meta.message_type == "loom.mongo.cdc"
        assert message.meta.key == "507f1f77bcf86cd799439011"
        assert message.meta.topic is None
        assert message.meta.partition is None
        assert message.meta.offset is None
