"""MongoDB CDC payload contracts."""

from loom.streaming.mongo._config import MongoConfig, MongoSourceConfig
from loom.streaming.mongo._event import (
    MongoBsonTimestamp,
    MongoCDCEvent,
    MongoCDCNamespace,
    MongoDBRef,
    MongoObjectId,
)
from loom.streaming.mongo._normalize import (
    build_mongo_cdc_event,
    build_mongo_cdc_message,
    normalize_bson_value,
)

__all__ = [
    "MongoConfig",
    "MongoBsonTimestamp",
    "MongoCDCEvent",
    "MongoCDCNamespace",
    "MongoDBRef",
    "MongoObjectId",
    "MongoSourceConfig",
    "build_mongo_cdc_event",
    "build_mongo_cdc_message",
    "normalize_bson_value",
]
