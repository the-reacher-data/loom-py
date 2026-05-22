"""Loom-safe MongoDB CDC payload contracts."""

from __future__ import annotations

from loom.core.model import LoomFrozenStruct


class MongoBsonTimestamp(LoomFrozenStruct, frozen=True):
    """Canonical Loom-safe representation of a BSON timestamp."""

    seconds: int
    increment: int


class MongoObjectId(LoomFrozenStruct, frozen=True, kw_only=True):
    """Structured representation of a MongoDB ObjectId.

    Decouples Loom domain code from pymongo by carrying only the
    information derivable from the 12-byte ObjectId value.

    Attributes:
        id: Lowercase hex string of the 24-character ObjectId.
        created_at_ms: Creation epoch in milliseconds, extracted from
            the 4-byte Unix timestamp embedded in the ObjectId, or
            ``None`` when the timestamp is unavailable.
    """

    id: str
    created_at_ms: int | None = None


class MongoDBRef(LoomFrozenStruct, frozen=True, kw_only=True):
    """Structured representation of a MongoDB DBRef (cross-collection reference).

    Carries the minimal information needed to resolve a reference without
    coupling domain code to the BSON DBRef wire format or pymongo types.

    Attributes:
        id: Lowercase hex string of the referenced document's ObjectId.
        collection: Name of the collection that holds the referenced document.
        database: Name of the database, or ``None`` when the reference is
            local to the current database.
    """

    id: str
    collection: str
    database: str | None = None


class MongoCDCNamespace(LoomFrozenStruct, frozen=True):
    """MongoDB namespace carried by a change event."""

    db: str
    coll: str | None = None


class MongoCDCEvent(LoomFrozenStruct, frozen=True, kw_only=True):
    """Normalized MongoDB CDC payload safe for the Loom pipeline."""

    event_id: str
    operation_type: str
    namespace: MongoCDCNamespace
    resume_token: dict[str, object]
    document_id: MongoObjectId | str | None = None
    cluster_time: MongoBsonTimestamp | None = None
    wall_time_ms: int | None = None
    full_document: dict[str, object] | None = None
    update_description: dict[str, object] | None = None
    raw_json: str


__all__ = [
    "MongoBsonTimestamp",
    "MongoObjectId",
    "MongoDBRef",
    "MongoCDCNamespace",
    "MongoCDCEvent",
]
