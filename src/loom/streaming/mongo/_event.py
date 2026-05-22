"""Loom-safe MongoDB CDC payload contracts."""

from __future__ import annotations

from loom.core.model import LoomFrozenStruct


class MongoBsonTimestamp(LoomFrozenStruct, frozen=True):
    """Canonical Loom-safe representation of a BSON timestamp."""

    seconds: int
    increment: int


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
    document_id: str | None = None
    cluster_time: MongoBsonTimestamp | None = None
    wall_time_ms: int | None = None
    full_document: dict[str, object] | None = None
    update_description: dict[str, object] | None = None
    raw_json: str


__all__ = ["MongoBsonTimestamp", "MongoCDCEvent", "MongoCDCNamespace"]
