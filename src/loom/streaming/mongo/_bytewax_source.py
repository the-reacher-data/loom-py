"""Bytewax source runtime for MongoDB CDC."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

from loom.streaming.compiler._plan import CompiledMongoCDCSource
from loom.streaming.core._message import Message
from loom.streaming.mongo._event import MongoCDCEvent
from loom.streaming.mongo._normalize import build_mongo_cdc_message


class MongoCDCPartition(StatefulSourcePartition[Message[MongoCDCEvent], dict[str, object] | None]):
    """Single MongoDB change-stream partition backed by PyMongo."""

    def __init__(
        self,
        source: CompiledMongoCDCSource,
        resume_state: dict[str, object] | None,
    ) -> None:
        self._source = source
        self._resume_token = resume_state
        self._client = _build_mongo_client(source)
        self._stream = _open_change_stream(self._client, source, resume_state)

    def next_batch(self) -> list[Message[MongoCDCEvent]]:
        """Poll one change event when available."""
        try:
            change = self._stream.try_next()
        except Exception as exc:
            if not _should_restart_from_now(self._source, exc):
                raise
            change = self._restart_from_now()
        if change is None:
            return []
        if not isinstance(change, Mapping):
            raise TypeError("Mongo change stream yielded a non-mapping event.")
        message = build_mongo_cdc_message(change)
        self._resume_token = message.payload.resume_token
        return [message]

    def snapshot(self) -> dict[str, object] | None:
        """Return the current Mongo resume token."""
        return self._resume_token

    def close(self) -> None:
        self._stream.close()
        self._client.close()

    def _restart_from_now(self) -> object | None:
        self._stream.close()
        self._resume_token = None
        self._stream = _open_change_stream(self._client, self._source, None)
        return cast(object | None, self._stream.try_next())


class MongoCDCSource(FixedPartitionedSource[Message[MongoCDCEvent], dict[str, object] | None]):
    """Bytewax source wrapping a MongoDB change stream."""

    _PARTITION_KEY = "mongo_cdc"

    def __init__(self, source: CompiledMongoCDCSource) -> None:
        self._source = source

    def list_parts(self) -> list[str]:
        """Return the fixed Mongo CDC partition key."""
        return [self._PARTITION_KEY]

    def build_part(
        self,
        step_id: str,
        for_part: str,
        resume_state: dict[str, object] | None,
    ) -> MongoCDCPartition:
        """Build the single Mongo CDC source partition."""
        del step_id
        if for_part != self._PARTITION_KEY:
            raise KeyError(f"Unknown Mongo CDC partition {for_part!r}.")
        return MongoCDCPartition(self._source, resume_state)


def _build_mongo_client(source: CompiledMongoCDCSource) -> Any:
    """Build a PyMongo client for one compiled Mongo source."""
    try:
        from pymongo import MongoClient
        from pymongo.server_api import ServerApi
    except ImportError as exc:
        raise ImportError(
            "MongoDB CDC support requires the optional 'pymongo' dependency."
        ) from exc

    kwargs: dict[str, Any] = {}
    if source.settings.server_api_version is not None:
        kwargs["server_api"] = ServerApi(source.settings.server_api_version)
    return MongoClient(source.settings.uri, **kwargs)


def _open_change_stream(
    client: Any,
    source: CompiledMongoCDCSource,
    resume_state: dict[str, object] | None,
) -> Any:
    """Open a Mongo change stream for one compiled source."""
    database = client[source.settings.database]
    watch_options = _resolve_watch_options(source.watch_options, resume_state)
    pipeline = _build_pipeline(source.collections, source.watch_options)
    if not source.collections:
        return database.watch(pipeline, **watch_options)
    if len(source.collections) == 1:
        return database[source.collections[0]].watch(pipeline, **watch_options)
    return database.watch(pipeline, **watch_options)


def _build_pipeline(
    collections: tuple[str, ...],
    watch_options: Mapping[str, object],
) -> list[dict[str, object]]:
    """Build the Mongo change-stream pipeline without mutating watch options."""
    stages: list[dict[str, object]] = []
    if len(collections) > 1:
        stages.append({"$match": {"ns.coll": {"$in": list(collections)}}})
    extra_pipeline = watch_options.get("pipeline")
    if isinstance(extra_pipeline, list):
        stages.extend(stage for stage in extra_pipeline if isinstance(stage, dict))
    return stages


def _resolve_watch_options(
    watch_options: Mapping[str, object],
    resume_state: dict[str, object] | None,
) -> dict[str, object]:
    """Resolve watch kwargs for one Mongo change stream open."""
    resolved = {key: value for key, value in watch_options.items() if key != "pipeline"}
    if resume_state is not None:
        resolved["resume_after"] = resume_state
    return resolved


def _should_restart_from_now(source: CompiledMongoCDCSource, exc: Exception) -> bool:
    if source.settings.on_oplog_expired != "restart_from_now":
        return False
    return _is_oplog_expired_error(exc)


def _is_oplog_expired_error(exc: Exception) -> bool:
    if type(exc).__name__ != "OperationFailure":
        return False
    code = getattr(exc, "code", None)
    details = getattr(exc, "details", None)
    code_name = details.get("codeName") if isinstance(details, Mapping) else None
    if code == 286 or code_name == "ChangeStreamHistoryLost":
        return True
    message = str(exc).lower()
    return (
        "changestreamhistorylost" in message
        or "resume point may no longer be in the oplog" in message
        or "resume of change stream was not possible" in message
    )


__all__ = ["MongoCDCPartition", "MongoCDCSource"]
