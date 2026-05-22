"""Typed MongoDB CDC configuration contracts."""

from __future__ import annotations

from typing import Any, Literal

import msgspec

from loom.core.model import LoomFrozenStruct
from loom.core.routing import DefaultingRouteResolver, LogicalRef


class MongoSourceConfig(LoomFrozenStruct, frozen=True, kw_only=True):
    """Connection settings for one MongoDB CDC source."""

    uri: str
    database: str
    watch_options: dict[str, Any] = msgspec.field(default_factory=dict)
    server_api_version: str | None = None
    on_oplog_expired: Literal["fail", "restart_from_now"] = "fail"


class MongoConfig(LoomFrozenStruct, frozen=True, kw_only=True):
    """Top-level MongoDB settings loaded from the ``mongo`` config section."""

    source: MongoSourceConfig | None = None
    sources: dict[str, MongoSourceConfig] = msgspec.field(default_factory=dict)

    def source_for(self, ref: str | LogicalRef) -> MongoSourceConfig:
        """Resolve source settings by logical reference with default fallback."""
        resolver = DefaultingRouteResolver(
            default=self.source,
            overrides=self.sources,
            kind="Mongo source config",
        )
        return resolver.resolve(ref)


__all__ = ["MongoConfig", "MongoSourceConfig"]
