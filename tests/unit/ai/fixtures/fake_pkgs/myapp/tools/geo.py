"""Geospatial toolset factory referenced by ``capability-python-agent/agent.yaml``."""

from __future__ import annotations

from dataclasses import dataclass

from loom.ai.abc import McpSession, ToolsetContext


@dataclass(frozen=True)
class GeoToolset:
    """Fake geospatial toolset carrying what the factory resolved at build.

    Attributes:
        max_results: Service points reported per lookup.
        radius_km: Search radius around the resolved coordinates.
        remote: The agent's shared MCP session the tools query, or ``None``.
    """

    max_results: int
    radius_km: int
    remote: McpSession | None


def build_geo_toolset(
    context: ToolsetContext,
    *,
    max_results: int = 3,
    radius_km: int = 25,
    server: str | None = None,
) -> object:
    """Build a fake geospatial toolset (``ToolsetFactory``-shaped).

    Args:
        context: Build-time context of the agent granting this toolset.
        max_results: Service points the toolset reports per lookup.
        radius_km: Search radius around the resolved coordinates.
        server: ``mcp`` server of the same agent whose session the tools
            reuse; ``None`` builds a toolset without a remote.
    """
    remote = context.remote(server) if server is not None else None
    return GeoToolset(max_results=max_results, radius_km=radius_km, remote=remote)
