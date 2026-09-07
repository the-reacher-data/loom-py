"""Geospatial toolset factory referenced by ``capability-python-agent/agent.yaml``."""

from __future__ import annotations

from dataclasses import dataclass

from loom.ai.abc import ToolsetContext


@dataclass(frozen=True)
class GeoToolset:
    """Fake geospatial toolset carrying what the factory resolved at build.

    Attributes:
        max_results: Service points reported per lookup.
        radius_km: Search radius around the resolved coordinates.
    """

    max_results: int
    radius_km: int


def build_geo_toolset(
    context: ToolsetContext,
    *,
    max_results: int = 3,
    radius_km: int = 25,
) -> object:
    """Build a fake geospatial toolset (``ToolsetFactory``-shaped).

    Args:
        context: Build-time context of the agent granting this toolset.
        max_results: Service points the toolset reports per lookup.
        radius_km: Search radius around the resolved coordinates.
    """
    del context
    return GeoToolset(max_results=max_results, radius_km=radius_km)
