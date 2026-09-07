"""Geospatial toolset factory referenced by ``capability-python-agent/agent.yaml``."""

from __future__ import annotations


def build_geo_toolset(container: object, *, max_results: int = 3, radius_km: int = 25) -> object:
    """Build a fake geospatial toolset (``ToolsetFactory``-shaped).

    Args:
        container: Application container the factory may resolve services from.
        max_results: Service points the toolset reports per lookup.
        radius_km: Search radius around the resolved coordinates.
    """
    del container, max_results, radius_km
    return object()
