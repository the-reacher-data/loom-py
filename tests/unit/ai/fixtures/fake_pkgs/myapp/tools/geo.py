"""Geospatial toolset factory referenced by ``capability_python.agent.yaml``."""

from __future__ import annotations


def build_geo_toolset(container: object) -> object:
    """Build a fake geospatial toolset (``ToolsetFactory``-shaped)."""
    return object()
