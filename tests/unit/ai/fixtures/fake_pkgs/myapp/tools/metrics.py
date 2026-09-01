"""Metrics toolset factory referenced by ``full.agent.yaml``."""

from __future__ import annotations


def build_metrics_toolset(container: object) -> object:
    """Build a fake metrics toolset (``ToolsetFactory``-shaped)."""
    return object()
