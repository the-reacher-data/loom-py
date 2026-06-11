"""Shared helpers for Prefect flow factories (etl_flow, maintenance_flow, …)."""

from __future__ import annotations

from typing import Any


def coerce_tags(raw: Any) -> tuple[str, ...]:
    """Parse the ``tags`` list from a raw YAML value into a tuple of strings."""
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise TypeError(f"tags: expected a list of strings, got {type(raw).__name__}")
    for value in raw:
        if not isinstance(value, str):
            raise TypeError(f"tags: every entry must be str, got {type(value).__name__}")
    return tuple(raw)


__all__ = ["coerce_tags"]
