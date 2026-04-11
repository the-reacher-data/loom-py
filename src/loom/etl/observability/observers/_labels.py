"""Shared label extraction utilities for observability observers."""

from __future__ import annotations

from typing import Any

from loom.etl.declarative.target._file import FileSpec
from loom.etl.declarative.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpsertSpec,
)
from loom.etl.declarative.target._temp import TempFanInSpec, TempSpec


def source_label(spec: Any) -> str:
    """Return a readable label for a source spec."""
    if getattr(spec, "table_ref", None) is not None:
        return str(spec.table_ref.ref)
    if getattr(spec, "temp_name", None) is not None:
        return f"temp:{spec.temp_name}"
    return str(spec.path) if spec.path else "?"


def target_label(spec: Any) -> str:
    """Return a readable label for a target spec."""
    if getattr(spec, "table_ref", None) is not None:
        return str(spec.table_ref.ref)
    if getattr(spec, "temp_name", None) is not None:
        return f"temp:{spec.temp_name}"
    return str(spec.path) if spec.path else "?"


def write_mode_label(spec: Any) -> str:
    """Return the write mode label for the given target spec."""
    match spec:
        case AppendSpec():
            return "append"
        case ReplaceSpec():
            return "replace"
        case ReplacePartitionsSpec():
            return "replace_partitions"
        case ReplaceWhereSpec():
            return "replace_where"
        case UpsertSpec():
            return "upsert"
        case TempSpec() | TempFanInSpec():
            return "temp"
        case FileSpec():
            return "file"
        case _:
            return "unknown"


__all__ = ["source_label", "target_label", "write_mode_label"]
