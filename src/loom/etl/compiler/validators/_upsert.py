"""Compile-time UPSERT constraint validation."""

from __future__ import annotations

from typing import Any, Protocol, cast

from loom.etl.compiler._errors import ETLCompilationError
from loom.etl.io.target import TargetSpec


class _UpsertSpecLike(Protocol):
    upsert_keys: tuple[str, ...]
    upsert_exclude: tuple[str, ...]
    upsert_include: tuple[str, ...]


def validate_upsert_spec(step_type: type[Any], spec: TargetSpec) -> None:
    """Validate UPSERT-specific constraints at compile time.

    Args:
        step_type: The step class being validated (for error messages).
        spec:      Compiled target spec.

    Raises:
        ETLCompilationError: When upsert_keys is empty, exclude and include
                             are both set, or exclude overlaps with upsert_keys.
    """
    if not _is_upsert_like(spec):
        return
    upsert_spec = cast(_UpsertSpecLike, spec)
    _check_upsert_keys_non_empty(step_type, upsert_spec)
    _check_upsert_exclude_include_exclusive(step_type, upsert_spec)
    _check_upsert_exclude_keys_disjoint(step_type, upsert_spec)


def _check_upsert_keys_non_empty(step_type: type[Any], spec: _UpsertSpecLike) -> None:
    if not spec.upsert_keys:
        raise ETLCompilationError.upsert_no_keys(step_type)


def _check_upsert_exclude_include_exclusive(step_type: type[Any], spec: _UpsertSpecLike) -> None:
    if spec.upsert_exclude and spec.upsert_include:
        raise ETLCompilationError.upsert_exclude_include_conflict(step_type)


def _check_upsert_exclude_keys_disjoint(step_type: type[Any], spec: _UpsertSpecLike) -> None:
    overlap = frozenset(spec.upsert_exclude) & frozenset(spec.upsert_keys)
    if overlap:
        raise ETLCompilationError.upsert_key_in_exclude(step_type, overlap)


def _is_upsert_like(spec: object) -> bool:
    keys = getattr(spec, "upsert_keys", None)
    exclude = getattr(spec, "upsert_exclude", None)
    include = getattr(spec, "upsert_include", None)
    return isinstance(keys, tuple) and isinstance(exclude, tuple) and isinstance(include, tuple)
