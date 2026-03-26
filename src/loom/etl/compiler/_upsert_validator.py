"""Compile-time UPSERT constraint validation."""

from __future__ import annotations

from typing import Any

from loom.etl._target import TargetSpec, WriteMode
from loom.etl.compiler._errors import ETLCompilationError


def validate_upsert_spec(step_type: type[Any], spec: TargetSpec) -> None:
    """Validate UPSERT-specific constraints at compile time.

    Args:
        step_type: The step class being validated (for error messages).
        spec:      Compiled target spec.

    Raises:
        ETLCompilationError: When upsert_keys is empty, exclude and include
                             are both set, or exclude overlaps with upsert_keys.
    """
    if spec.mode is not WriteMode.UPSERT:
        return
    _check_upsert_keys_non_empty(step_type, spec)
    _check_upsert_exclude_include_exclusive(step_type, spec)
    _check_upsert_exclude_keys_disjoint(step_type, spec)


def _check_upsert_keys_non_empty(step_type: type[Any], spec: TargetSpec) -> None:
    if not spec.upsert_keys:
        raise ETLCompilationError.upsert_no_keys(step_type)


def _check_upsert_exclude_include_exclusive(step_type: type[Any], spec: TargetSpec) -> None:
    if spec.upsert_exclude and spec.upsert_include:
        raise ETLCompilationError.upsert_exclude_include_conflict(step_type)


def _check_upsert_exclude_keys_disjoint(step_type: type[Any], spec: TargetSpec) -> None:
    overlap = frozenset(spec.upsert_exclude) & frozenset(spec.upsert_keys)
    if overlap:
        raise ETLCompilationError.upsert_key_in_exclude(step_type, overlap)
