"""Unit tests for compiler._upsert_validator — direct function coverage."""

from __future__ import annotations

from typing import Any

import pytest

from loom.etl import ETLParams, ETLStep, IntoTable
from loom.etl.compiler import ETLCompilationError
from loom.etl.compiler._upsert_validator import validate_upsert_spec


class _P(ETLParams):
    pass


def _spec(step_type: type[ETLStep[Any]]):  # type: ignore[return]
    """Extract TargetSpec from a step's IntoTable declaration."""
    return step_type.target._to_spec()  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Non-UPSERT specs are always valid
# ---------------------------------------------------------------------------


class _AppendStep(ETLStep[_P]):
    target = IntoTable("t.out").append()

    def execute(self, params: _P) -> Any:
        return None


def test_non_upsert_spec_skipped() -> None:
    validate_upsert_spec(_AppendStep, _spec(_AppendStep))


# ---------------------------------------------------------------------------
# Valid UPSERT
# ---------------------------------------------------------------------------


class _ValidUpsert(ETLStep[_P]):
    target = IntoTable("t.out").upsert(keys=("id",))

    def execute(self, params: _P) -> Any:
        return None


def test_valid_upsert_passes() -> None:
    validate_upsert_spec(_ValidUpsert, _spec(_ValidUpsert))


class _UpsertWithExclude(ETLStep[_P]):
    target = IntoTable("t.out").upsert(keys=("id",), exclude=("created_at",))

    def execute(self, params: _P) -> Any:
        return None


def test_valid_upsert_with_exclude_passes() -> None:
    validate_upsert_spec(_UpsertWithExclude, _spec(_UpsertWithExclude))


class _UpsertWithInclude(ETLStep[_P]):
    target = IntoTable("t.out").upsert(keys=("id",), include=("amount",))

    def execute(self, params: _P) -> Any:
        return None


def test_valid_upsert_with_include_passes() -> None:
    validate_upsert_spec(_UpsertWithInclude, _spec(_UpsertWithInclude))


# ---------------------------------------------------------------------------
# Error: empty keys
# ---------------------------------------------------------------------------


class _NoKeys(ETLStep[_P]):
    target = IntoTable("t.out").upsert(keys=())

    def execute(self, params: _P) -> Any:
        return None


def test_empty_keys_raises() -> None:
    with pytest.raises(ETLCompilationError, match="at least one key"):
        validate_upsert_spec(_NoKeys, _spec(_NoKeys))


# ---------------------------------------------------------------------------
# Error: both exclude and include
# ---------------------------------------------------------------------------


class _BothExcludeInclude(ETLStep[_P]):
    target = IntoTable("t.out").upsert(keys=("id",), exclude=("x",), include=("y",))

    def execute(self, params: _P) -> Any:
        return None


def test_exclude_and_include_raises() -> None:
    with pytest.raises(ETLCompilationError, match="mutually exclusive"):
        validate_upsert_spec(_BothExcludeInclude, _spec(_BothExcludeInclude))


# ---------------------------------------------------------------------------
# Error: exclude overlaps keys
# ---------------------------------------------------------------------------


class _ExcludeOverlapsKey(ETLStep[_P]):
    target = IntoTable("t.out").upsert(keys=("id",), exclude=("id", "amount"))

    def execute(self, params: _P) -> Any:
        return None


def test_exclude_overlaps_key_raises() -> None:
    with pytest.raises(ETLCompilationError, match="id"):
        validate_upsert_spec(_ExcludeOverlapsKey, _spec(_ExcludeOverlapsKey))
