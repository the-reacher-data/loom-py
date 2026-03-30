"""Tests for runner-specific errors."""

from __future__ import annotations

import importlib

import loom.etl.runner.errors as errors_mod

errors_mod = importlib.reload(errors_mod)
InvalidStageError = errors_mod.InvalidStageError


def test_invalid_stage_error_message_lists_requested_names() -> None:
    error = InvalidStageError(frozenset({"MissingStep", "MissingProcess"}))
    text = str(error)

    assert "No steps or processes match include=" in text
    assert "MissingStep" in text
    assert "MissingProcess" in text
