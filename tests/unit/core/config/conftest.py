"""Shared fixtures for config tests."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _isolate_builtin_resolvers(clear_builtin_resolvers: None) -> None:
    """Unregister ``secrets`` and ``ssm`` around every config test."""
