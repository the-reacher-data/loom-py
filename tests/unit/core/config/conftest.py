"""Shared fixtures for config tests."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from omegaconf import OmegaConf

_BUILTIN_RESOLVER_NAMES = ("secrets", "ssm")


def _clear_builtin_resolvers() -> None:
    for name in _BUILTIN_RESOLVER_NAMES:
        if OmegaConf.has_resolver(name):
            OmegaConf.clear_resolver(name)


@pytest.fixture(autouse=True)
def clear_builtin_resolvers() -> Iterator[None]:
    """Unregister ``secrets`` and ``ssm`` before and after each test."""
    _clear_builtin_resolvers()
    yield
    _clear_builtin_resolvers()
