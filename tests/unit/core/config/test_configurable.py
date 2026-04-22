"""Unit tests for declarative config bindings."""

from __future__ import annotations

import pytest

from loom.core.config import ConfigBinding, Configurable
from loom.core.model import LoomFrozenStruct


class _Worker(Configurable):
    def __init__(self, *, timeout_ms: int = 5000) -> None:
        self.timeout_ms = timeout_ms


def test_from_config_returns_deferred_binding() -> None:
    binding = _Worker.from_config("streaming.tasks.scrape_one", timeout_ms=20_000)

    assert isinstance(binding, LoomFrozenStruct)
    assert binding.target is _Worker
    assert binding.config_path == "streaming.tasks.scrape_one"
    assert binding.overrides == {"timeout_ms": 20_000}


def test_configure_returns_binding_without_config_path() -> None:
    binding = _Worker.configure(timeout_ms=10_000)

    assert binding.target is _Worker
    assert binding.config_path == ""
    assert binding.overrides == {"timeout_ms": 10_000}


def test_from_config_rejects_empty_path() -> None:
    with pytest.raises(ValueError, match="config_path"):
        _Worker.from_config(" ")


def test_config_binding_rejects_blank_non_empty_path() -> None:
    with pytest.raises(ValueError, match="config_path"):
        ConfigBinding(target=_Worker, config_path=" ")
