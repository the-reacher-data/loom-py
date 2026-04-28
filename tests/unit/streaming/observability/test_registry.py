"""Adapter node registry tests for streaming observability."""

from __future__ import annotations

from types import MappingProxyType

import pytest

from loom.streaming.bytewax._adapter import _NODE_HANDLERS
from loom.streaming.nodes._step import RecordStep


def test_node_handlers_is_immutable() -> None:
    assert isinstance(_NODE_HANDLERS, MappingProxyType)

    with pytest.raises(TypeError):
        _NODE_HANDLERS[object] = lambda *a: None  # type: ignore[index]


def test_node_handlers_cover_step_handlers() -> None:
    assert RecordStep in _NODE_HANDLERS
