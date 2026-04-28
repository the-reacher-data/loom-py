"""Fixtures for streaming observability tests."""

from __future__ import annotations

import pytest

from loom.streaming import StreamFlow
from tests.unit.streaming.observability.cases import (
    DropItem,
    RecordingFlowObserver,
    build_drop_flow,
)


@pytest.fixture
def recording_flow_observer() -> RecordingFlowObserver:
    """Return a fresh recording observer for assertions."""
    return RecordingFlowObserver()


@pytest.fixture
def drop_flow() -> StreamFlow[DropItem, DropItem]:
    """Return a flow that emits unrouted task errors."""
    return build_drop_flow()


@pytest.fixture
def drop_item() -> DropItem:
    """Return a payload used by unrouted error tests."""
    return DropItem(value="x")
