"""Shared fixtures for streaming unit tests."""

from __future__ import annotations

import pytest

from tests.unit.streaming.support.flow_cases import (
    StreamFlowCase,
    build_async_flow_case,
    build_router_flow_case,
    build_simple_validation_flow_case,
    build_with_batch_flow_case,
    build_with_batch_scope_flow_case,
)


@pytest.fixture
def simple_validation_flow_case() -> StreamFlowCase:
    """Return the simplest public DSL flow case."""
    return build_simple_validation_flow_case()


@pytest.fixture
def router_flow_case() -> StreamFlowCase:
    """Return a public DSL flow case with multiple router branches."""
    return build_router_flow_case()


@pytest.fixture
def with_batch_flow_case() -> StreamFlowCase:
    """Return a public DSL flow case with With and ForEach."""
    return build_with_batch_flow_case()


@pytest.fixture
def with_batch_scope_flow_case() -> StreamFlowCase:
    """Return a public DSL flow case with BATCH-scoped With resources."""
    return build_with_batch_scope_flow_case()


@pytest.fixture
def async_flow_case() -> StreamFlowCase:
    """Return a public DSL flow case with WithAsync and ForEach."""
    return build_async_flow_case()
