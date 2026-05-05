"""Shared fixtures for reusable streaming flow cases."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import cast

import pytest
from omegaconf import DictConfig

from tests.unit.streaming.flows.cases import (
    StreamFlowCase,
    build_async_flow_case,
    build_fork_flow_case,
    build_fork_when_flow_case,
    build_fork_with_flow_case,
    build_router_flow_case,
    build_simple_validation_flow_case,
    build_with_batch_flow_case,
    build_with_batch_scope_flow_case,
)

FlowCaseBuilder = Callable[[DictConfig], StreamFlowCase]

_FLOW_CASE_BUILDERS: Sequence[tuple[str, FlowCaseBuilder]] = (
    ("simple", build_simple_validation_flow_case),
    ("router", build_router_flow_case),
    ("with_batch", build_with_batch_flow_case),
    ("with_batch_scope", build_with_batch_scope_flow_case),
    ("async", build_async_flow_case),
    ("fork", build_fork_flow_case),
    ("fork_with", build_fork_with_flow_case),
    ("fork_when", build_fork_when_flow_case),
)


@pytest.fixture(
    params=[builder for _, builder in _FLOW_CASE_BUILDERS],
    ids=[name for name, _ in _FLOW_CASE_BUILDERS],
)
def flow_case(
    request: pytest.FixtureRequest,
    streaming_kafka_config: DictConfig,
) -> StreamFlowCase:
    """Return one reusable streaming flow case."""
    builder = cast(FlowCaseBuilder, request.param)
    return builder(streaming_kafka_config)
