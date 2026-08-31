"""Limits phase: every policy value must sit inside its published range."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Final

from loom.ai.declarative import PolicySpec
from loom.ai.declarative._v1 import (
    MAX_ITERATIONS_MAX,
    MAX_ITERATIONS_MIN,
    RETRIES_MAX,
    RETRIES_MIN,
    RUN_TIMEOUT_MS_MAX,
    RUN_TIMEOUT_MS_MIN,
    TOOL_TIMEOUT_MS_MAX,
    TOOL_TIMEOUT_MS_MIN,
)
from loom.ai.errors import AgentCompilationIssue, policy_out_of_range

_POLICY_RANGES: Final[Mapping[str, tuple[int, int]]] = MappingProxyType(
    {
        "retries": (RETRIES_MIN, RETRIES_MAX),
        "tool_timeout_ms": (TOOL_TIMEOUT_MS_MIN, TOOL_TIMEOUT_MS_MAX),
        "max_iterations": (MAX_ITERATIONS_MIN, MAX_ITERATIONS_MAX),
        "run_timeout_ms": (RUN_TIMEOUT_MS_MIN, RUN_TIMEOUT_MS_MAX),
    }
)


def validate_policies(policies: PolicySpec, component: str) -> list[AgentCompilationIssue]:
    """Check every policy value against its published inclusive range.

    Args:
        policies: Declared execution limits.
        component: Artifact path or agent name the issues point at.

    Returns:
        One ``POLICY_OUT_OF_RANGE`` issue per value outside its range.
    """
    issues: list[AgentCompilationIssue] = []
    for name, (minimum, maximum) in _POLICY_RANGES.items():
        value: int = getattr(policies, name)
        if value < minimum or value > maximum:
            issues.append(policy_out_of_range(component, name, value, minimum, maximum))
    return issues
