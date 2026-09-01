"""Limits phase failures (T050): every ``PolicySpec`` range, below and above."""

from __future__ import annotations

from collections.abc import Callable

import pytest

import loom.ai.compiler  # noqa: F401  — red until the compiler exists
from loom.ai.declarative import AgentSpecV1, PolicySpec
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
from loom.ai.errors import AgentCompilationIssue, AgentErrorCode

_RANGES: dict[str, tuple[int, int]] = {
    "retries": (RETRIES_MIN, RETRIES_MAX),
    "tool_timeout_ms": (TOOL_TIMEOUT_MS_MIN, TOOL_TIMEOUT_MS_MAX),
    "max_iterations": (MAX_ITERATIONS_MIN, MAX_ITERATIONS_MAX),
    "run_timeout_ms": (RUN_TIMEOUT_MS_MIN, RUN_TIMEOUT_MS_MAX),
}


def _out_of_range_cases() -> list[tuple[str, int]]:
    cases: list[tuple[str, int]] = []
    for name, (minimum, maximum) in _RANGES.items():
        cases.append((name, minimum - 1))
        cases.append((name, maximum + 1))
    return cases


@pytest.mark.parametrize(
    ("policy", "value"),
    _out_of_range_cases(),
    ids=[f"{name}_{value}" for name, value in _out_of_range_cases()],
)
def test_reports_policy_out_of_range_when_value_leaves_its_range(
    spec_factory: Callable[..., AgentSpecV1],
    single_issue_for: Callable[..., AgentCompilationIssue],
    policy: str,
    value: int,
) -> None:
    spec = spec_factory(policies=PolicySpec(**{policy: value}))
    issue = single_issue_for(spec)
    assert (issue.code, issue.field) == (
        AgentErrorCode.POLICY_OUT_OF_RANGE,
        f"policies.{policy}",
    )


@pytest.mark.parametrize("bound", [0, 1], ids=["minima", "maxima"])
def test_compiles_clean_when_every_policy_sits_on_its_inclusive_bound(
    spec_factory: Callable[..., AgentSpecV1],
    compiler_factory: Callable[..., object],
    bound: int,
) -> None:
    values = {name: limits[bound] for name, limits in _RANGES.items()}
    spec = spec_factory(policies=PolicySpec(**values))
    plan = compiler_factory().compile(spec)  # type: ignore[attr-defined]
    assert plan.policies == PolicySpec(**values)
