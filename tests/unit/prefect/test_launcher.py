"""Tests for loom.prefect._launcher (Protocol + run_etl_in_container).

Verifies the ContainerLauncher Protocol contract and the orchestration of
submit / wait / cancel inside run_etl_in_container.

All modules under test are unimplemented (TDD red phase) — import failure
is expected initially.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import msgspec
import pytest

from loom.prefect._ctx import FlowCtx
from loom.prefect._launcher import (
    ContainerExecution,
    ContainerLauncher,
    ContainerLaunchError,
    ContainerResult,
    ContainerTaskFailedError,
    ContainerTaskTimeoutError,
    run_etl_in_container,
)


class _DummyParams(msgspec.Struct, frozen=True, kw_only=True):
    business_date: str
    countries: tuple[str, ...] = ()


def _make_ctx() -> FlowCtx:
    return FlowCtx(
        correlation_id="corr-1",
        run_id="run-1",
        environment="prod",
    )


def _make_params() -> _DummyParams:
    return _DummyParams(business_date="2026-06-02", countries=("ES", "FR"))


class _StubLauncher:
    """Minimal class that satisfies the ContainerLauncher Protocol."""

    def __init__(self) -> None:
        self.submit_calls: list[dict[str, Any]] = []
        self.cancel_calls: list[ContainerExecution] = []
        self.wait_result: ContainerResult | None = None
        self.wait_exc: BaseException | None = None
        self.cancel_exc: BaseException | None = None
        self.submit_exc: BaseException | None = None
        self.execution = ContainerExecution(id="exec-1", logs_url=None)

    def submit(
        self,
        *,
        env: Mapping[str, str],
        tags: Mapping[str, str] = {},
    ) -> ContainerExecution:
        if self.submit_exc is not None:
            raise self.submit_exc
        self.submit_calls.append({"env": dict(env), "tags": dict(tags)})
        return self.execution

    def wait(
        self,
        execution: ContainerExecution,
        *,
        poll_interval_seconds: float,
        timeout_seconds: int,
    ) -> ContainerResult:
        if self.wait_exc is not None:
            raise self.wait_exc
        assert self.wait_result is not None
        return self.wait_result

    def cancel(self, execution: ContainerExecution) -> None:
        self.cancel_calls.append(execution)
        if self.cancel_exc is not None:
            raise self.cancel_exc


def test_stub_satisfies_container_launcher_protocol() -> None:
    launcher = _StubLauncher()
    assert isinstance(launcher, ContainerLauncher)


def test_run_etl_in_container_returns_zero_on_success() -> None:
    launcher = _StubLauncher()
    launcher.wait_result = ContainerResult(exit_code=0, reason=None)

    rc = run_etl_in_container(
        ctx=_make_ctx(),
        params=_make_params(),
        launcher=launcher,
        poll_interval_seconds=0.01,
        timeout_seconds=60,
    )
    assert rc == 0


def test_run_etl_in_container_submits_with_serialized_env_and_tags() -> None:
    launcher = _StubLauncher()
    launcher.wait_result = ContainerResult(exit_code=0, reason=None)
    ctx = _make_ctx()
    params = _make_params()

    run_etl_in_container(
        ctx=ctx,
        params=params,
        launcher=launcher,
        poll_interval_seconds=0.01,
        timeout_seconds=60,
    )
    assert len(launcher.submit_calls) == 1
    call = launcher.submit_calls[0]
    env = call["env"]
    # Without flow_name only the two JSON envs are exported.
    assert set(env.keys()) == {"LOOM_FLOW_CTX_JSON", "LOOM_FLOW_PARAMS_JSON"}

    # Round-trip the JSON env values back to their structs.
    decoded_ctx = msgspec.json.decode(env["LOOM_FLOW_CTX_JSON"], type=FlowCtx)
    decoded_params = msgspec.json.decode(env["LOOM_FLOW_PARAMS_JSON"], type=_DummyParams)
    assert decoded_ctx == ctx
    assert decoded_params == params

    tags = call["tags"]
    assert tags["loom.correlation_id"] == ctx.correlation_id
    assert tags["loom.run_id"] == ctx.run_id
    assert tags["loom.environment"] == ctx.environment


def test_run_etl_in_container_injects_flow_name_when_provided() -> None:
    """flow_name kwarg becomes LOOM_FLOW_NAME env var so the container's
    inner build_etl_flow can share the outer's name (one entry in
    Prefect's Flows catalog per ETL)."""
    launcher = _StubLauncher()
    launcher.wait_result = ContainerResult(exit_code=0, reason=None)

    run_etl_in_container(
        ctx=_make_ctx(),
        params=_make_params(),
        launcher=launcher,
        flow_name="daily-orders",
        poll_interval_seconds=0.01,
        timeout_seconds=60,
    )

    env = launcher.submit_calls[0]["env"]
    assert env["LOOM_FLOW_NAME"] == "daily-orders"
    assert "LOOM_FLOW_CTX_JSON" in env
    assert "LOOM_FLOW_PARAMS_JSON" in env


def test_run_etl_in_container_omits_flow_name_when_not_provided() -> None:
    """flow_name=None must not export LOOM_FLOW_NAME (no implicit default)."""
    launcher = _StubLauncher()
    launcher.wait_result = ContainerResult(exit_code=0, reason=None)

    run_etl_in_container(
        ctx=_make_ctx(),
        params=_make_params(),
        launcher=launcher,
        poll_interval_seconds=0.01,
        timeout_seconds=60,
    )

    env = launcher.submit_calls[0]["env"]
    assert "LOOM_FLOW_NAME" not in env


def test_run_etl_in_container_raises_failed_on_non_zero_exit() -> None:
    launcher = _StubLauncher()
    launcher.wait_result = ContainerResult(exit_code=137, reason="OOMKilled")

    with pytest.raises(ContainerTaskFailedError) as excinfo:
        run_etl_in_container(
            ctx=_make_ctx(),
            params=_make_params(),
            launcher=launcher,
            poll_interval_seconds=0.01,
            timeout_seconds=60,
        )
    err = excinfo.value
    assert err.execution_id == "exec-1"
    assert err.exit_code == 137
    assert err.reason == "OOMKilled"
    # No cancel needed on plain failure.
    assert launcher.cancel_calls == []


def test_run_etl_in_container_cancels_on_timeout() -> None:
    launcher = _StubLauncher()
    launcher.wait_exc = ContainerTaskTimeoutError(execution_id="exec-1")

    with pytest.raises(ContainerTaskTimeoutError):
        run_etl_in_container(
            ctx=_make_ctx(),
            params=_make_params(),
            launcher=launcher,
            poll_interval_seconds=0.01,
            timeout_seconds=1,
        )
    assert len(launcher.cancel_calls) == 1


def test_run_etl_in_container_propagates_timeout_when_cancel_raises() -> None:
    launcher = _StubLauncher()
    launcher.wait_exc = ContainerTaskTimeoutError(execution_id="exec-1")
    launcher.cancel_exc = RuntimeError("cancel exploded")

    with pytest.raises(ContainerTaskTimeoutError):
        run_etl_in_container(
            ctx=_make_ctx(),
            params=_make_params(),
            launcher=launcher,
            poll_interval_seconds=0.01,
            timeout_seconds=1,
        )


def test_run_etl_in_container_propagates_launch_error_without_cancel() -> None:
    launcher = _StubLauncher()
    launcher.submit_exc = ContainerLaunchError("boom")

    with pytest.raises(ContainerLaunchError):
        run_etl_in_container(
            ctx=_make_ctx(),
            params=_make_params(),
            launcher=launcher,
            poll_interval_seconds=0.01,
            timeout_seconds=60,
        )
    assert launcher.cancel_calls == []


def test_container_execution_struct_fields() -> None:
    execution = ContainerExecution(id="task-arn", logs_url="https://logs")
    assert execution.id == "task-arn"
    assert execution.logs_url == "https://logs"


def test_container_result_struct_fields() -> None:
    result = ContainerResult(exit_code=0, reason=None)
    assert result.exit_code == 0
    assert result.reason is None
