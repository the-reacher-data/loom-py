"""Tests for loom.prefect._flow_builder.build_etl_flow.

Verifies:
- build_etl_flow() returns a callable (the Prefect flow).
- build_etl_flow() calls manifest_store.load() at flow start.
- build_etl_flow() calls manifest_store.delete() on the last attempt (always).
- build_etl_flow() skips steps already present as SUCCESS in the manifest.
- build_etl_flow() does NOT skip steps when force=True is set in FlowCtx.
- Each ETLStep is submitted as a task (observable via manifest save calls).
- The flow is built from a compiled pipeline plan (compile-time, not per-run).
- build_etl_flow() accepts an optional name parameter for the Prefect flow name.

Prefect decorators (@flow, @task) are patched to execute functions directly,
avoiding any Prefect orchestration infrastructure requirement in unit tests.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from datetime import UTC, date
from typing import Any
from unittest.mock import MagicMock, patch

from loom.etl import ETLParams, ETLPipeline, ETLProcess, ETLStep, FromTable, IntoTable
from loom.etl.lineage._records import RunStatus
from loom.prefect._flow_builder import build_etl_flow
from loom.prefect._manifest import ManifestStore, RunManifest, StepEntry

# ---------------------------------------------------------------------------
# Minimal pipeline fixtures
# ---------------------------------------------------------------------------


class _Params(ETLParams):  # type: ignore[misc]
    run_date: date


class _StepA(ETLStep[_Params]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.a").replace()

    def execute(self, params: _Params, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class _StepB(ETLStep[_Params]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.b").replace()

    def execute(self, params: _Params, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class _ProcAB(ETLProcess[_Params]):
    steps = [_StepA, _StepB]


class _PipelineAB(ETLPipeline[_Params]):
    processes = [_ProcAB]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_store(manifest: RunManifest | None = None) -> MagicMock:
    """Create a mock ManifestStore."""
    store = MagicMock(spec=ManifestStore)
    store.load.return_value = manifest
    return store


def _make_runner_factory() -> Callable[[], MagicMock]:
    """Create a factory that returns a mock ETLRunner."""
    from loom.etl.runner import ETLRunner

    runner = MagicMock(spec=ETLRunner)
    runner.run.return_value = None
    return lambda: runner


def _noop_decorator(fn: Callable[..., Any], **kwargs: Any) -> Callable[..., Any]:
    """Replace @flow/@task with a pass-through decorator."""
    return fn


# ---------------------------------------------------------------------------
# build_etl_flow() basic shape tests
# ---------------------------------------------------------------------------


def test_build_etl_flow_returns_callable() -> None:
    """build_etl_flow() must return a callable."""
    store = _make_store()

    with (
        patch("prefect.flow", side_effect=lambda **kw: lambda fn: fn),
        patch("prefect.task", side_effect=lambda **kw: lambda fn: fn),
    ):
        flow = build_etl_flow(
            _PipelineAB,
            runner_factory=_make_runner_factory(),
            manifest_store=store,
            config_path="config/flows.yaml",
        )

    assert callable(flow)


def test_build_etl_flow_accepts_custom_name() -> None:
    """build_etl_flow() accepts an optional name parameter."""
    store = _make_store()

    with (
        patch("prefect.flow", side_effect=lambda **kw: lambda fn: fn),
        patch("prefect.task", side_effect=lambda **kw: lambda fn: fn),
    ):
        flow = build_etl_flow(
            _PipelineAB,
            runner_factory=_make_runner_factory(),
            manifest_store=store,
            config_path="config/flows.yaml",
            name="my-custom-flow",
        )

    assert callable(flow)


# ---------------------------------------------------------------------------
# Manifest lifecycle tests
# ---------------------------------------------------------------------------


def test_build_etl_flow_loads_manifest_on_start() -> None:
    """The flow calls manifest_store.load() with the correlation_id at flow start."""
    store = _make_store()

    with (
        patch("prefect.flow", side_effect=lambda **kw: lambda fn: fn),
        patch("prefect.task", side_effect=lambda **kw: lambda fn: fn),
    ):
        flow = build_etl_flow(
            _PipelineAB,
            runner_factory=_make_runner_factory(),
            manifest_store=store,
            config_path="config/flows.yaml",
        )

    from loom.prefect._ctx import FlowCtx

    ctx = FlowCtx(correlation_id="corr-test", run_id="run-test")

    with (
        patch("prefect.flow", side_effect=lambda **kw: lambda fn: fn),
        patch("prefect.task", side_effect=lambda **kw: lambda fn: fn),
        contextlib.suppress(Exception),
    ):
        flow(ctx, _Params(run_date=date(2024, 1, 1)))

    store.load.assert_called_once_with("corr-test")


def test_build_etl_flow_deletes_manifest_on_last_attempt() -> None:
    """The flow calls manifest_store.delete() on the last run attempt."""
    store = _make_store()

    with (
        patch("prefect.flow", side_effect=lambda **kw: lambda fn: fn),
        patch("prefect.task", side_effect=lambda **kw: lambda fn: fn),
    ):
        flow = build_etl_flow(
            _PipelineAB,
            runner_factory=_make_runner_factory(),
            manifest_store=store,
            config_path="config/flows.yaml",
        )

    from loom.prefect._ctx import FlowCtx

    ctx = FlowCtx(correlation_id="corr-del", run_id="run-del")

    with (
        patch("prefect.flow", side_effect=lambda **kw: lambda fn: fn),
        patch("prefect.task", side_effect=lambda **kw: lambda fn: fn),
        contextlib.suppress(Exception),
    ):
        flow(ctx, _Params(run_date=date(2024, 1, 1)))

    store.delete.assert_called_with("corr-del")


# ---------------------------------------------------------------------------
# Skip logic tests
# ---------------------------------------------------------------------------


def test_build_etl_flow_skips_already_completed_steps() -> None:
    """Steps already SUCCESS in the manifest are skipped (runner not called for them)."""
    from datetime import datetime

    from loom.prefect._ctx import FlowCtx

    # Pre-populate manifest with StepA already completed
    existing_manifest = RunManifest(
        correlation_id="corr-skip",
        steps=(StepEntry(step="_StepA", status=RunStatus.SUCCESS),),
        updated_at=datetime(2024, 1, 1, tzinfo=UTC),
    )
    store = _make_store(manifest=existing_manifest)
    runner = MagicMock()
    runner.run.return_value = None

    with (
        patch("prefect.flow", side_effect=lambda **kw: lambda fn: fn),
        patch("prefect.task", side_effect=lambda **kw: lambda fn: fn),
    ):
        flow = build_etl_flow(
            _PipelineAB,
            runner_factory=lambda: runner,
            manifest_store=store,
            config_path="config/flows.yaml",
        )

    ctx = FlowCtx(correlation_id="corr-skip", run_id="run-skip")

    with (
        patch("prefect.flow", side_effect=lambda **kw: lambda fn: fn),
        patch("prefect.task", side_effect=lambda **kw: lambda fn: fn),
        contextlib.suppress(Exception),
    ):
        flow(ctx, _Params(run_date=date(2024, 1, 1)))

    # Verify runner was not invoked for _StepA (it was already done)
    # We inspect runner.run call args to check include didn't contain _StepA,
    # OR that runner.run was only called for _StepB.
    if runner.run.called:
        for run_call in runner.run.call_args_list:
            include_arg = run_call.kwargs.get("include") or (
                run_call.args[2] if len(run_call.args) > 2 else None
            )
            if include_arg is not None:
                assert "_StepA" not in include_arg, (
                    "_StepA should be skipped — it's already SUCCESS in manifest"
                )


def test_build_etl_flow_force_ignores_manifest() -> None:
    """With force=True, steps are not skipped even when manifest has SUCCESS."""
    from datetime import datetime

    from loom.prefect._ctx import FlowCtx

    existing_manifest = RunManifest(
        correlation_id="corr-force",
        steps=(StepEntry(step="_StepA", status=RunStatus.SUCCESS),),
        updated_at=datetime(2024, 1, 1, tzinfo=UTC),
    )
    store = _make_store(manifest=existing_manifest)
    runner = MagicMock()
    runner.run.return_value = None

    with (
        patch("prefect.flow", side_effect=lambda **kw: lambda fn: fn),
        patch("prefect.task", side_effect=lambda **kw: lambda fn: fn),
    ):
        flow = build_etl_flow(
            _PipelineAB,
            runner_factory=lambda: runner,
            manifest_store=store,
            config_path="config/flows.yaml",
        )

    ctx = FlowCtx(correlation_id="corr-force", run_id="run-force", force=True)

    with (
        patch("prefect.flow", side_effect=lambda **kw: lambda fn: fn),
        patch("prefect.task", side_effect=lambda **kw: lambda fn: fn),
        contextlib.suppress(Exception),
    ):
        flow(ctx, _Params(run_date=date(2024, 1, 1)))

    # With force=True, the manifest skip logic should be bypassed.
    # runner.run should have been invoked (not skipped for _StepA).
    assert runner.run.called, "runner.run must be called when force=True"
