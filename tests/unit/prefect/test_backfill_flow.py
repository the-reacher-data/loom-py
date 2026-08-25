"""Tests for the chunked backfill Prefect flow factory."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from prefect import Flow

from loom.etl import ETLParams, ETLPipeline, ETLProcess, ETLStep, FromTable, IntoTable
from loom.prefect import BackfillChunk, backfill_flow
from loom.prefect.flow import _backfill_body

_CHUNK_PROCESS = "StagingProcess"
_FINALIZE_PROCESS = "ModelRefreshProcess"


class _WindowParams(ETLParams, frozen=True):
    updated_at_from: datetime
    updated_at_to: datetime


class _StageStep(ETLStep[_WindowParams]):
    src = FromTable("raw.events")
    target = IntoTable("staging.events").replace()

    def execute(self, params: _WindowParams, *, src: Any = None, **frames: Any) -> Any:
        return src


class _RefreshStep(ETLStep[_WindowParams]):
    src = FromTable("staging.events")
    target = IntoTable("model.events").replace()

    def execute(self, params: _WindowParams, *, src: Any = None, **frames: Any) -> Any:
        return src


class StagingProcess(ETLProcess[_WindowParams]):
    steps = [_StageStep]


class ModelRefreshProcess(ETLProcess[_WindowParams]):
    steps = [_RefreshStep]


class _Pipeline(ETLPipeline[_WindowParams]):
    processes = [StagingProcess, ModelRefreshProcess]


def _write_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "backfill.yaml"
    config_path.write_text("params: {}\n", encoding="utf-8")
    return config_path


def _build_flow(
    tmp_path: Path,
    chunk: BackfillChunk = "day",
    per_chunk_processes: list[str] | None = None,
    finalize_processes: list[str] | None = None,
) -> Flow[..., None]:
    flow: Flow[..., None] = backfill_flow(
        name="backfill",
        pipeline=_Pipeline,
        params_type=_WindowParams,
        config_path=str(_write_config(tmp_path)),
        source_file=__file__,
        per_chunk_processes=per_chunk_processes or [_CHUNK_PROCESS],
        finalize_processes=finalize_processes or [_FINALIZE_PROCESS],
        window_start_field="updated_at_from",
        window_end_field="updated_at_to",
        chunk=chunk,
    )
    return flow


@pytest.fixture
def runner(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    runner = MagicMock()
    monkeypatch.setattr(_backfill_body.ETLRunner, "from_yaml", lambda *a, **k: runner)
    monkeypatch.setattr(_backfill_body, "prefect_flow_run_id", lambda: None)
    monkeypatch.setattr(_backfill_body, "install_log_bridge", lambda *a, **k: None)
    monkeypatch.setattr(_backfill_body, "uninstall_log_bridge", lambda *a, **k: None)
    monkeypatch.setattr(_backfill_body, "maybe_delete_manifest", lambda *a, **k: None)
    return runner


def _windows(runner: MagicMock) -> list[tuple[datetime, datetime]]:
    """Per-chunk windows from every runner.run call that ran per_chunk_processes."""
    out = []
    for call in runner.run.call_args_list:
        if call.kwargs["include"] == [_CHUNK_PROCESS]:
            params = call.args[1]
            out.append((params.updated_at_from, params.updated_at_to))
    return out


def test_n_day_window_yields_n_daily_runs(tmp_path: Path, runner: MagicMock) -> None:
    flow = _build_flow(tmp_path)
    flow.fn(
        updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
        updated_at_to=datetime(2024, 1, 4, tzinfo=UTC),
    )
    assert _windows(runner) == [
        (datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 2, tzinfo=UTC)),
        (datetime(2024, 1, 2, tzinfo=UTC), datetime(2024, 1, 3, tzinfo=UTC)),
        (datetime(2024, 1, 3, tzinfo=UTC), datetime(2024, 1, 4, tzinfo=UTC)),
    ]


def test_days_run_oldest_first(tmp_path: Path, runner: MagicMock) -> None:
    flow = _build_flow(tmp_path)
    flow.fn(
        updated_at_from=datetime(2024, 3, 10, tzinfo=UTC),
        updated_at_to=datetime(2024, 3, 13, tzinfo=UTC),
    )
    starts = [w[0] for w in _windows(runner)]
    assert starts == sorted(starts)


def test_partial_day_window_covers_whole_days(tmp_path: Path, runner: MagicMock) -> None:
    # from mid-day D0 to mid-day D2 -> whole days D0, D1, D2.
    flow = _build_flow(tmp_path)
    flow.fn(
        updated_at_from=datetime(2024, 1, 1, 13, 30, tzinfo=UTC),
        updated_at_to=datetime(2024, 1, 3, 5, 0, tzinfo=UTC),
    )
    assert _windows(runner) == [
        (datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 2, tzinfo=UTC)),
        (datetime(2024, 1, 2, tzinfo=UTC), datetime(2024, 1, 3, tzinfo=UTC)),
        (datetime(2024, 1, 3, tzinfo=UTC), datetime(2024, 1, 4, tzinfo=UTC)),
    ]


def test_finalize_runs_once_with_start_of_current_chunk(
    tmp_path: Path, runner: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        _backfill_body, "_now_utc", lambda: datetime(2024, 7, 24, 15, 30, tzinfo=UTC)
    )
    flow = _build_flow(tmp_path)
    flow.fn(
        updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
        updated_at_to=datetime(2024, 1, 3, tzinfo=UTC),
    )
    finalize_calls = [
        c for c in runner.run.call_args_list if c.kwargs["include"] == [_FINALIZE_PROCESS]
    ]
    assert len(finalize_calls) == 1
    finalize_params = finalize_calls[0].args[1]
    # start of the current day chunk, not now.
    assert finalize_params.updated_at_to == datetime(2024, 7, 24, tzinfo=UTC)


def test_start_from_skips_earlier_days(tmp_path: Path, runner: MagicMock) -> None:
    flow = _build_flow(tmp_path)
    flow.fn(
        updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
        updated_at_to=datetime(2024, 1, 5, tzinfo=UTC),
        start_from=datetime(2024, 1, 3, 9, 0, tzinfo=UTC),
    )
    assert [w[0] for w in _windows(runner)] == [
        datetime(2024, 1, 3, tzinfo=UTC),
        datetime(2024, 1, 4, tzinfo=UTC),
    ]


def test_start_from_before_window_start_skips_nothing(tmp_path: Path, runner: MagicMock) -> None:
    flow = _build_flow(tmp_path)
    flow.fn(
        updated_at_from=datetime(2024, 1, 3, tzinfo=UTC),
        updated_at_to=datetime(2024, 1, 5, tzinfo=UTC),
        start_from=datetime(2023, 12, 25, tzinfo=UTC),
    )
    assert [w[0] for w in _windows(runner)] == [
        datetime(2024, 1, 3, tzinfo=UTC),
        datetime(2024, 1, 4, tzinfo=UTC),
    ]


def test_each_chunk_runs_full_per_chunk_process_set(tmp_path: Path, runner: MagicMock) -> None:
    # No cross-chunk step skipping: every chunk passes the complete include set.
    flow = _build_flow(tmp_path)
    flow.fn(
        updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
        updated_at_to=datetime(2024, 1, 3, tzinfo=UTC),
    )
    day_calls = [c for c in runner.run.call_args_list if c.kwargs["include"] == [_CHUNK_PROCESS]]
    assert len(day_calls) == 2
    assert all(c.kwargs["include"] == [_CHUNK_PROCESS] for c in day_calls)


def test_daily_runs_use_day_scoped_correlation_ids(tmp_path: Path, runner: MagicMock) -> None:
    flow = _build_flow(tmp_path)
    flow.fn(
        updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
        updated_at_to=datetime(2024, 1, 3, tzinfo=UTC),
    )
    corr_ids = [
        c.kwargs["correlation_id"]
        for c in runner.run.call_args_list
        if c.kwargs["include"] == [_CHUNK_PROCESS]
    ]
    assert corr_ids == ["backfill-20240101", "backfill-20240102"]
    assert len(set(corr_ids)) == len(corr_ids)


def test_empty_window_still_finalizes_once(tmp_path: Path, runner: MagicMock) -> None:
    flow = _build_flow(tmp_path)
    flow.fn(
        updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
        updated_at_to=datetime(2024, 1, 1, tzinfo=UTC),
    )
    assert _windows(runner) == []
    finalize_calls = [
        c for c in runner.run.call_args_list if c.kwargs["include"] == [_FINALIZE_PROCESS]
    ]
    assert len(finalize_calls) == 1


def test_hour_chunk_slices_by_hour(tmp_path: Path, runner: MagicMock) -> None:
    flow = _build_flow(tmp_path, chunk="hour")
    flow.fn(
        updated_at_from=datetime(2024, 1, 1, 10, 20, tzinfo=UTC),
        updated_at_to=datetime(2024, 1, 1, 13, 0, tzinfo=UTC),
    )
    assert _windows(runner) == [
        (datetime(2024, 1, 1, 10, tzinfo=UTC), datetime(2024, 1, 1, 11, tzinfo=UTC)),
        (datetime(2024, 1, 1, 11, tzinfo=UTC), datetime(2024, 1, 1, 12, tzinfo=UTC)),
        (datetime(2024, 1, 1, 12, tzinfo=UTC), datetime(2024, 1, 1, 13, tzinfo=UTC)),
    ]


def test_month_chunk_slices_by_month_across_year_boundary(
    tmp_path: Path, runner: MagicMock
) -> None:
    flow = _build_flow(tmp_path, chunk="month")
    flow.fn(
        updated_at_from=datetime(2023, 11, 15, tzinfo=UTC),
        updated_at_to=datetime(2024, 2, 1, tzinfo=UTC),
    )
    assert _windows(runner) == [
        (datetime(2023, 11, 1, tzinfo=UTC), datetime(2023, 12, 1, tzinfo=UTC)),
        (datetime(2023, 12, 1, tzinfo=UTC), datetime(2024, 1, 1, tzinfo=UTC)),
        (datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 2, 1, tzinfo=UTC)),
    ]


def test_year_chunk_slices_by_calendar_year(tmp_path: Path, runner: MagicMock) -> None:
    # partial edges: mid-2022 to mid-2025 -> whole years 2022..2025.
    flow = _build_flow(tmp_path, chunk="year")
    flow.fn(
        updated_at_from=datetime(2022, 6, 15, 10, 30, tzinfo=UTC),
        updated_at_to=datetime(2025, 3, 1, tzinfo=UTC),
    )
    assert _windows(runner) == [
        (datetime(2022, 1, 1, tzinfo=UTC), datetime(2023, 1, 1, tzinfo=UTC)),
        (datetime(2023, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, tzinfo=UTC)),
        (datetime(2024, 1, 1, tzinfo=UTC), datetime(2025, 1, 1, tzinfo=UTC)),
        (datetime(2025, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC)),
    ]


def test_year_chunk_exact_boundary_excludes_end_year(tmp_path: Path, runner: MagicMock) -> None:
    # exclusive end exactly on Jan 1 -> that year is NOT backfilled.
    flow = _build_flow(tmp_path, chunk="year")
    flow.fn(
        updated_at_from=datetime(2023, 1, 1, tzinfo=UTC),
        updated_at_to=datetime(2025, 1, 1, tzinfo=UTC),
    )
    assert _windows(runner) == [
        (datetime(2023, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, tzinfo=UTC)),
        (datetime(2024, 1, 1, tzinfo=UTC), datetime(2025, 1, 1, tzinfo=UTC)),
    ]


def test_year_runs_use_year_scoped_correlation_ids(tmp_path: Path, runner: MagicMock) -> None:
    flow = _build_flow(tmp_path, chunk="year")
    flow.fn(
        updated_at_from=datetime(2023, 1, 1, tzinfo=UTC),
        updated_at_to=datetime(2025, 1, 1, tzinfo=UTC),
    )
    corr_ids = [
        c.kwargs["correlation_id"]
        for c in runner.run.call_args_list
        if c.kwargs["include"] == [_CHUNK_PROCESS]
    ]
    assert corr_ids == ["backfill-2023", "backfill-2024"]
    assert len(set(corr_ids)) == len(corr_ids)


def test_year_start_from_skips_earlier_years(tmp_path: Path, runner: MagicMock) -> None:
    flow = _build_flow(tmp_path, chunk="year")
    flow.fn(
        updated_at_from=datetime(2015, 1, 1, tzinfo=UTC),
        updated_at_to=datetime(2019, 1, 1, tzinfo=UTC),
        start_from=datetime(2017, 8, 3, 9, 0, tzinfo=UTC),
    )
    assert [w[0] for w in _windows(runner)] == [
        datetime(2017, 1, 1, tzinfo=UTC),
        datetime(2018, 1, 1, tzinfo=UTC),
    ]


def test_factory_rejects_unknown_per_chunk_process(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="per_chunk_processes"):
        _build_flow(tmp_path, per_chunk_processes=["NoSuchProcess"])


def test_factory_rejects_unknown_finalize_process(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="finalize_processes"):
        _build_flow(tmp_path, finalize_processes=["NoSuchStep"])


def test_factory_accepts_step_names(tmp_path: Path) -> None:
    # include= accepts step names too, so the factory validation must as well.
    flow = _build_flow(tmp_path, per_chunk_processes=["_StageStep"])
    assert flow.name == "backfill"
