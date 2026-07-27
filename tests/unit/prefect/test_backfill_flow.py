"""Tests for the day-by-day backfill Prefect flow factory."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from loom.etl.pipeline import ETLParams, ETLPipeline
from loom.prefect.flow import _backfill


class _WindowParams(ETLParams, frozen=True):
    updated_at_from: datetime
    updated_at_to: datetime


class _Pipeline(ETLPipeline[_WindowParams]):
    processes = []


def _write_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "backfill.yaml"
    config_path.write_text("params: {}\n", encoding="utf-8")
    return config_path


def _build_flow(tmp_path: Path, chunk: str = "day") -> object:
    return _backfill.backfill_flow(
        name="backfill",
        pipeline=_Pipeline,
        params_type=_WindowParams,
        config_path=str(_write_config(tmp_path)),
        source_file=__file__,
        per_chunk_processes=["StagingProcess"],
        finalize_processes=["ModelRefreshProcess"],
        chunk=chunk,  # type: ignore[arg-type]
    )


@pytest.fixture
def runner(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    runner = MagicMock()
    monkeypatch.setattr(_backfill.ETLRunner, "from_yaml", lambda *a, **k: runner)
    monkeypatch.setattr(_backfill, "prefect_flow_run_id", lambda: None)
    monkeypatch.setattr(_backfill, "install_log_bridge", lambda *a, **k: None)
    monkeypatch.setattr(_backfill, "uninstall_log_bridge", lambda *a, **k: None)
    monkeypatch.setattr(_backfill, "_maybe_delete_manifest", lambda *a, **k: None)
    return runner


def _windows(runner: MagicMock) -> list[tuple[datetime, datetime]]:
    """Per-chunk windows from every runner.run call that ran per_chunk_processes."""
    out = []
    for call in runner.run.call_args_list:
        if call.kwargs["include"] == ["StagingProcess"]:
            params = call.args[1]
            out.append((params.updated_at_from, params.updated_at_to))
    return out


def test_n_day_window_yields_n_daily_runs(tmp_path: Path, runner: MagicMock) -> None:
    """chunk='day' — the granularity CUIMO uses."""
    flow = _build_flow(tmp_path)
    flow.fn(  # type: ignore[attr-defined]
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
    flow.fn(  # type: ignore[attr-defined]
        updated_at_from=datetime(2024, 3, 10, tzinfo=UTC),
        updated_at_to=datetime(2024, 3, 13, tzinfo=UTC),
    )
    starts = [w[0] for w in _windows(runner)]
    assert starts == sorted(starts)


def test_partial_day_window_covers_whole_days(tmp_path: Path, runner: MagicMock) -> None:
    # from mid-day D0 to mid-day D2 -> whole days D0, D1, D2.
    flow = _build_flow(tmp_path)
    flow.fn(  # type: ignore[attr-defined]
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
    monkeypatch.setattr(_backfill, "_now_utc", lambda: datetime(2024, 7, 24, 15, 30, tzinfo=UTC))
    flow = _build_flow(tmp_path)
    flow.fn(  # type: ignore[attr-defined]
        updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
        updated_at_to=datetime(2024, 1, 3, tzinfo=UTC),
    )
    finalize_calls = [
        c for c in runner.run.call_args_list if c.kwargs["include"] == ["ModelRefreshProcess"]
    ]
    assert len(finalize_calls) == 1
    finalize_params = finalize_calls[0].args[1]
    # start of the current day chunk, not now.
    assert finalize_params.updated_at_to == datetime(2024, 7, 24, tzinfo=UTC)


def test_start_from_skips_earlier_days(tmp_path: Path, runner: MagicMock) -> None:
    flow = _build_flow(tmp_path)
    flow.fn(  # type: ignore[attr-defined]
        updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
        updated_at_to=datetime(2024, 1, 5, tzinfo=UTC),
        start_from=datetime(2024, 1, 3, 9, 0, tzinfo=UTC),
    )
    assert [w[0] for w in _windows(runner)] == [
        datetime(2024, 1, 3, tzinfo=UTC),
        datetime(2024, 1, 4, tzinfo=UTC),
    ]


def test_each_chunk_runs_full_per_chunk_process_set(tmp_path: Path, runner: MagicMock) -> None:
    # No cross-chunk step skipping: every chunk passes the complete include set.
    flow = _build_flow(tmp_path)
    flow.fn(  # type: ignore[attr-defined]
        updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
        updated_at_to=datetime(2024, 1, 3, tzinfo=UTC),
    )
    day_calls = [c for c in runner.run.call_args_list if c.kwargs["include"] == ["StagingProcess"]]
    assert len(day_calls) == 2
    assert all(c.kwargs["include"] == ["StagingProcess"] for c in day_calls)


def test_daily_runs_use_day_scoped_correlation_ids(tmp_path: Path, runner: MagicMock) -> None:
    flow = _build_flow(tmp_path)
    flow.fn(  # type: ignore[attr-defined]
        updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
        updated_at_to=datetime(2024, 1, 3, tzinfo=UTC),
    )
    corr_ids = [
        c.kwargs["correlation_id"]
        for c in runner.run.call_args_list
        if c.kwargs["include"] == ["StagingProcess"]
    ]
    assert corr_ids == ["backfill-20240101", "backfill-20240102"]
    assert len(set(corr_ids)) == len(corr_ids)


def test_empty_window_still_finalizes_once(tmp_path: Path, runner: MagicMock) -> None:
    flow = _build_flow(tmp_path)
    flow.fn(  # type: ignore[attr-defined]
        updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
        updated_at_to=datetime(2024, 1, 1, tzinfo=UTC),
    )
    assert _windows(runner) == []
    finalize_calls = [
        c for c in runner.run.call_args_list if c.kwargs["include"] == ["ModelRefreshProcess"]
    ]
    assert len(finalize_calls) == 1


def test_hour_chunk_slices_by_hour(tmp_path: Path, runner: MagicMock) -> None:
    flow = _build_flow(tmp_path, chunk="hour")
    flow.fn(  # type: ignore[attr-defined]
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
    flow.fn(  # type: ignore[attr-defined]
        updated_at_from=datetime(2023, 11, 15, tzinfo=UTC),
        updated_at_to=datetime(2024, 2, 1, tzinfo=UTC),
    )
    assert _windows(runner) == [
        (datetime(2023, 11, 1, tzinfo=UTC), datetime(2023, 12, 1, tzinfo=UTC)),
        (datetime(2023, 12, 1, tzinfo=UTC), datetime(2024, 1, 1, tzinfo=UTC)),
        (datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 2, 1, tzinfo=UTC)),
    ]
