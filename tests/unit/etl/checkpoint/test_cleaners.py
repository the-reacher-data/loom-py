"""Unit tests for cloud checkpoint cleaners."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import fsspec.core as fsspec_core
import pytest

from loom.etl.checkpoint._cleaners import CheckpointCleaner


def test_cleaner_calls_rm_when_path_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_fs = MagicMock()
    mock_fs.exists.return_value = True
    monkeypatch.setattr(
        fsspec_core,
        "url_to_fs",
        lambda *_args, **_kwargs: (mock_fs, "/bucket/tmp"),
    )

    CheckpointCleaner().delete_tree("s3://bucket/tmp")

    mock_fs.rm.assert_called_once_with("/bucket/tmp", recursive=True)


def test_cleaner_skips_rm_when_path_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_fs = MagicMock()
    mock_fs.exists.return_value = False
    monkeypatch.setattr(
        fsspec_core,
        "url_to_fs",
        lambda *_args, **_kwargs: (mock_fs, "/bucket/tmp"),
    )
    CheckpointCleaner().delete_tree("s3://bucket/tmp")

    mock_fs.rm.assert_not_called()


def test_cleaner_logs_warning_on_exception(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(*_args: object, **_kwargs: object) -> tuple[object, str]:
        raise Exception("no credentials")

    monkeypatch.setattr(fsspec_core, "url_to_fs", _raise)

    import logging

    log_ctx = caplog.at_level(logging.WARNING, logger="loom.etl.checkpoint._cleaners")
    with log_ctx:
        CheckpointCleaner().delete_tree("s3://bucket/tmp")

    assert any("cleanup skipped" in r.message for r in caplog.records)


def test_cleaner_does_not_raise_on_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(*_args: object, **_kwargs: object) -> tuple[object, str]:
        raise RuntimeError("boom")

    monkeypatch.setattr(fsspec_core, "url_to_fs", _raise)
    CheckpointCleaner().delete_tree("s3://bucket/tmp")  # must not raise


# ---------------------------------------------------------------------------
# CheckpointStore integration — cleaner is called on cleanup
# ---------------------------------------------------------------------------


def test_checkpoint_store_cleanup_run(tmp_path: Path) -> None:
    from loom.etl.checkpoint import CheckpointStore

    store = CheckpointStore(root=str(tmp_path))
    run_dir = tmp_path / "runs" / "run-abc"
    run_dir.mkdir(parents=True)

    store.cleanup_run("run-abc")

    assert not run_dir.exists()
