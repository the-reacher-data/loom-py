"""Unit tests for TempCleaner implementations and AutoTempCleaner dispatch."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from loom.etl._temp_cleaners import (
    AutoTempCleaner,
    DbutilsTempCleaner,
    FsspecTempCleaner,
    LocalTempCleaner,
    TempCleaner,
    _is_cloud_path,
)

# ---------------------------------------------------------------------------
# _is_cloud_path
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path,expected",
    [
        ("/tmp/loom/runs/abc", False),
        ("relative/path", False),
        ("s3://bucket/tmp", True),
        ("gs://bucket/tmp", True),
        ("gcs://bucket/tmp", True),
        ("abfss://container@account/tmp", True),
        ("abfs://container@account/tmp", True),
        ("dbfs:/tmp/loom", True),
        ("az://container/tmp", True),
    ],
)
def test_is_cloud_path(path: str, expected: bool) -> None:
    assert _is_cloud_path(path) is expected


# ---------------------------------------------------------------------------
# TempCleaner Protocol
# ---------------------------------------------------------------------------


def test_temp_cleaner_protocol_satisfied_by_all_implementations() -> None:
    assert isinstance(LocalTempCleaner(), TempCleaner)
    assert isinstance(FsspecTempCleaner(), TempCleaner)
    assert isinstance(DbutilsTempCleaner(MagicMock()), TempCleaner)
    assert isinstance(AutoTempCleaner(), TempCleaner)


# ---------------------------------------------------------------------------
# LocalTempCleaner
# ---------------------------------------------------------------------------


def test_local_cleaner_removes_existing_directory(tmp_path: Path) -> None:
    target = tmp_path / "runs" / "abc123"
    target.mkdir(parents=True)
    (target / "data.arrow").write_bytes(b"x")

    LocalTempCleaner().delete_tree(str(target))

    assert not target.exists()


def test_local_cleaner_noop_on_missing_path(tmp_path: Path) -> None:
    missing = str(tmp_path / "nonexistent")
    LocalTempCleaner().delete_tree(missing)  # must not raise


# ---------------------------------------------------------------------------
# FsspecTempCleaner
# ---------------------------------------------------------------------------


def _mock_fsspec(fs: MagicMock, fpath: str) -> MagicMock:
    """Return a mock fsspec module whose url_to_fs returns (fs, fpath)."""
    mock_module = MagicMock()
    mock_module.core.url_to_fs.return_value = (fs, fpath)
    return mock_module


def test_fsspec_cleaner_calls_rm_when_path_exists() -> None:
    mock_fs = MagicMock()
    mock_fs.exists.return_value = True
    mock_module = _mock_fsspec(mock_fs, "/bucket/tmp")

    with patch.dict(sys.modules, {"fsspec": mock_module, "fsspec.core": mock_module.core}):
        FsspecTempCleaner().delete_tree("s3://bucket/tmp")

    mock_module.core.url_to_fs.assert_called_once_with("s3://bucket/tmp")
    mock_fs.rm.assert_called_once_with("/bucket/tmp", recursive=True)


def test_fsspec_cleaner_skips_rm_when_path_absent() -> None:
    mock_fs = MagicMock()
    mock_fs.exists.return_value = False
    mock_module = _mock_fsspec(mock_fs, "/bucket/tmp")

    with patch.dict(sys.modules, {"fsspec": mock_module, "fsspec.core": mock_module.core}):
        FsspecTempCleaner().delete_tree("s3://bucket/tmp")

    mock_fs.rm.assert_not_called()


def test_fsspec_cleaner_logs_warning_on_exception(caplog: pytest.LogCaptureFixture) -> None:
    mock_module = MagicMock()
    mock_module.core.url_to_fs.side_effect = Exception("no credentials")

    import logging

    modules = {"fsspec": mock_module, "fsspec.core": mock_module.core}
    log_ctx = caplog.at_level(logging.WARNING, logger="loom.etl._temp_cleaners")
    with patch.dict(sys.modules, modules), log_ctx:
        FsspecTempCleaner().delete_tree("s3://bucket/tmp")

    assert any("cleanup skipped" in r.message for r in caplog.records)


def test_fsspec_cleaner_does_not_raise_on_exception() -> None:
    mock_module = MagicMock()
    mock_module.core.url_to_fs.side_effect = RuntimeError("boom")

    with patch.dict(sys.modules, {"fsspec": mock_module, "fsspec.core": mock_module.core}):
        FsspecTempCleaner().delete_tree("s3://bucket/tmp")  # must not raise


# ---------------------------------------------------------------------------
# DbutilsTempCleaner
# ---------------------------------------------------------------------------


def test_dbutils_cleaner_calls_fs_rm() -> None:
    dbutils = MagicMock()
    DbutilsTempCleaner(dbutils).delete_tree("dbfs:/tmp/loom/runs/abc")
    dbutils.fs.rm.assert_called_once_with("dbfs:/tmp/loom/runs/abc", recurse=True)


def test_dbutils_cleaner_logs_warning_on_exception(caplog: pytest.LogCaptureFixture) -> None:
    dbutils = MagicMock()
    dbutils.fs.rm.side_effect = Exception("permission denied")
    import logging

    with caplog.at_level(logging.WARNING, logger="loom.etl._temp_cleaners"):
        DbutilsTempCleaner(dbutils).delete_tree("dbfs:/tmp/loom/runs/abc")

    assert any("cleanup skipped" in r.message for r in caplog.records)


def test_dbutils_cleaner_does_not_raise_on_exception() -> None:
    dbutils = MagicMock()
    dbutils.fs.rm.side_effect = RuntimeError("boom")
    DbutilsTempCleaner(dbutils).delete_tree("dbfs:/tmp/loom/runs/abc")  # must not raise


# ---------------------------------------------------------------------------
# AutoTempCleaner — dispatch
# ---------------------------------------------------------------------------


def test_auto_cleaner_dispatches_local_path_to_local(tmp_path: Path) -> None:
    target = tmp_path / "runs" / "abc"
    target.mkdir(parents=True)

    AutoTempCleaner().delete_tree(str(target))

    assert not target.exists()


def test_auto_cleaner_dispatches_cloud_path_to_fsspec() -> None:
    mock_fs = MagicMock()
    mock_fs.exists.return_value = True
    mock_module = _mock_fsspec(mock_fs, "/bucket/tmp")

    with patch.dict(sys.modules, {"fsspec": mock_module, "fsspec.core": mock_module.core}):
        AutoTempCleaner().delete_tree("s3://bucket/tmp/runs/abc")

    mock_fs.rm.assert_called_once_with("/bucket/tmp", recursive=True)


def test_auto_cleaner_dispatches_dbfs_path_to_fsspec() -> None:
    """dbfs:/ is a cloud path — AutoTempCleaner routes to FsspecTempCleaner."""
    mock_fs = MagicMock()
    mock_fs.exists.return_value = False
    mock_module = _mock_fsspec(mock_fs, "/dbfs/tmp")

    with patch.dict(sys.modules, {"fsspec": mock_module, "fsspec.core": mock_module.core}):
        AutoTempCleaner().delete_tree("dbfs:/tmp/loom/runs/abc")

    mock_module.core.url_to_fs.assert_called_once()


# ---------------------------------------------------------------------------
# IntermediateStore integration — cleaner is called on cleanup
# ---------------------------------------------------------------------------


def test_intermediate_store_uses_injected_cleaner(tmp_path: Path) -> None:
    from loom.etl._temp_store import IntermediateStore

    called: list[str] = []

    class SpyCleaner:
        def delete_tree(self, path: str) -> None:
            called.append(path)

    store = IntermediateStore(tmp_root=str(tmp_path), cleaner=SpyCleaner())
    store.cleanup_run("run-123")

    assert len(called) == 1
    assert called[0].endswith("runs/run-123")


def test_intermediate_store_uses_auto_cleaner_by_default(tmp_path: Path) -> None:
    from loom.etl._temp_store import IntermediateStore

    store = IntermediateStore(tmp_root=str(tmp_path))
    run_dir = tmp_path / "runs" / "run-abc"
    run_dir.mkdir(parents=True)

    store.cleanup_run("run-abc")

    assert not run_dir.exists()


def test_cleanup_stale_warns_for_cloud_root(caplog: pytest.LogCaptureFixture) -> None:
    import logging

    from loom.etl._temp_store import IntermediateStore

    store = IntermediateStore(tmp_root="s3://bucket/tmp")
    with caplog.at_level(logging.WARNING, logger="loom.etl._temp_store"):
        store.cleanup_stale(older_than_seconds=3600)

    assert any("lifecycle policy" in r.message for r in caplog.records)
