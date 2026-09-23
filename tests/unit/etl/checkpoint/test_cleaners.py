"""Unit tests for cloud checkpoint cleaners."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import fsspec.core as fsspec_core
import pytest

from loom.etl.checkpoint._cleaners import CheckpointCleaner, _checkpoint_storage_options


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
        raise PermissionError("no credentials")

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


# ---------------------------------------------------------------------------
# Storage options filter — server-side encryption survives it
# ---------------------------------------------------------------------------


def test_storage_options_keep_encryption_without_endpoint() -> None:
    options = _checkpoint_storage_options(
        {
            "aws_server_side_encryption": "aws:kms",
            "aws_sse_kms_key_id": "alias/loom-temp",
            "aws_sse_bucket_key_enabled": "true",
        }
    )

    assert options == {
        "s3_additional_kwargs": {
            "ServerSideEncryption": "aws:kms",
            "SSEKMSKeyId": "alias/loom-temp",
            "BucketKeyEnabled": True,
        }
    }


def test_storage_options_keep_encryption_with_endpoint() -> None:
    options = _checkpoint_storage_options(
        {
            "endpoint_url": "http://minio:9000",
            "access_key_id": "ak",
            "secret_access_key": "sk",
            "aws_server_side_encryption": "aws:kms",
        }
    )

    assert options == {
        "endpoint_url": "http://minio:9000",
        "key": "ak",
        "secret": "sk",
        "s3_additional_kwargs": {"ServerSideEncryption": "aws:kms"},
    }


def test_storage_options_accept_uppercase_encryption_keys() -> None:
    options = _checkpoint_storage_options(
        {
            "AWS_SERVER_SIDE_ENCRYPTION": "aws:kms",
            "AWS_SSE_KMS_KEY_ID": "alias/loom-temp",
        }
    )

    assert options["s3_additional_kwargs"] == {
        "ServerSideEncryption": "aws:kms",
        "SSEKMSKeyId": "alias/loom-temp",
    }


def test_storage_options_drop_unsupported_keys(caplog: pytest.LogCaptureFixture) -> None:
    import logging

    log_ctx = caplog.at_level(logging.DEBUG, logger="loom.etl.checkpoint._cleaners")
    with log_ctx:
        options = _checkpoint_storage_options({"aws_region": "eu-west-1", "aws_allow_http": "true"})

    # Previously the whole mapping collapsed to {} unless an endpoint was set;
    # now only the keys fsspec cannot take are dropped, and never in silence.
    assert options == {}
    assert any("storage options ignored" in r.message for r in caplog.records)


def test_cleaner_forwards_encryption_to_fsspec(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}
    mock_fs = MagicMock()
    mock_fs.exists.return_value = True

    def _url_to_fs(_path: str, **kwargs: object) -> tuple[object, str]:
        seen.update(kwargs)
        return mock_fs, "/bucket/tmp"

    monkeypatch.setattr(fsspec_core, "url_to_fs", _url_to_fs)

    cleaner = CheckpointCleaner(storage_options={"aws_server_side_encryption": "aws:kms"})
    cleaner.delete_tree("s3://bucket/tmp")

    assert seen == {"s3_additional_kwargs": {"ServerSideEncryption": "aws:kms"}}
