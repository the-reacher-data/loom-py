"""Tests for ETL runner config loader."""

from __future__ import annotations

from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from loom.etl.lineage._config import ETLObservabilityConfig
from loom.etl.runner.config_loader import _load_yaml
from loom.etl.storage._config import StorageConfig


def test_load_yaml_reads_storage_and_observability_sections(tmp_path: Path) -> None:
    path = tmp_path / "loom.yaml"
    path.write_text(
        """
storage:
  defaults:
    table_path:
      uri: /var/lib/loom/lake
observability:
  log:
    enabled: false
  lineage:
    enabled: true
    root: s3://bucket/runs
""",
        encoding="utf-8",
    )

    storage, obs = _load_yaml(str(path))

    assert isinstance(storage, StorageConfig)
    assert storage.defaults.table_path is not None
    assert storage.defaults.table_path.uri == "/var/lib/loom/lake"
    assert isinstance(obs, ETLObservabilityConfig)
    assert obs.log.enabled is False
    assert obs.lineage.enabled is True
    assert obs.lineage.root == "s3://bucket/runs"


def test_load_yaml_uses_default_observability_when_missing(tmp_path: Path) -> None:
    path = tmp_path / "loom.yaml"
    path.write_text(
        "storage:\n  defaults:\n    table_path:\n      uri: /var/lib/loom/lake\n",
        encoding="utf-8",
    )

    storage, obs = _load_yaml(str(path))

    assert isinstance(storage, StorageConfig)
    assert storage.defaults.table_path is not None
    assert storage.defaults.table_path.uri == "/var/lib/loom/lake"
    assert obs == ETLObservabilityConfig()


def test_load_yaml_raises_when_storage_key_missing(tmp_path: Path) -> None:
    path = tmp_path / "loom.yaml"
    path.write_text("observability:\n  log:\n    enabled: true\n", encoding="utf-8")

    with pytest.raises(KeyError, match="storage"):
        _load_yaml(str(path))


def test_load_yaml_from_cloud_uri() -> None:
    yaml_content = (
        "storage:\n"
        "  defaults:\n"
        "    table_path:\n"
        "      uri: s3://my-lake/delta\n"
        "observability:\n"
        "  log:\n"
        "    enabled: true\n"
        "  lineage:\n"
        "    enabled: false\n"
    )

    mock_open = MagicMock()
    mock_open.return_value.__enter__ = MagicMock(return_value=StringIO(yaml_content))
    mock_open.return_value.__exit__ = MagicMock(return_value=False)

    with patch("fsspec.open", mock_open):
        storage, obs = _load_yaml("s3://my-bucket/config/prod.yaml")

    assert isinstance(storage, StorageConfig)
    assert storage.defaults.table_path is not None
    assert storage.defaults.table_path.uri == "s3://my-lake/delta"
    assert obs.log.enabled is True
    mock_open.assert_called_once_with("s3://my-bucket/config/prod.yaml", mode="r", encoding="utf-8")
