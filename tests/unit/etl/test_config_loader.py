"""Tests for runner config loader helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.etl.observability.config import ObservabilityConfig
from loom.etl.runner.config_loader import _load_yaml, _parse_yaml_content, _read_yaml_file
from loom.etl.storage._config import DeltaConfig


def test_parse_yaml_content_reads_storage_and_observability_sections() -> None:
    content = """
storage:
  type: delta
  root: /var/lib/loom/lake
observability:
  log: false
  slow_step_threshold_ms: 1500
"""

    storage, obs = _parse_yaml_content(content)

    assert isinstance(storage, DeltaConfig)
    assert storage.root == "/var/lib/loom/lake"
    assert isinstance(obs, ObservabilityConfig)
    assert obs.log is False
    assert obs.slow_step_threshold_ms == 1500


def test_parse_yaml_content_uses_default_observability_when_missing() -> None:
    storage, obs = _parse_yaml_content("storage:\n  root: /var/lib/loom/lake\n")

    assert isinstance(storage, DeltaConfig)
    assert storage.root == "/var/lib/loom/lake"
    assert obs == ObservabilityConfig()


@pytest.mark.parametrize(
    "content,exc,match",
    [
        ("- just\n- a\n- list\n", TypeError, "Expected a mapping"),
        ("observability:\n  log: true\n", KeyError, "storage"),
    ],
)
def test_parse_yaml_content_errors(content: str, exc: type[Exception], match: str) -> None:
    with pytest.raises(exc, match=match):
        _parse_yaml_content(content)


def test_read_yaml_file_reads_raw_text(tmp_path: Path) -> None:
    path = tmp_path / "loom.yaml"
    path.write_text("storage:\n  root: /var/lib/loom/lake\n", encoding="utf-8")

    assert _read_yaml_file(path) == "storage:\n  root: /var/lib/loom/lake\n"


def test_load_yaml_reads_and_parses_file(tmp_path: Path) -> None:
    path = tmp_path / "loom.yaml"
    path.write_text(
        """
storage:
  root: /var/lib/loom/lake
observability:
  log: true
""",
        encoding="utf-8",
    )

    storage, obs = _load_yaml(path)
    assert isinstance(storage, DeltaConfig)
    assert storage.root == "/var/lib/loom/lake"
    assert obs.log is True
