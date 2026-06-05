"""Tests for ``loom.prefect.deploy._yaml.read_yaml``."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from loom.prefect.deploy._yaml import read_yaml


def _write(tmp_path: Path, name: str, body: str) -> Path:
    p = tmp_path / name
    p.write_text(textwrap.dedent(body), encoding="utf-8")
    return p


def test_read_yaml_returns_top_level_mapping(tmp_path: Path) -> None:
    cfg = _write(tmp_path, "etl.yaml", "etl: foo\nparams:\n  a: 1\n")
    out = read_yaml(str(cfg))
    assert out["etl"] == "foo"
    assert out["params"] == {"a": 1}


def test_read_yaml_applies_single_level_extends(tmp_path: Path) -> None:
    _write(tmp_path, "base.yaml", "params:\n  shared: x\nschedule:\n  cron: '* * * * *'\n")
    cfg = _write(
        tmp_path,
        "etl.yaml",
        "extends: base.yaml\netl: foo\nparams:\n  shared: overridden\n  extra: y\n",
    )
    out = read_yaml(str(cfg))
    assert out["etl"] == "foo"
    assert out["schedule"] == {"cron": "* * * * *"}
    # Child override wins; child-only key is kept.
    assert out["params"] == {"shared": "overridden", "extra": "y"}
    # ``extends`` key is stripped from the merged result.
    assert "extends" not in out


def test_read_yaml_applies_extends_chain(tmp_path: Path) -> None:
    _write(tmp_path, "grand.yaml", "schedule:\n  cron: '0 0 * * *'\nparams:\n  a: 1\n  b: 2\n")
    _write(tmp_path, "parent.yaml", "extends: grand.yaml\nparams:\n  b: 20\n  c: 3\n")
    cfg = _write(tmp_path, "child.yaml", "extends: parent.yaml\netl: foo\nparams:\n  c: 30\n")
    out = read_yaml(str(cfg))
    assert out["etl"] == "foo"
    assert out["schedule"] == {"cron": "0 0 * * *"}
    # b overridden by parent, c overridden by child, a inherited from grand.
    assert out["params"] == {"a": 1, "b": 20, "c": 30}


def test_read_yaml_detects_extends_cycle(tmp_path: Path) -> None:
    _write(tmp_path, "a.yaml", "extends: b.yaml\nparams: {a: 1}\n")
    _write(tmp_path, "b.yaml", "extends: a.yaml\nparams: {b: 2}\n")
    with pytest.raises(ValueError, match="cycle"):
        read_yaml(str(tmp_path / "a.yaml"))


def test_read_yaml_extends_is_relative_to_each_file(tmp_path: Path) -> None:
    nested = tmp_path / "nested"
    nested.mkdir()
    _write(tmp_path, "root.yaml", "params:\n  inherited: from-root\n")
    _write(nested, "mid.yaml", "extends: ../root.yaml\nparams:\n  mid: middle\n")
    cfg = _write(nested, "leaf.yaml", "extends: mid.yaml\netl: foo\n")
    out = read_yaml(str(cfg))
    assert out["params"] == {"inherited": "from-root", "mid": "middle"}


def test_read_yaml_rejects_non_mapping_top_level(tmp_path: Path) -> None:
    cfg = _write(tmp_path, "list.yaml", "- item1\n- item2\n")
    with pytest.raises(ValueError, match="mapping"):
        read_yaml(str(cfg))
