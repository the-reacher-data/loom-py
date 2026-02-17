from __future__ import annotations

import tomllib
from pathlib import Path

from loom import __version__


def test_package_version_matches_pyproject() -> None:
    pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    expected = data["project"]["version"]
    assert __version__ == expected
