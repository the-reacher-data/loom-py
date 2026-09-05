"""Contract tests for the optional-dependency extras declared by ``loom-kernel``.

Each extra must install what the subsystem it names imports, so the checks read
the installed distribution metadata (``Requires-Dist``) rather than the source
tree: that is what ``pip install "loom-kernel[<extra>]"`` resolves against.
"""

from __future__ import annotations

from importlib import metadata

from packaging.requirements import Requirement


def _extra_requirements(extra: str) -> dict[str, Requirement]:
    """Return the requirements published under ``extra``, keyed by project name."""
    declared = metadata.requires("loom-kernel") or []
    found: dict[str, Requirement] = {}
    for line in declared:
        requirement = Requirement(line)
        marker = requirement.marker
        if marker is None or not marker.evaluate({"extra": extra}):
            continue
        found[requirement.name.lower()] = requirement
    return found


def test_mongo_extra_installs_pymongo() -> None:
    requirements = _extra_requirements("mongo")

    assert "pymongo" in requirements
    assert str(requirements["pymongo"].specifier) == "<5.0,>=4.6"


def test_sqlalchemy_extra_pins_greenlet_explicitly() -> None:
    requirements = _extra_requirements("sqlalchemy")

    assert "sqlalchemy" in requirements
    assert "greenlet" in requirements
    assert requirements["greenlet"].specifier.contains("3.0")


def test_streaming_extra_skips_bytewax_on_python_313() -> None:
    marker = _extra_requirements("streaming")["bytewax"].marker

    assert marker is not None
    assert marker.evaluate({"extra": "streaming", "python_version": "3.12"})
    assert not marker.evaluate({"extra": "streaming", "python_version": "3.13"})
