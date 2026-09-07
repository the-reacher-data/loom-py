"""Contract tests for what loom-py's release delegates and what it must declare.

The mechanism lives in loom-actions; what stays here is the call, so these tests
pin the two things a reader of this repository could get wrong: the inputs the
reusable workflow needs to publish, and the fact that a merge without the label
publishes nothing.
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any, cast

import yaml

ROOT = Path(__file__).parents[3]
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "release.yml"
REUSABLE = "the-reacher-data/loom-actions/.github/workflows/release-on-label.yml"


def _workflow() -> dict[str, Any]:
    return cast(dict[str, Any], yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8")))


def _triggers() -> dict[str, Any]:
    workflow = cast(dict[Any, Any], _workflow())
    return cast(dict[str, Any], workflow.get("on", workflow.get(True)))


def _release_job() -> dict[str, Any]:
    return cast(dict[str, Any], cast(dict[str, Any], _workflow()["jobs"])["release"])


class TestTheCall:
    def test_the_release_is_delegated_to_a_pinned_reusable_workflow(self) -> None:
        uses = cast(str, _release_job()["uses"])
        reference, _, revision = uses.partition("@")
        assert reference == REUSABLE
        assert len(revision) == 40, "the reusable workflow must be pinned to a commit"

    def test_publishing_is_asked_for_with_the_name_it_checks(self) -> None:
        inputs = cast(dict[str, Any], _release_job()["with"])
        assert inputs["publish-to-pypi"] is True
        assert inputs["package-name"] == "loom-kernel"

    def test_the_call_grants_what_publishing_needs(self) -> None:
        permissions = cast(dict[str, Any], _release_job()["permissions"])
        assert permissions["contents"] == "write"
        assert permissions["id-token"] == "write"

    def test_a_dispatch_can_resume_a_halted_release(self) -> None:
        assert "merge_sha" in _triggers()["workflow_dispatch"]["inputs"]
        assert "inputs.merge_sha" in cast(str, _release_job()["with"]["merge-sha"])


class TestNothingElsePublishes:
    def test_a_push_to_master_does_not_release(self) -> None:
        assert "push" not in _triggers()

    def test_the_release_reacts_to_a_closed_pull_request(self) -> None:
        assert _triggers()["pull_request"]["types"] == ["closed"]

    def test_no_index_token_is_read_by_any_workflow(self) -> None:
        for workflow in (WORKFLOW_PATH.parent).glob("*.yml"):
            assert "PYPI_API_TOKEN" not in workflow.read_text(encoding="utf-8"), workflow.name


class TestTheVersionComesFromTheTag:
    def test_the_package_declares_no_static_version(self) -> None:
        pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        assert "version" not in pyproject["project"]
        assert pyproject["project"]["dynamic"] == ["version"]
        assert pyproject["tool"]["hatch"]["version"]["source"] == "vcs"

    def test_a_prerelease_version_carries_no_local_label(self) -> None:
        pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        raw = pyproject["tool"]["hatch"]["version"]["raw-options"]
        assert raw["local_scheme"] == "no-local-version"

    def test_commitizen_reads_the_version_from_the_tags(self) -> None:
        pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        commitizen = pyproject["tool"]["commitizen"]
        assert commitizen["version_provider"] == "scm"
        assert "version_files" not in commitizen
