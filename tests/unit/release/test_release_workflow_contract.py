"""Contract tests for the shape release.yml must keep to publish safely."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import yaml

WORKFLOW_PATH = Path(__file__).parents[3] / ".github" / "workflows" / "release.yml"


def _workflow() -> dict[str, Any]:
    return cast(dict[str, Any], yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8")))


def _triggers() -> dict[str, Any]:
    """Return the trigger mapping, whose YAML key ``on`` loads as a boolean."""
    workflow = cast(dict[Any, Any], _workflow())
    return cast(dict[str, Any], workflow.get("on", workflow.get(True)))


def _job(name: str) -> dict[str, Any]:
    return cast(dict[str, Any], cast(dict[str, Any], _workflow()["jobs"])[name])


def _steps(job: str) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], _job(job)["steps"])


def _step_named(job: str, name: str) -> tuple[int, dict[str, Any]]:
    for index, step in enumerate(_steps(job)):
        if step.get("name") == name:
            return index, step
    raise AssertionError(f"job {job!r} has no step named {name!r}")


class TestTrigger:
    def test_publishes_on_a_merged_pull_request_and_on_a_dispatch(self) -> None:
        triggers = _triggers()
        assert triggers["pull_request"]["types"] == ["closed"]
        assert "workflow_dispatch" in triggers

    def test_no_push_to_master_publishes(self) -> None:
        assert "push" not in _triggers()

    def test_the_label_and_the_merge_are_both_required(self) -> None:
        condition = cast(str, _job("plan")["if"])
        assert "merged == true" in condition
        assert "labels.*.name, 'release')" in condition

    def test_the_release_commit_is_the_merge_commit(self) -> None:
        assert "pull_request.merge_commit_sha" in cast(str, _job("plan")["env"]["MERGE_SHA"])


class TestPlan:
    def test_the_version_comes_from_the_planner(self) -> None:
        _, step = _step_named("plan", "Plan the release")
        assert "scripts.release.plan_release" in cast(str, step["run"])

    def test_a_commit_outside_master_is_refused(self) -> None:
        _, step = _step_named("plan", "Require the release commit on master")
        assert "merge-base --is-ancestor" in cast(str, step["run"])

    def test_the_tag_is_the_last_thing_the_plan_does(self) -> None:
        tag_index, _ = _step_named("plan", "Create the immutable version tag")
        assert tag_index == len(_steps("plan")) - 1

    def test_the_build_waits_for_the_tag(self) -> None:
        assert _job("build")["needs"] == "plan"

    def test_an_existing_tag_on_another_commit_refuses(self) -> None:
        _, step = _step_named("plan", "Create the immutable version tag")
        assert "expected ${MERGE_SHA}" in cast(str, step["run"])


class TestBuild:
    def test_builds_from_the_tag_with_its_history(self) -> None:
        checkout = _steps("build")[0]
        assert checkout["with"]["fetch-depth"] == 0
        assert "needs.plan.outputs.version" in cast(str, checkout["with"]["ref"])

    def test_the_built_version_must_match_the_tag(self) -> None:
        _, step = _step_named("build", "Require the built version to match the tag")
        assert "loom_kernel-${VERSION}-py3-none-any.whl" in cast(str, step["run"])

    def test_the_locked_resolution_is_enforced(self) -> None:
        _, step = _step_named("build", "Build package")
        assert "uv lock --check" in cast(str, step["run"])


class TestPublish:
    def test_publishing_waits_for_the_plan_and_the_build(self) -> None:
        assert cast(list[str], _job("publish")["needs"]) == ["plan", "build"]

    def test_no_skip_existing_hides_a_partial_upload(self) -> None:
        for step in _steps("publish"):
            assert "skip-existing" not in str(step.get("with", {}))

    def test_the_released_commit_is_validated_on_master(self) -> None:
        _, step = _step_named("publish", "Validate the released commit on master")
        assert "ci-main.yml" in cast(str, step["run"])
        assert step["continue-on-error"] is True
