from __future__ import annotations

import re
import shlex
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import yaml

WORKFLOW_PATH = Path(__file__).parents[3] / ".github" / "workflows" / "release.yml"


def _steps() -> list[dict[str, Any]]:
    workflow = cast(dict[str, Any], yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8")))
    jobs = cast(dict[str, Any], workflow["jobs"])
    return [
        step
        for job_name in ("prepare", "build", "publish")
        for step in cast(list[dict[str, Any]], cast(dict[str, Any], jobs[job_name])["steps"])
    ]


def _step_index_containing(command: str) -> int:
    for index, step in enumerate(_steps()):
        if command in cast(str, step.get("run", "")):
            return index
    raise AssertionError(f"workflow has no step containing {command!r}")


def _step_named(name: str) -> tuple[int, dict[str, Any]]:
    for index, step in enumerate(_steps()):
        if step.get("name") == name:
            return index, step
    raise AssertionError(f"workflow has no step named {name!r}")


def _step_run(name: str) -> str:
    _, step = _step_named(name)
    return cast(str, step["run"])


def _line_index(
    lines: list[str],
    predicate: Callable[[str], bool],
    description: str,
) -> int:
    for index, line in enumerate(lines):
        if predicate(line):
            return index
    raise AssertionError(f"workflow is missing {description}")


def _matching_fi(lines: list[str], opening_index: int) -> int:
    depth = 0
    for index in range(opening_index, len(lines)):
        command = lines[index].strip()
        if re.fullmatch(r"if\b.+;\s*then", command):
            depth += 1
        elif command == "fi":
            depth -= 1
            if depth == 0:
                return index
    raise AssertionError("release PR creation guard has no matching fi")


def test_syncs_project_version_only_when_creating_a_new_release_pr() -> None:
    create_step_index, create_step = _step_named("Generate version bump commit")
    sync_steps = [
        index
        for index, step in enumerate(_steps())
        if any(
            "sed -i" in line and "pyproject.toml" in line
            for line in cast(str, step.get("run", "")).splitlines()
        )
    ]

    assert sync_steps == [create_step_index] and "steps.pr-state.outputs.create == 'true'" in cast(
        str, create_step["if"]
    )


def test_regenerates_and_commits_lockfile_only_when_creating_release_pr() -> None:
    _, create_step = _step_named("Generate version bump commit")
    lines = cast(str, create_step["run"]).splitlines()
    uv_lock_index = _line_index(
        lines,
        lambda line: line.strip().startswith("uv lock"),
        "uv lock regeneration",
    )
    git_add_index = _line_index(
        lines,
        lambda line: line.strip().startswith("git add "),
        "release metadata staging",
    )

    assert (
        uv_lock_index < git_add_index
        and "uv.lock" in lines[git_add_index].split()
        and "GH_TOKEN" not in cast(dict[str, Any], create_step.get("env", {}))
    )


def test_validates_release_after_pr_preparation_and_before_build() -> None:
    preparation_step_index, _ = _step_named("Push version bump and create PR")
    validation_step_index = _step_index_containing("checkout_merged_release.py")
    build_step_index, _ = _step_named("Build package")

    assert preparation_step_index < validation_step_index < build_step_index


def test_workflow_delegates_all_merge_decisions_to_validated_helper() -> None:
    run_scripts = "\n".join(cast(str, step.get("run", "")) for step in _steps())

    assert "gh pr merge" not in run_scripts and "--auto" not in run_scripts


def test_build_uses_frozen_lockfile_and_checks_tracked_cleanliness_afterwards() -> None:
    lines = _step_run("Build package").splitlines()
    build_index = _line_index(
        lines,
        lambda line: "python -m build" in line,
        "package build command",
    )
    cleanliness_index = _line_index(
        lines,
        lambda line: "git status --porcelain" in line and "--untracked-files=no" in line,
        "post-build tracked-cleanliness check",
    )

    assert "--frozen" in shlex.split(lines[build_index]) and build_index < cleanliness_index


def test_passes_validated_release_sha_to_both_tag_commands() -> None:
    immutable = _step_run("Create immutable version tag")
    floating = _step_run("Update floating major tag")

    assert all('-f sha="${RELEASE_SHA}"' in script for script in (immutable, floating))


def test_automatic_and_manual_releases_share_stable_concurrency_group() -> None:
    workflow = cast(dict[str, Any], yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8")))
    concurrency = cast(dict[str, Any], workflow["concurrency"])
    group = cast(str, concurrency["group"])

    assert "github.ref" not in group and group == "release-${{ github.workflow }}"


def test_regression_v0160_attempt_two_checks_remote_immutable_tag_exactly() -> None:
    tag_script = _step_run("Create immutable version tag")

    assert (
        "git/matching-refs/tags/v${VERSION}" in tag_script
        and "REMOTE_VERSION_SHA" in tag_script
        and '"${REMOTE_VERSION_SHA}" != "${RELEASE_SHA}"' in tag_script
        and "exit 1" in tag_script
        and 'ref="${TAG_REF}"' in tag_script
    )


def test_updates_floating_major_tag_without_remote_delete_window() -> None:
    tag_script = _step_run("Update floating major tag")

    assert (
        "--method PATCH" in tag_script
        and '-f sha="${RELEASE_SHA}"' in tag_script
        and "-F force=true" in tag_script
        and "--method DELETE" not in tag_script
    )


def test_build_and_publish_jobs_use_minimum_permissions_and_no_persisted_credentials() -> None:
    workflow = cast(dict[str, Any], yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8")))
    jobs = cast(dict[str, Any], workflow["jobs"])
    build = cast(dict[str, Any], jobs["build"])
    publish = cast(dict[str, Any], jobs["publish"])
    prepare = cast(dict[str, Any], jobs["prepare"])
    prepare_checkout = next(
        step
        for step in cast(list[dict[str, Any]], prepare["steps"])
        if step.get("uses", "").startswith("actions/checkout@")
    )
    build_checkout = next(
        step
        for step in cast(list[dict[str, Any]], build["steps"])
        if step.get("uses", "").startswith("actions/checkout@")
    )

    assert build["permissions"] == {"contents": "read"}
    assert cast(dict[str, Any], prepare_checkout["with"])["persist-credentials"] is False
    assert cast(dict[str, Any], build_checkout["with"])["persist-credentials"] is False
    assert publish["permissions"] == {"contents": "write", "id-token": "write"}


def test_pypi_secret_is_scoped_to_publish_steps_and_skip_existing_is_forbidden() -> None:
    workflow_text = WORKFLOW_PATH.read_text(encoding="utf-8")
    workflow = cast(dict[str, Any], yaml.safe_load(workflow_text))
    jobs = cast(dict[str, Any], workflow["jobs"])

    assert "PYPI_API_TOKEN" not in cast(dict[str, Any], jobs["prepare"]).get("env", {})
    assert "PYPI_API_TOKEN" not in cast(dict[str, Any], jobs["build"]).get("env", {})
    assert "skip-existing" not in workflow_text


def test_tags_and_github_release_complete_before_irreversible_pypi_publish() -> None:
    immutable_index, _ = _step_named("Create immutable version tag")
    major_index, _ = _step_named("Update floating major tag")
    release_index, _ = _step_named("Create GitHub release")
    token_publish_index, _ = _step_named("Publish package to PyPI (token mode)")
    trusted_publish_index, _ = _step_named("Publish package to PyPI (trusted publisher)")

    assert (
        immutable_index < major_index < release_index < token_publish_index < trusted_publish_index
    )


def test_release_tooling_versions_are_pinned() -> None:
    workflow_text = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert 'pip install --force-reinstall "uv==0.10.2"' in workflow_text
    assert workflow_text.index("Generate release changelog") < workflow_text.index(
        "Install pinned uv after release actions"
    )
    assert '--with "build==1.3.0"' in workflow_text
    assert "pip install uv\n" not in workflow_text


def test_manual_release_is_restricted_to_master_checkout() -> None:
    workflow_text = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert 'if [ "${RELEASE_BRANCH}" != "master" ]; then' in workflow_text
    assert "github.event.pull_request.merge_commit_sha" in workflow_text


def test_restores_only_known_release_action_side_effects_before_pr_handling() -> None:
    restore_script = _step_run("Restore action side effects")

    assert (
        "pyproject.toml|uv.lock" in restore_script
        and "git restore --source=HEAD --staged --worktree -- pyproject.toml uv.lock"
        in restore_script
        and "unexpected tracked file" in restore_script
    )


def test_lockfile_validation_runs_only_in_unprivileged_build_job() -> None:
    workflow = cast(dict[str, Any], yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8")))
    jobs = cast(dict[str, Any], workflow["jobs"])
    prepare_text = "\n".join(
        cast(str, step.get("run", ""))
        for step in cast(list[dict[str, Any]], cast(dict[str, Any], jobs["prepare"])["steps"])
    )
    build_text = "\n".join(
        cast(str, step.get("run", ""))
        for step in cast(list[dict[str, Any]], cast(dict[str, Any], jobs["build"])["steps"])
    )

    assert "uv lock --check" not in prepare_text
    assert "uv lock --check" in build_text


def test_automatic_release_checkout_is_bound_to_triggering_merge_sha() -> None:
    workflow_text = WORKFLOW_PATH.read_text(encoding="utf-8")
    helper_script = _step_run("Check out validated release commit")

    assert (
        "github.event.pull_request.merge_commit_sha" in workflow_text
        and "--expected-base-sha" not in helper_script
    )
