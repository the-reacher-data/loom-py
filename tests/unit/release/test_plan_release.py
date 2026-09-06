"""Unit tests for the release a labelled merge ships."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scripts.release.plan_release import (
    ReleasePlanError,
    classify_branch,
    highest_part,
    next_version,
    plan_release,
)

_RULES = {
    "major": ("breaking/.*",),
    "minor": ("feature/.*", "feat/.*", "multifeature/.*"),
    "patch": ("hotfix/.*", "fix/.*", "refactor/.*", "perf/.*"),
    "release_ignore": ("wip/.*", "docs/.*", "chore/.*", "ci/.*", "test/.*", "build/.*"),
}


def _git(repository: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(repository), *arguments),
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _repository(tmp_path: Path, rules: str) -> Path:
    repository = tmp_path / "repo"
    repository.mkdir()
    _git(repository, "init", "--initial-branch=master")
    _git(repository, "config", "user.email", "release@example.com")
    _git(repository, "config", "user.name", "Release")
    (repository / "pyproject.toml").write_text(rules, encoding="utf-8")
    _git(repository, "add", "pyproject.toml")
    _git(repository, "commit", "-m", "chore: initial")
    return repository


def _rules_toml() -> str:
    lines = ["[tool.semantic_branch]"]
    for key, patterns in _RULES.items():
        rendered = ", ".join(f'"{pattern}"' for pattern in patterns)
        lines.append(f"{key} = [{rendered}]")
    return "\n".join(lines) + "\n"


def _commit(repository: Path, message: str) -> str:
    (repository / message.replace(":", "_").replace(" ", "_")).write_text("x", encoding="utf-8")
    _git(repository, "add", ".")
    _git(repository, "commit", "-m", message)
    return _git(repository, "rev-parse", "HEAD")


class TestClassifyBranch:
    def test_reads_the_part_a_prefix_asks_for(self) -> None:
        assert classify_branch("feat/x", _RULES) == "minor"
        assert classify_branch("fix/x", _RULES) == "patch"
        assert classify_branch("breaking/x", _RULES) == "major"

    def test_an_ignore_class_ships_nothing_and_is_not_an_anomaly(self) -> None:
        assert classify_branch("ci/x", _RULES) is None
        assert classify_branch("docs/x", _RULES) is None

    def test_an_unknown_prefix_refuses_instead_of_guessing(self) -> None:
        with pytest.raises(ReleasePlanError, match="matches no class"):
            classify_branch("spike/x", _RULES)


class TestHighestPart:
    def test_a_feature_in_the_batch_wins_over_every_fix(self) -> None:
        assert highest_part(["patch", "minor", "patch", None]) == "minor"

    def test_a_breaking_change_wins_over_a_feature(self) -> None:
        assert highest_part(["minor", "major", "patch"]) == "major"

    def test_returns_none_when_nothing_ships_a_version(self) -> None:
        assert highest_part([None, None]) is None


class TestNextVersion:
    @pytest.mark.parametrize(
        ("last_tag", "part", "expected"),
        [
            ("v1.10.0", "patch", "1.10.1"),
            ("v1.10.0", "minor", "1.11.0"),
            ("v1.10.0", "major", "2.0.0"),
            ("v1.9.9", "patch", "1.9.10"),
        ],
    )
    def test_raises_only_the_requested_part(self, last_tag: str, part: str, expected: str) -> None:
        assert next_version(last_tag, part) == expected

    def test_starts_from_zero_without_a_tag(self) -> None:
        assert next_version(None, "minor") == "0.1.0"


class TestPlanRelease:
    def test_a_batch_holding_a_feature_ships_a_minor(self, tmp_path: Path) -> None:
        repository = _repository(tmp_path, _rules_toml())
        _git(repository, "tag", "v1.10.0")
        first = _commit(repository, "fix: one")
        second = _commit(repository, "feat: two")
        third = _commit(repository, "ci: three")
        refs = {first: ("fix/one",), second: ("feat/two",), third: ("ci/three",)}

        plan = plan_release(repository, third, lambda sha: refs[sha])

        assert (plan.last_tag, plan.part, plan.version) == ("v1.10.0", "minor", "1.11.0")
        assert len(plan.shipped) == 3

    def test_the_marked_commit_carries_no_special_weight(self, tmp_path: Path) -> None:
        repository = _repository(tmp_path, _rules_toml())
        _git(repository, "tag", "v1.10.0")
        feature = _commit(repository, "feat: one")
        marked = _commit(repository, "fix: two")
        refs = {feature: ("feat/one",), marked: ("fix/two",)}

        plan = plan_release(repository, marked, lambda sha: refs[sha])

        assert plan.version == "1.11.0"

    def test_a_range_ending_before_a_feature_leaves_it_for_the_next_release(
        self, tmp_path: Path
    ) -> None:
        repository = _repository(tmp_path, _rules_toml())
        _git(repository, "tag", "v1.10.0")
        marked = _commit(repository, "fix: one")
        later = _commit(repository, "feat: two")
        refs = {marked: ("fix/one",), later: ("feat/two",)}

        plan = plan_release(repository, marked, lambda sha: refs[sha])

        assert plan.version == "1.10.1"

    def test_refuses_a_range_that_ships_no_version(self, tmp_path: Path) -> None:
        repository = _repository(tmp_path, _rules_toml())
        _git(repository, "tag", "v1.10.0")
        only = _commit(repository, "ci: one")

        with pytest.raises(ReleasePlanError, match="ships no version"):
            plan_release(repository, only, lambda _sha: ("ci/one",))

    def test_refuses_an_empty_range(self, tmp_path: Path) -> None:
        repository = _repository(tmp_path, _rules_toml())
        _git(repository, "tag", "v1.10.0")
        head = _git(repository, "rev-parse", "HEAD")

        with pytest.raises(ReleasePlanError, match="nothing to release"):
            plan_release(repository, head, lambda _sha: ("fix/x",))

    def test_refuses_a_commit_that_belongs_to_no_pull_request(self, tmp_path: Path) -> None:
        repository = _repository(tmp_path, _rules_toml())
        _git(repository, "tag", "v1.10.0")
        pushed = _commit(repository, "fix: direct")

        with pytest.raises(ReleasePlanError, match="belongs to no pull request"):
            plan_release(repository, pushed, lambda _sha: ())

    def test_refuses_an_unclassified_branch_instead_of_lowering_the_part(
        self, tmp_path: Path
    ) -> None:
        repository = _repository(tmp_path, _rules_toml())
        _git(repository, "tag", "v1.10.0")
        _commit(repository, "feat: one")
        marked = _commit(repository, "spike: two")
        with pytest.raises(ReleasePlanError, match="matches no class"):
            plan_release(repository, marked, lambda _sha: ("spike/two",))

    def test_renders_every_shipped_branch_for_an_operator(self, tmp_path: Path) -> None:
        repository = _repository(tmp_path, _rules_toml())
        _git(repository, "tag", "v1.10.0")
        marked = _commit(repository, "feat: one")

        rendered = plan_release(repository, marked, lambda _sha: ("feat/one",)).render()

        assert "version  : 1.11.0" in rendered
        assert "feat/one" in rendered
