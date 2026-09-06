from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scripts.release import build_release_notes as notes


def _git(repository: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repository), *arguments],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _repository(tmp_path: Path) -> Path:
    repository = tmp_path / "repo"
    repository.mkdir()
    _git(repository, "init", "--initial-branch=master")
    _git(repository, "config", "user.name", "Release Test")
    _git(repository, "config", "user.email", "release-test@example.com")
    return repository


def _commit(repository: Path, subject: str) -> str:
    marker = repository / "history.txt"
    marker.write_text(f"{subject}\n", encoding="utf-8")
    _git(repository, "add", "history.txt")
    _git(repository, "commit", "-m", subject)
    return _git(repository, "rev-parse", "HEAD")


def test_every_commit_is_listed_when_the_repository_has_no_release_tag(tmp_path: Path) -> None:
    repository = _repository(tmp_path)
    _commit(repository, "feat(core): first")
    _commit(repository, "fix(core): second")

    assert notes.latest_release_tag(repository) is None
    assert notes.build_release_notes(repository, "0.1.0") == (
        "# 🚀 Release 0.1.0\n\nChanges:\n\n- fix(core): second\n- feat(core): first\n"
    )


def test_only_commits_after_the_last_tag_are_listed(tmp_path: Path) -> None:
    repository = _repository(tmp_path)
    _commit(repository, "feat(core): released already")
    _git(repository, "tag", "v1.9.3")
    _commit(repository, "feat(config): compose configuration across files")

    assert notes.build_release_notes(repository, "1.10.0") == (
        "# 🚀 Release 1.10.0\n\nChanges since v1.9.3:\n\n"
        "- feat(config): compose configuration across files\n"
    )


def test_a_tag_outside_the_history_of_head_is_ignored(tmp_path: Path) -> None:
    repository = _repository(tmp_path)
    _commit(repository, "feat(core): trunk")
    _git(repository, "checkout", "-b", "sidetrack")
    _commit(repository, "feat(core): only on the sidetrack")
    _git(repository, "tag", "v9.9.9")
    _git(repository, "checkout", "master")
    _commit(repository, "fix(core): on master after the sidetrack tag")

    assert notes.latest_release_tag(repository) is None
    assert "only on the sidetrack" not in notes.build_release_notes(repository, "0.1.0")


def test_release_tags_are_ordered_by_version_and_not_lexically(tmp_path: Path) -> None:
    repository = _repository(tmp_path)
    _commit(repository, "feat(core): base")
    _git(repository, "tag", "v1.9.9")
    _commit(repository, "fix(core): after the ninth patch")
    _git(repository, "tag", "v1.9.10")
    _commit(repository, "fix(core): after the tenth patch")

    assert notes.latest_release_tag(repository) == "v1.9.10"
    assert notes.build_release_notes(repository, "1.9.11") == (
        "# 🚀 Release 1.9.11\n\nChanges since v1.9.10:\n\n- fix(core): after the tenth patch\n"
    )


def test_a_release_with_no_accumulated_commits_is_refused(tmp_path: Path) -> None:
    repository = _repository(tmp_path)
    _commit(repository, "feat(core): released already")
    _git(repository, "tag", "v1.9.3")

    with pytest.raises(notes.ReleaseNotesError, match="nothing to release"):
        notes.build_release_notes(repository, "1.9.4")


def test_merge_commits_are_left_out_of_the_notes(tmp_path: Path) -> None:
    repository = _repository(tmp_path)
    _commit(repository, "feat(core): base")
    _git(repository, "tag", "v1.0.0")
    _git(repository, "checkout", "-b", "topic")
    _commit(repository, "feat(core): topic work")
    _git(repository, "checkout", "master")
    _git(repository, "merge", "--no-ff", "-m", "Merge pull request #1 from topic", "topic")

    entries = notes.release_entries(repository, "v1.0.0")

    assert entries == ("feat(core): topic work",)


def test_notes_are_written_to_the_requested_file(tmp_path: Path) -> None:
    repository = _repository(tmp_path)
    _commit(repository, "feat(core): first")
    output = tmp_path / "CHANGELOG_RELEASE.md"

    exit_code = notes.main(
        ["--repository", str(repository), "--version", "0.1.0", "--output", str(output)]
    )

    assert (exit_code, output.read_text(encoding="utf-8").splitlines()[0]) == (
        0,
        "# 🚀 Release 0.1.0",
    )


def test_the_command_fails_closed_when_there_is_nothing_to_release(tmp_path: Path) -> None:
    repository = _repository(tmp_path)
    _commit(repository, "feat(core): released already")
    _git(repository, "tag", "v1.9.3")
    output = tmp_path / "CHANGELOG_RELEASE.md"

    with pytest.raises(SystemExit) as failure:
        notes.main(["--repository", str(repository), "--version", "1.9.4", "--output", str(output)])

    assert (failure.value.code, output.exists()) == (1, False)
