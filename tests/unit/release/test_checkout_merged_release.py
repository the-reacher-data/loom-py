from __future__ import annotations

import inspect
import os
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from scripts.release import checkout_merged_release as release

EXPECTED_OWNER = "the-reacher-data"
EXPECTED_VERSION = "0.16.1"
RELEASE_BRANCH = f"docs/release-v{EXPECTED_VERSION}"
EXPECTED_FILES = ("CHANGELOG.md", "pyproject.toml", "uv.lock")
INITIAL_VERSION = "0.15.1"


@dataclass(frozen=True)
class RemoteRepository:
    checkout: Path
    base_sha: str
    head_sha: str
    merge_sha: str


class PullRequestReader:
    def __init__(
        self,
        snapshots: Sequence[Sequence[release.PullRequestSnapshot]],
        *,
        expected_branch: str = RELEASE_BRANCH,
    ) -> None:
        self._snapshots = snapshots
        self._expected_branch = expected_branch
        self.call_count = 0

    def __call__(self, release_branch: str) -> Sequence[release.PullRequestSnapshot]:
        if release_branch != self._expected_branch:
            raise AssertionError(f"unexpected release branch: {release_branch}")

        index = min(self.call_count, len(self._snapshots) - 1)
        self.call_count += 1
        return self._snapshots[index]


class MergeRecorder:
    def __init__(self, repository: RemoteRepository | None = None) -> None:
        self.calls: list[tuple[int, str]] = []
        self._repository = repository

    def __call__(self, pull_request_number: int, expected_head_sha: str) -> None:
        self.calls.append((pull_request_number, expected_head_sha))
        if self._repository is not None:
            _git(
                self._repository.checkout,
                "fetch",
                "origin",
                f"refs/heads/unmerged-v{EXPECTED_VERSION}",
            )
            _git(
                self._repository.checkout,
                "push",
                "origin",
                f"{self._repository.merge_sha}:refs/heads/master",
            )


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def _git(repository: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _metadata(
    *,
    project_version: str,
    commitizen_version: str,
    lock_version: str,
) -> tuple[str, str]:
    pyproject = (
        "[project]\n"
        'name = "loom-kernel"\n'
        f'version = "{project_version}"\n\n'
        "[tool.commitizen]\n"
        'name = "cz_conventional_commits"\n'
        f'version = "{commitizen_version}"\n'
    )
    lockfile = (
        "version = 1\n"
        "revision = 3\n"
        'requires-python = ">=3.11"\n\n'
        "[[package]]\n"
        'name = "loom-kernel"\n'
        f'version = "{lock_version}"\n'
        'source = { editable = "." }\n'
    )
    return pyproject, lockfile


def _write_metadata(
    repository: Path,
    *,
    project_version: str,
    commitizen_version: str,
    lock_version: str,
) -> None:
    pyproject, lockfile = _metadata(
        project_version=project_version,
        commitizen_version=commitizen_version,
        lock_version=lock_version,
    )
    (repository / "pyproject.toml").write_text(pyproject, encoding="utf-8")
    (repository / "uv.lock").write_text(lockfile, encoding="utf-8")


def _create_remote_repository(
    tmp_path: Path,
    *,
    expected_version: str = EXPECTED_VERSION,
    project_version: str | None = None,
    commitizen_version: str | None = None,
    lock_version: str | None = None,
    merge_on_master: bool = True,
) -> RemoteRepository:
    origin = tmp_path / "origin.git"
    author = tmp_path / "author"
    checkout = tmp_path / "checkout"

    origin.mkdir()
    author.mkdir()
    _git(origin, "init", "--bare")
    _git(author, "init", "--initial-branch=master")
    _git(author, "config", "user.name", "Release Test")
    _git(author, "config", "user.email", "release-test@example.com")
    _write_metadata(
        author,
        project_version=INITIAL_VERSION,
        commitizen_version=INITIAL_VERSION,
        lock_version=INITIAL_VERSION,
    )
    (author / "CHANGELOG.md").write_text("# Changelog\n", encoding="utf-8")
    (author / "tracked.txt").write_text("clean\n", encoding="utf-8")
    _git(author, "add", ".")
    _git(author, "commit", "-m", "initial release")
    base_sha = _git(author, "rev-parse", "HEAD")
    _git(author, "remote", "add", "origin", str(origin))
    _git(author, "push", "--set-upstream", "origin", "master")

    subprocess.run(
        ["git", "clone", str(origin), str(checkout)],
        check=True,
        capture_output=True,
        text=True,
    )

    release_branch = f"docs/release-v{expected_version}"
    _git(author, "checkout", "-b", release_branch)
    _write_metadata(
        author,
        project_version=project_version or expected_version,
        commitizen_version=commitizen_version or expected_version,
        lock_version=lock_version or expected_version,
    )
    (author / "CHANGELOG.md").write_text(
        f"# Release v{expected_version}\n\n# Changelog\n",
        encoding="utf-8",
    )
    _git(author, "add", "CHANGELOG.md", "pyproject.toml", "uv.lock")
    _git(author, "commit", "-m", f"chore(release): bump version to {expected_version}")
    head_sha = _git(author, "rev-parse", "HEAD")
    _git(author, "push", "--set-upstream", "origin", release_branch)

    _git(author, "checkout", "master")
    _git(author, "merge", "--squash", release_branch)
    _git(author, "commit", "-m", f"chore(release): squash version {expected_version}")
    merge_sha = _git(author, "rev-parse", "HEAD")
    if merge_on_master:
        _git(author, "push", "origin", "master")
    else:
        _git(author, "push", "origin", f"HEAD:refs/heads/unmerged-v{expected_version}")

    return RemoteRepository(
        checkout=checkout,
        base_sha=base_sha,
        head_sha=head_sha,
        merge_sha=merge_sha,
    )


def _snapshot(**overrides: Any) -> release.PullRequestSnapshot:
    values: dict[str, Any] = {
        "number": 89,
        "state": "MERGED",
        "base_ref": "master",
        "head_ref": RELEASE_BRANCH,
        "owner_login": EXPECTED_OWNER,
        "head_sha": "a" * 40,
        "files": EXPECTED_FILES,
        "merge_state_status": "CLEAN",
        "merge_sha": "b" * 40,
        "check_outcomes": (),
    }
    values.update(overrides)
    expected_fields = set(values)
    actual_fields = set(getattr(release.PullRequestSnapshot, "__dataclass_fields__", {}))
    assert expected_fields <= actual_fields, (
        f"PullRequestSnapshot is missing reviewed fields: {sorted(expected_fields - actual_fields)}"
    )
    return release.PullRequestSnapshot(**values)


def _reader(
    repository: RemoteRepository,
    *states: release.PullRequestSnapshot,
    release_branch: str = RELEASE_BRANCH,
) -> PullRequestReader:
    snapshots = states or (
        _snapshot(
            head_ref=release_branch,
            head_sha=repository.head_sha,
            merge_sha=repository.merge_sha,
        ),
    )
    return PullRequestReader(
        [(snapshot,) for snapshot in snapshots],
        expected_branch=release_branch,
    )


def _install_fake_uv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    exit_code: int = 0,
) -> Path:
    executable_directory = tmp_path / "fake-bin"
    executable_directory.mkdir(exist_ok=True)
    call_log = tmp_path / "uv-calls.log"
    executable = executable_directory / "uv"
    executable.write_text(
        f'#!/bin/sh\nprintf "%s\\n" "$*" >> "$UV_CALL_LOG"\nexit {exit_code}\n',
        encoding="utf-8",
    )
    executable.chmod(0o755)
    monkeypatch.setenv("UV_CALL_LOG", str(call_log))
    monkeypatch.setenv(
        "PATH",
        f"{executable_directory}{os.pathsep}{os.environ.get('PATH', '')}",
    )
    return call_log


def _checkout(
    repository: RemoteRepository,
    reader: PullRequestReader,
    merger: MergeRecorder,
    clock: FakeClock,
    *,
    expected_version: str = EXPECTED_VERSION,
    release_branch: str = RELEASE_BRANCH,
    timeout_seconds: float = 10.0,
) -> str:
    required_parameters = {
        "repository",
        "release_branch",
        "expected_version",
        "expected_owner",
        "get_pull_requests",
        "merge_pull_request",
        "timeout_seconds",
        "poll_interval_seconds",
        "monotonic",
        "sleep",
    }
    actual_parameters = set(inspect.signature(release.checkout_merged_release).parameters)
    assert required_parameters <= actual_parameters, (
        f"checkout_merged_release is missing reviewed parameters: "
        f"{sorted(required_parameters - actual_parameters)}"
    )
    return release.checkout_merged_release(
        repository=repository.checkout,
        release_branch=release_branch,
        expected_version=expected_version,
        expected_owner=EXPECTED_OWNER,
        get_pull_requests=reader,
        merge_pull_request=merger,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=1.0,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )


def test_regression_v0160_attempt_one_checks_out_preexisting_merged_pr(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    version = "0.16.0"
    branch = f"docs/release-v{version}"
    repository = _create_remote_repository(tmp_path, expected_version=version)
    reader = _reader(repository, release_branch=branch)
    _install_fake_uv(tmp_path, monkeypatch)

    resolved_sha = _checkout(
        repository,
        reader,
        MergeRecorder(),
        FakeClock(),
        expected_version=version,
        release_branch=branch,
    )

    assert (resolved_sha, _git(repository.checkout, "rev-parse", "HEAD")) == (
        repository.merge_sha,
        repository.merge_sha,
    )


def test_clean_pr_merges_immediately_by_number_and_expected_head_sha(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _create_remote_repository(tmp_path, merge_on_master=False)
    clean = _snapshot(
        state="OPEN",
        head_sha=repository.head_sha,
        merge_state_status="CLEAN",
        merge_sha=None,
    )
    merged = _snapshot(head_sha=repository.head_sha, merge_sha=repository.merge_sha)
    reader = _reader(repository, clean, merged)
    merger = MergeRecorder(repository)
    _install_fake_uv(tmp_path, monkeypatch)

    _checkout(repository, reader, merger, FakeClock())

    assert merger.calls == [(clean.number, repository.head_sha)]


def test_pending_pr_is_polled_until_clean_then_merged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _create_remote_repository(tmp_path, merge_on_master=False)
    pending = _snapshot(
        state="OPEN",
        head_sha=repository.head_sha,
        merge_state_status="UNKNOWN",
        merge_sha=None,
    )
    clean = _snapshot(
        state="OPEN",
        head_sha=repository.head_sha,
        merge_state_status="CLEAN",
        merge_sha=None,
    )
    merged = _snapshot(head_sha=repository.head_sha, merge_sha=repository.merge_sha)
    reader = _reader(repository, pending, clean, merged)
    merger = MergeRecorder(repository)
    _install_fake_uv(tmp_path, monkeypatch)

    resolved_sha = _checkout(repository, reader, merger, FakeClock())

    assert (resolved_sha, reader.call_count, merger.calls) == (
        repository.merge_sha,
        3,
        [(clean.number, repository.head_sha)],
    )


def test_unstable_pr_is_merged_once_every_reported_check_passed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _create_remote_repository(tmp_path, merge_on_master=False)
    unstable = _snapshot(
        state="OPEN",
        head_sha=repository.head_sha,
        merge_state_status="UNSTABLE",
        merge_sha=None,
        check_outcomes=(release.CheckOutcome.PASSED, release.CheckOutcome.PASSED),
    )
    merged = _snapshot(head_sha=repository.head_sha, merge_sha=repository.merge_sha)
    merger = MergeRecorder(repository)
    _install_fake_uv(tmp_path, monkeypatch)

    resolved_sha = _checkout(repository, _reader(repository, unstable, merged), merger, FakeClock())

    assert (resolved_sha, merger.calls) == (
        repository.merge_sha,
        [(unstable.number, repository.head_sha)],
    )


@pytest.mark.parametrize(
    "check_outcomes",
    [
        pytest.param((), id="no-check-reported"),
        pytest.param(
            (release.CheckOutcome.PASSED, release.CheckOutcome.PENDING),
            id="check-still-running",
        ),
        pytest.param(
            (release.CheckOutcome.PASSED, release.CheckOutcome.FAILED),
            id="check-failed",
        ),
    ],
)
def test_unstable_pr_is_never_merged_unless_all_checks_passed(
    tmp_path: Path,
    check_outcomes: tuple[release.CheckOutcome, ...],
) -> None:
    repository = _create_remote_repository(tmp_path)
    unstable = _snapshot(
        state="OPEN",
        head_sha=repository.head_sha,
        merge_state_status="UNSTABLE",
        merge_sha=None,
        check_outcomes=check_outcomes,
    )
    merger = MergeRecorder()

    with pytest.raises(release.ReleaseCheckoutError, match="timed out"):
        _checkout(
            repository,
            _reader(repository, unstable),
            merger,
            FakeClock(),
            timeout_seconds=2.0,
        )

    assert merger.calls == []


@pytest.mark.parametrize("candidate_count", [0, 2])
def test_fails_closed_unless_exactly_one_release_pr_candidate_exists(
    tmp_path: Path,
    candidate_count: int,
) -> None:
    repository = _create_remote_repository(tmp_path, merge_on_master=False)
    candidate = _snapshot(head_sha=repository.head_sha, merge_sha=repository.merge_sha)
    reader = PullRequestReader([(candidate,) * candidate_count])

    with pytest.raises(release.ReleaseCheckoutError, match="exactly one"):
        _checkout(repository, reader, MergeRecorder(), FakeClock())


def test_fails_closed_when_release_pr_is_closed(tmp_path: Path) -> None:
    repository = _create_remote_repository(tmp_path, merge_on_master=False)
    closed = _snapshot(
        state="CLOSED",
        head_sha=repository.head_sha,
        merge_state_status="UNKNOWN",
        merge_sha=None,
    )

    with pytest.raises(release.ReleaseCheckoutError, match="closed"):
        _checkout(repository, _reader(repository, closed), MergeRecorder(), FakeClock())


def test_fails_closed_when_release_pr_has_conflicts(tmp_path: Path) -> None:
    repository = _create_remote_repository(tmp_path)
    conflicting = _snapshot(
        state="OPEN",
        head_sha=repository.head_sha,
        merge_state_status="DIRTY",
        merge_sha=None,
    )

    with pytest.raises(release.ReleaseCheckoutError, match="conflict|DIRTY"):
        _checkout(repository, _reader(repository, conflicting), MergeRecorder(), FakeClock())


def test_fails_closed_when_release_pr_base_is_behind(tmp_path: Path) -> None:
    repository = _create_remote_repository(tmp_path)
    behind = _snapshot(
        state="OPEN",
        head_sha=repository.head_sha,
        merge_state_status="BEHIND",
        merge_sha=None,
    )

    with pytest.raises(release.ReleaseCheckoutError, match="base advanced"):
        _checkout(repository, _reader(repository, behind), MergeRecorder(), FakeClock())


def test_fails_before_merge_when_origin_master_advanced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _create_remote_repository(tmp_path, merge_on_master=False)
    _git(repository.checkout, "config", "user.name", "Concurrent Merge")
    _git(repository.checkout, "config", "user.email", "merge@example.com")
    (repository.checkout / "tracked.txt").write_text("advanced\n", encoding="utf-8")
    _git(repository.checkout, "add", "tracked.txt")
    _git(repository.checkout, "commit", "-m", "concurrent master change")
    _git(repository.checkout, "push", "origin", "master")
    clean = _snapshot(
        state="OPEN",
        head_sha=repository.head_sha,
        merge_state_status="CLEAN",
        merge_sha=None,
    )
    merger = MergeRecorder()
    _install_fake_uv(tmp_path, monkeypatch)

    with pytest.raises(release.ReleaseCheckoutError, match="origin/master advanced"):
        _checkout(repository, _reader(repository, clean), merger, FakeClock())

    assert merger.calls == []


def test_fails_closed_when_pending_pr_times_out(tmp_path: Path) -> None:
    repository = _create_remote_repository(tmp_path)
    pending = _snapshot(
        state="OPEN",
        head_sha=repository.head_sha,
        merge_state_status="UNKNOWN",
        merge_sha=None,
    )

    with pytest.raises(release.ReleaseCheckoutError, match="timed out"):
        _checkout(
            repository,
            _reader(repository, pending),
            MergeRecorder(),
            FakeClock(),
            timeout_seconds=2.0,
        )


def test_fails_closed_when_pr_head_mutates_during_polling(tmp_path: Path) -> None:
    repository = _create_remote_repository(tmp_path)
    pending = _snapshot(
        state="OPEN",
        head_sha=repository.head_sha,
        merge_state_status="UNKNOWN",
        merge_sha=None,
    )
    mutated = _snapshot(
        state="OPEN",
        head_sha="c" * 40,
        merge_state_status="CLEAN",
        merge_sha=None,
    )

    with pytest.raises(release.ReleaseCheckoutError, match="head.*changed|mutat"):
        _checkout(
            repository,
            _reader(repository, pending, mutated),
            MergeRecorder(),
            FakeClock(),
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("base_ref", "develop", "base"),
        ("head_ref", "docs/release-v9.9.9", "head"),
        ("owner_login", "untrusted-fork", "owner"),
        ("files", ("pyproject.toml", "scripts/backdoor.py"), "files"),
    ],
)
def test_fails_closed_when_pr_identity_or_files_are_unexpected(
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
) -> None:
    repository = _create_remote_repository(tmp_path)
    candidate = _snapshot(
        head_sha=repository.head_sha,
        merge_sha=repository.merge_sha,
        **{field: value},
    )

    with pytest.raises(release.ReleaseCheckoutError, match=message):
        _checkout(repository, _reader(repository, candidate), MergeRecorder(), FakeClock())


def test_fails_closed_when_release_pr_omits_a_required_file(tmp_path: Path) -> None:
    repository = _create_remote_repository(tmp_path)
    candidate = _snapshot(
        head_sha=repository.head_sha,
        merge_sha=repository.merge_sha,
        files=("CHANGELOG.md", "pyproject.toml"),
    )

    with pytest.raises(release.ReleaseCheckoutError, match="files"):
        _checkout(repository, _reader(repository, candidate), MergeRecorder(), FakeClock())


def test_validates_open_pr_metadata_before_requesting_merge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _create_remote_repository(
        tmp_path,
        project_version="9.9.9",
        merge_on_master=False,
    )
    clean = _snapshot(
        state="OPEN",
        head_sha=repository.head_sha,
        merge_state_status="CLEAN",
        merge_sha=None,
    )
    merged = _snapshot(head_sha=repository.head_sha, merge_sha=repository.merge_sha)
    merger = MergeRecorder()
    _install_fake_uv(tmp_path, monkeypatch)

    with pytest.raises(release.ReleaseCheckoutError, match="project"):
        _checkout(
            repository,
            _reader(repository, clean, merged),
            merger,
            FakeClock(),
        )

    assert merger.calls == []


@pytest.mark.parametrize(
    ("state", "field"),
    [
        ("OPEN", "head_sha"),
        ("MERGED", "merge_sha"),
    ],
)
def test_fails_closed_when_pr_contains_an_invalid_sha(
    tmp_path: Path,
    state: str,
    field: str,
) -> None:
    repository = _create_remote_repository(tmp_path)
    values: dict[str, object] = {
        "state": state,
        "head_sha": repository.head_sha,
        "merge_sha": repository.merge_sha,
    }
    values[field] = "not-a-git-sha"
    if state == "OPEN":
        values.update(merge_state_status="CLEAN", merge_sha=None)
    candidate = _snapshot(**values)

    with pytest.raises(release.ReleaseCheckoutError, match="SHA"):
        _checkout(repository, _reader(repository, candidate), MergeRecorder(), FakeClock())


@pytest.mark.parametrize(
    ("version_field", "message"),
    [
        ("project_version", "project"),
        ("commitizen_version", "Commitizen|commitizen"),
        ("lock_version", "uv.lock|lock"),
    ],
)
def test_fails_closed_when_release_metadata_versions_differ(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    version_field: str,
    message: str,
) -> None:
    versions: dict[str, Any] = {version_field: "9.9.9"}
    repository = _create_remote_repository(tmp_path, **versions)
    _install_fake_uv(tmp_path, monkeypatch)

    with pytest.raises(release.ReleaseCheckoutError, match=message):
        _checkout(repository, _reader(repository), MergeRecorder(), FakeClock())


def test_fails_closed_when_merge_sha_is_not_on_origin_master(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _create_remote_repository(tmp_path, merge_on_master=False)
    _install_fake_uv(tmp_path, monkeypatch)

    with pytest.raises(release.ReleaseCheckoutError, match="origin/master"):
        _checkout(repository, _reader(repository), MergeRecorder(), FakeClock())


def test_allows_only_changelog_release_as_untracked_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _create_remote_repository(tmp_path)
    (repository.checkout / "CHANGELOG_RELEASE.md").write_text(
        "# Generated release notes\n",
        encoding="utf-8",
    )
    _install_fake_uv(tmp_path, monkeypatch)

    resolved_sha = _checkout(repository, _reader(repository), MergeRecorder(), FakeClock())

    assert resolved_sha == repository.merge_sha


def test_fails_closed_when_an_unexpected_untracked_file_exists(tmp_path: Path) -> None:
    repository = _create_remote_repository(tmp_path)
    (repository.checkout / "unexpected.txt").write_text("unsafe\n", encoding="utf-8")

    with pytest.raises(release.ReleaseCheckoutError, match="untracked"):
        _checkout(repository, _reader(repository), MergeRecorder(), FakeClock())


def test_fails_closed_when_checkout_has_dirty_tracked_files(tmp_path: Path) -> None:
    repository = _create_remote_repository(tmp_path)
    (repository.checkout / "tracked.txt").write_text("dirty\n", encoding="utf-8")

    with pytest.raises(release.ReleaseCheckoutError, match="dirty"):
        _checkout(repository, _reader(repository), MergeRecorder(), FakeClock())


def test_github_merge_adapter_uses_number_and_match_head_without_auto(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[list[str]] = []

    def run(
        command: Sequence[str],
        *,
        check: bool,
        capture_output: bool,
        text: bool,
    ) -> subprocess.CompletedProcess[str]:
        assert (check, capture_output, text) == (True, True, True)
        captured.append(list(command))
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(release.subprocess, "run", run)
    merge_adapter = getattr(release, "_merge_pull_request", None)
    assert callable(merge_adapter), (
        "helper must expose its GitHub merge adapter for contract testing"
    )

    merge_adapter(89, "a" * 40)

    assert captured == [
        [
            "gh",
            "pr",
            "merge",
            "89",
            "--squash",
            "--match-head-commit",
            "a" * 40,
        ]
    ]


def test_github_reader_adapter_requests_the_status_check_rollup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[list[str]] = []

    def run(
        command: Sequence[str],
        *,
        check: bool,
        capture_output: bool,
        text: bool,
    ) -> subprocess.CompletedProcess[str]:
        assert (check, capture_output, text) == (True, True, True)
        captured.append(list(command))
        return subprocess.CompletedProcess(command, 0, "[]", "")

    monkeypatch.setattr(release.subprocess, "run", run)

    snapshots = release._read_pull_requests(RELEASE_BRANCH)

    assert snapshots == ()
    assert "statusCheckRollup" in captured[0][-1]


def test_github_payload_adapter_parses_complete_release_pr_identity() -> None:
    payload = {
        "number": 89,
        "state": "OPEN",
        "baseRefName": "master",
        "headRefName": RELEASE_BRANCH,
        "headRepositoryOwner": {"login": EXPECTED_OWNER},
        "headRefOid": "a" * 40,
        "changedFiles": 3,
        "files": [{"path": path} for path in EXPECTED_FILES],
        "mergeStateStatus": "CLEAN",
        "mergeCommit": None,
    }

    snapshot = release._snapshot_from_payload(payload)

    assert snapshot == _snapshot(state="OPEN", merge_sha=None)


def test_github_payload_adapter_normalises_every_reported_status_check() -> None:
    payload = {
        "number": 89,
        "state": "OPEN",
        "baseRefName": "master",
        "headRefName": RELEASE_BRANCH,
        "headRepositoryOwner": {"login": EXPECTED_OWNER},
        "headRefOid": "a" * 40,
        "changedFiles": 3,
        "files": [{"path": path} for path in EXPECTED_FILES],
        "mergeStateStatus": "UNSTABLE",
        "mergeCommit": None,
        "statusCheckRollup": [
            {"__typename": "CheckRun", "status": "COMPLETED", "conclusion": "SUCCESS"},
            {"__typename": "CheckRun", "status": "COMPLETED", "conclusion": "SKIPPED"},
            {"__typename": "CheckRun", "status": "IN_PROGRESS", "conclusion": ""},
            {"__typename": "CheckRun", "status": "COMPLETED", "conclusion": "FAILURE"},
            {"__typename": "StatusContext", "context": "codecov/patch", "state": "SUCCESS"},
            {"__typename": "StatusContext", "context": "codecov/project", "state": "PENDING"},
            {"__typename": "StatusContext", "context": "legacy/gate", "state": "ERROR"},
        ],
    }

    snapshot = release._snapshot_from_payload(payload)

    assert snapshot.check_outcomes == (
        release.CheckOutcome.PASSED,
        release.CheckOutcome.PASSED,
        release.CheckOutcome.PENDING,
        release.CheckOutcome.FAILED,
        release.CheckOutcome.PASSED,
        release.CheckOutcome.PENDING,
        release.CheckOutcome.FAILED,
    )


def test_github_payload_adapter_rejects_an_unknown_status_check_type() -> None:
    payload = {
        "number": 89,
        "state": "OPEN",
        "baseRefName": "master",
        "headRefName": RELEASE_BRANCH,
        "headRepositoryOwner": {"login": EXPECTED_OWNER},
        "headRefOid": "a" * 40,
        "changedFiles": 3,
        "files": [{"path": path} for path in EXPECTED_FILES],
        "mergeStateStatus": "UNSTABLE",
        "mergeCommit": None,
        "statusCheckRollup": [{"__typename": "DeploymentStatus", "state": "SUCCESS"}],
    }

    with pytest.raises(release.ReleaseCheckoutError, match="unknown status check type"):
        release._snapshot_from_payload(payload)


def test_github_payload_adapter_rejects_truncated_file_list() -> None:
    payload = {
        "number": 89,
        "state": "OPEN",
        "baseRefName": "master",
        "headRefName": RELEASE_BRANCH,
        "headRepositoryOwner": {"login": EXPECTED_OWNER},
        "headRefOid": "a" * 40,
        "changedFiles": 3,
        "files": [{"path": "pyproject.toml"}],
        "mergeStateStatus": "CLEAN",
        "mergeCommit": None,
    }

    with pytest.raises(release.ReleaseCheckoutError, match="incomplete.*file"):
        release._snapshot_from_payload(payload)


def test_github_payload_adapter_rejects_incomplete_owner_identity() -> None:
    payload = {
        "number": 89,
        "state": "OPEN",
        "baseRefName": "master",
        "headRefName": RELEASE_BRANCH,
        "headRepositoryOwner": {},
        "headRefOid": "a" * 40,
        "changedFiles": 3,
        "files": [{"path": path} for path in EXPECTED_FILES],
        "mergeStateStatus": "CLEAN",
        "mergeCommit": None,
    }

    with pytest.raises(release.ReleaseCheckoutError, match="owner"):
        release._snapshot_from_payload(payload)
