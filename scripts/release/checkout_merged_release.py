"""Resolve, merge, and validate the commit used to publish a release."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import tomllib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, NoReturn

_FULL_GIT_SHA = re.compile(r"^[0-9a-fA-F]{40}$")
_EXPECTED_BASE_REF = "master"
_ALLOWED_RELEASE_FILES = frozenset({"CHANGELOG.md", "pyproject.toml", "uv.lock"})
_ALLOWED_UNTRACKED_FILES = frozenset({"CHANGELOG_RELEASE.md"})
_PENDING_MERGE_STATES = frozenset({"BLOCKED", "HAS_HOOKS", "UNKNOWN", "UNSTABLE"})
_CLOSED_STATE_ERRORS = {"CLOSED": "release PR closed without being merged"}
_BLOCKED_MERGE_STATE_ERRORS = {
    "DIRTY": "release PR has conflicts (merge state DIRTY)",
    "BEHIND": "release PR base advanced after the release snapshot",
    "DRAFT": "release PR is a draft and cannot be merged",
}
_PASSED_CHECK_CONCLUSIONS = frozenset({"SUCCESS", "NEUTRAL", "SKIPPED"})
_PENDING_STATUS_STATES = frozenset({"PENDING", "EXPECTED"})
_COMPLETED_CHECK_STATUS = "COMPLETED"


class ReleaseCheckoutError(RuntimeError):
    """Raised when a release commit cannot be resolved and validated safely."""


class CheckOutcome(StrEnum):
    """Normalised outcome of a single status check reported on a commit."""

    PASSED = "PASSED"
    PENDING = "PENDING"
    FAILED = "FAILED"


@dataclass(frozen=True)
class PullRequestSnapshot:
    """Complete remote identity and merge state for a release pull request."""

    number: int
    state: str
    base_ref: str
    head_ref: str
    owner_login: str
    head_sha: str
    files: tuple[str, ...]
    merge_state_status: str
    merge_sha: str | None
    check_outcomes: tuple[CheckOutcome, ...] = ()


PullRequestReader = Callable[[str], Sequence[PullRequestSnapshot]]
PullRequestMerger = Callable[[int, str], None]
PullRequestHeadValidator = Callable[[str], str]
MonotonicClock = Callable[[], float]
Sleeper = Callable[[float], None]


def _command_error(
    command: Sequence[str],
    error: OSError | subprocess.CalledProcessError | json.JSONDecodeError,
) -> str:
    if isinstance(error, subprocess.CalledProcessError):
        detail = (error.stderr or "").strip() or (error.stdout or "").strip()
    else:
        detail = str(error)
    return f"{' '.join(command)} failed: {detail or 'unknown error'}"


def _run_git(repository: Path, *arguments: str) -> str:
    command = ["git", *arguments]
    try:
        completed = subprocess.run(
            command,
            cwd=repository,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise ReleaseCheckoutError(_command_error(command, error)) from error
    return completed.stdout.strip()


def _ensure_checkout_is_clean(repository: Path) -> None:
    status = _run_git(
        repository,
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=all",
    )
    unexpected_untracked: list[str] = []
    for entry in status.split("\0"):
        if not entry:
            continue
        status_code, path = entry[:2], entry[3:]
        if status_code != "??":
            raise ReleaseCheckoutError("tracked checkout is dirty")
        if path not in _ALLOWED_UNTRACKED_FILES:
            unexpected_untracked.append(path)

    if unexpected_untracked:
        raise ReleaseCheckoutError(
            f"unexpected untracked files: {', '.join(sorted(unexpected_untracked))}"
        )


def _normalise_sha(value: str | None, *, label: str) -> str:
    if value is None or not _FULL_GIT_SHA.fullmatch(value.strip()):
        raise ReleaseCheckoutError(f"{label} is not a valid full Git SHA")
    return value.strip().lower()


def _validate_snapshot(
    snapshot: PullRequestSnapshot,
    *,
    release_branch: str,
    expected_owner: str,
    expected_head_sha: str | None,
) -> str:
    if snapshot.base_ref != _EXPECTED_BASE_REF:
        raise ReleaseCheckoutError(
            f"release PR base {snapshot.base_ref!r} is not {_EXPECTED_BASE_REF!r}"
        )
    if snapshot.head_ref != release_branch:
        raise ReleaseCheckoutError(
            f"release PR head {snapshot.head_ref!r} is not {release_branch!r}"
        )
    if snapshot.owner_login != expected_owner:
        raise ReleaseCheckoutError(
            f"release PR owner {snapshot.owner_login!r} is not {expected_owner!r}"
        )

    changed_files = frozenset(snapshot.files)
    if changed_files != _ALLOWED_RELEASE_FILES:
        missing_files = _ALLOWED_RELEASE_FILES - changed_files
        unexpected_files = changed_files - _ALLOWED_RELEASE_FILES
        details = []
        if missing_files:
            details.append(f"missing {', '.join(sorted(missing_files))}")
        if unexpected_files:
            details.append(f"unexpected {', '.join(sorted(unexpected_files))}")
        raise ReleaseCheckoutError(
            f"release PR files do not match the required set: {'; '.join(details)}"
        )

    head_sha = _normalise_sha(snapshot.head_sha, label="release PR head SHA")
    if expected_head_sha is not None and head_sha != expected_head_sha:
        raise ReleaseCheckoutError(
            f"release PR head changed from {expected_head_sha} to {head_sha}"
        )
    return head_sha


def _read_one_validated_snapshot(
    release_branch: str,
    expected_owner: str,
    expected_head_sha: str | None,
    get_pull_requests: PullRequestReader,
) -> tuple[PullRequestSnapshot, str]:
    snapshots = get_pull_requests(release_branch)
    if len(snapshots) != 1:
        raise ReleaseCheckoutError(
            f"expected exactly one release PR candidate, found {len(snapshots)}"
        )

    snapshot = snapshots[0]
    head_sha = _validate_snapshot(
        snapshot,
        release_branch=release_branch,
        expected_owner=expected_owner,
        expected_head_sha=expected_head_sha,
    )
    return snapshot, head_sha


def _is_ready_to_merge(merge_state: str, check_outcomes: Sequence[CheckOutcome]) -> bool:
    if merge_state == "CLEAN":
        return True
    if merge_state != "UNSTABLE":
        return False
    # GitHub reports UNSTABLE while the head commit has no successful legacy
    # commit status. That is the permanent state of a release PR: Codecov only
    # reports through the Checks API, and no workflow runs on a branch pushed by
    # GITHUB_TOKEN, so no coverage is ever uploaded for it. Merging is still
    # safe as long as every check that did report has passed.
    return bool(check_outcomes) and all(
        outcome is CheckOutcome.PASSED for outcome in check_outcomes
    )


def _validate_polling_bounds(timeout_seconds: float, poll_interval_seconds: float) -> None:
    if timeout_seconds < 0:
        raise ReleaseCheckoutError("merge timeout must not be negative")
    if poll_interval_seconds <= 0:
        raise ReleaseCheckoutError("poll interval must be greater than zero")


def _require_open_pull_request(snapshot: PullRequestSnapshot) -> None:
    """Refuse any PR state other than OPEN — MERGED is handled by the caller."""
    state = snapshot.state.upper()
    if state == "OPEN":
        return
    message = _CLOSED_STATE_ERRORS.get(state)
    if message is not None:
        raise ReleaseCheckoutError(message)
    raise ReleaseCheckoutError(f"release PR has unexpected state {snapshot.state!r}")


def _require_mergeable_state(snapshot: PullRequestSnapshot) -> None:
    """Refuse merge states that can never become mergeable on their own."""
    message = _BLOCKED_MERGE_STATE_ERRORS.get(snapshot.merge_state_status.upper())
    if message is not None:
        raise ReleaseCheckoutError(message)


def _wait_for_merge(
    release_branch: str,
    expected_owner: str,
    get_pull_requests: PullRequestReader,
    merge_pull_request: PullRequestMerger,
    validate_pull_request_head: PullRequestHeadValidator,
    *,
    timeout_seconds: float,
    poll_interval_seconds: float,
    monotonic: MonotonicClock,
    sleep: Sleeper,
) -> tuple[str, str | None]:
    _validate_polling_bounds(timeout_seconds, poll_interval_seconds)

    deadline = monotonic() + timeout_seconds
    expected_head_sha: str | None = None
    expected_base_sha: str | None = None
    merge_requested = False

    while True:
        snapshot, head_sha = _read_one_validated_snapshot(
            release_branch,
            expected_owner,
            expected_head_sha,
            get_pull_requests,
        )
        expected_head_sha = head_sha
        merge_state = snapshot.merge_state_status.upper()

        if snapshot.state.upper() == "MERGED":
            return (
                _normalise_sha(snapshot.merge_sha, label="release PR merge SHA"),
                expected_base_sha,
            )
        _require_open_pull_request(snapshot)
        _require_mergeable_state(snapshot)

        if not merge_requested and _is_ready_to_merge(merge_state, snapshot.check_outcomes):
            expected_base_sha = validate_pull_request_head(head_sha)
            merge_pull_request(snapshot.number, head_sha)
            merge_requested = True
        elif merge_state not in _PENDING_MERGE_STATES and merge_state != "CLEAN":
            raise ReleaseCheckoutError(
                f"release PR has unexpected merge state {snapshot.merge_state_status!r}"
            )

        now = monotonic()
        if now >= deadline:
            raise ReleaseCheckoutError("timed out waiting for release PR to merge")
        sleep(min(poll_interval_seconds, deadline - now))


def _read_release_metadata(repository: Path) -> tuple[str, str, str]:
    pyproject_path = repository / "pyproject.toml"
    lock_path = repository / "uv.lock"
    try:
        with pyproject_path.open("rb") as pyproject_file:
            pyproject = tomllib.load(pyproject_file)
        with lock_path.open("rb") as lock_file:
            lockfile = tomllib.load(lock_file)

        project = pyproject["project"]
        project_name = project["name"]
        project_version = project["version"]
        commitizen_version = pyproject["tool"]["commitizen"]["version"]
        packages = lockfile["package"]
    except (OSError, KeyError, TypeError, tomllib.TOMLDecodeError) as error:
        raise ReleaseCheckoutError("cannot read release metadata") from error

    if not all(
        isinstance(value, str) for value in (project_name, project_version, commitizen_version)
    ):
        raise ReleaseCheckoutError("project and Commitizen versions must be strings")
    if not isinstance(packages, list):
        raise ReleaseCheckoutError("uv.lock package metadata is invalid")

    root_versions = [
        package.get("version")
        for package in packages
        if isinstance(package, Mapping)
        and package.get("name") == project_name
        and package.get("source") == {"editable": "."}
    ]
    if len(root_versions) != 1 or not isinstance(root_versions[0], str):
        raise ReleaseCheckoutError("uv.lock must contain exactly one editable root package")

    return project_version, commitizen_version, root_versions[0]


def _validate_release_metadata(repository: Path, expected_version: str) -> None:
    project_version, commitizen_version, lock_version = _read_release_metadata(repository)
    if project_version != expected_version:
        raise ReleaseCheckoutError(
            f"project version {project_version!r} differs from expected version "
            f"{expected_version!r}"
        )
    if commitizen_version != expected_version:
        raise ReleaseCheckoutError(
            f"Commitizen version {commitizen_version!r} differs from expected version "
            f"{expected_version!r}"
        )
    if lock_version != expected_version:
        raise ReleaseCheckoutError(
            f"uv.lock root version {lock_version!r} differs from expected version "
            f"{expected_version!r}"
        )


def _validate_checked_out_release(repository: Path, expected_version: str) -> None:
    _ensure_checkout_is_clean(repository)
    _validate_release_metadata(repository, expected_version)
    _ensure_checkout_is_clean(repository)


def _fetch_and_validate_release_base(
    repository: Path,
    expected_base_sha: str,
) -> None:
    _run_git(
        repository,
        "fetch",
        "--no-tags",
        "origin",
        "+refs/heads/master:refs/remotes/origin/master",
    )
    remote_base_sha = _normalise_sha(
        _run_git(repository, "rev-parse", "origin/master"),
        label="origin/master SHA",
    )
    if remote_base_sha != expected_base_sha:
        raise ReleaseCheckoutError(
            f"origin/master advanced from release base {expected_base_sha} to {remote_base_sha}"
        )


def _checkout_and_validate_release_head(
    repository: Path,
    release_branch: str,
    expected_head_sha: str,
    expected_version: str,
) -> str:
    remote_ref = "refs/remotes/origin/release-validation"
    _run_git(
        repository,
        "fetch",
        "--no-tags",
        "origin",
        f"+refs/heads/{release_branch}:{remote_ref}",
    )
    fetched_head_sha = _normalise_sha(
        _run_git(repository, "rev-parse", remote_ref),
        label="fetched release PR head SHA",
    )
    if fetched_head_sha != expected_head_sha:
        raise ReleaseCheckoutError(
            f"fetched release PR head {fetched_head_sha} differs from "
            f"expected head {expected_head_sha}"
        )

    expected_base_sha = _normalise_sha(
        _run_git(repository, "rev-parse", f"{fetched_head_sha}^"),
        label="release PR base SHA",
    )
    _fetch_and_validate_release_base(repository, expected_base_sha)
    _run_git(repository, "checkout", "--detach", fetched_head_sha)
    _validate_checked_out_release(repository, expected_version)
    return expected_base_sha


def checkout_merged_release(
    *,
    repository: Path,
    release_branch: str,
    expected_version: str,
    expected_owner: str,
    get_pull_requests: PullRequestReader,
    merge_pull_request: PullRequestMerger,
    timeout_seconds: float,
    poll_interval_seconds: float,
    monotonic: MonotonicClock = time.monotonic,
    sleep: Sleeper = time.sleep,
) -> str:
    """Merge if needed, then check out and validate the remote merge commit.

    The pull request is merged when GitHub reports it as CLEAN, or as UNSTABLE
    with every reported status check passing. A release PR never reaches CLEAN
    because its head commit carries no legacy commit status, so requiring CLEAN
    alone would always time out.
    """

    _ensure_checkout_is_clean(repository)

    def validate_pull_request_head(expected_head_sha: str) -> str:
        return _checkout_and_validate_release_head(
            repository,
            release_branch,
            expected_head_sha,
            expected_version,
        )

    merge_sha, expected_base_sha = _wait_for_merge(
        release_branch,
        expected_owner,
        get_pull_requests,
        merge_pull_request,
        validate_pull_request_head,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        monotonic=monotonic,
        sleep=sleep,
    )

    _run_git(
        repository,
        "fetch",
        "--no-tags",
        "origin",
        "+refs/heads/master:refs/remotes/origin/master",
    )
    try:
        _run_git(repository, "merge-base", "--is-ancestor", merge_sha, "origin/master")
    except ReleaseCheckoutError as error:
        raise ReleaseCheckoutError(
            f"release merge SHA {merge_sha} is not an ancestor of origin/master"
        ) from error

    if expected_base_sha is not None:
        merge_parent_sha = _normalise_sha(
            _run_git(repository, "rev-parse", f"{merge_sha}^"),
            label="release merge parent SHA",
        )
        if merge_parent_sha != expected_base_sha:
            raise ReleaseCheckoutError(
                f"release merge parent {merge_parent_sha} differs from "
                f"release base {expected_base_sha}"
            )

    _run_git(repository, "checkout", "--detach", merge_sha)
    checked_out_sha = _normalise_sha(
        _run_git(repository, "rev-parse", "HEAD"),
        label="checked out SHA",
    )
    if checked_out_sha != merge_sha:
        raise ReleaseCheckoutError(
            f"checked out SHA {checked_out_sha} differs from merge SHA {merge_sha}"
        )

    _validate_checked_out_release(repository, expected_version)
    return merge_sha


def _extract_string(payload: Mapping[Any, Any], key: str, *, context: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise ReleaseCheckoutError(f"GitHub returned invalid {context}")
    return value


def _status_context_outcome(payload: Mapping[Any, Any]) -> CheckOutcome:
    state = _extract_string(payload, "state", context="status check state").upper()
    if state == "SUCCESS":
        return CheckOutcome.PASSED
    if state in _PENDING_STATUS_STATES:
        return CheckOutcome.PENDING
    return CheckOutcome.FAILED


def _check_run_outcome(payload: Mapping[Any, Any]) -> CheckOutcome:
    status = _extract_string(payload, "status", context="check run status").upper()
    if status != _COMPLETED_CHECK_STATUS:
        return CheckOutcome.PENDING
    conclusion = _extract_string(payload, "conclusion", context="check run conclusion").upper()
    if conclusion in _PASSED_CHECK_CONCLUSIONS:
        return CheckOutcome.PASSED
    return CheckOutcome.FAILED


def _check_outcome_from_payload(payload: object) -> CheckOutcome:
    if not isinstance(payload, Mapping):
        raise ReleaseCheckoutError("GitHub returned an invalid status check")
    kind = _extract_string(payload, "__typename", context="status check type")
    if kind == "StatusContext":
        return _status_context_outcome(payload)
    if kind == "CheckRun":
        return _check_run_outcome(payload)
    raise ReleaseCheckoutError(f"GitHub returned an unknown status check type {kind!r}")


def _check_outcomes_from_payload(payload: object) -> tuple[CheckOutcome, ...]:
    if payload is None:
        return ()
    if not isinstance(payload, list):
        raise ReleaseCheckoutError("GitHub returned an invalid status check list")
    return tuple(_check_outcome_from_payload(entry) for entry in payload)


def _snapshot_from_payload(payload: object) -> PullRequestSnapshot:
    if not isinstance(payload, Mapping):
        raise ReleaseCheckoutError("GitHub returned an invalid release PR")

    number = payload.get("number")
    owner = payload.get("headRepositoryOwner")
    files = payload.get("files")
    changed_file_count = payload.get("changedFiles")
    merge_commit = payload.get("mergeCommit")
    if (
        type(number) is not int
        or not isinstance(owner, Mapping)
        or not isinstance(files, list)
        or type(changed_file_count) is not int
    ):
        raise ReleaseCheckoutError("GitHub returned incomplete release PR identity")

    file_paths: list[str] = []
    for file_payload in files:
        if not isinstance(file_payload, Mapping):
            raise ReleaseCheckoutError("GitHub returned invalid release PR files")
        file_paths.append(_extract_string(file_payload, "path", context="release PR file"))
    if changed_file_count != len(file_paths):
        raise ReleaseCheckoutError("GitHub returned an incomplete release PR file list")

    if merge_commit is None:
        merge_sha = None
    elif isinstance(merge_commit, Mapping):
        merge_sha = _extract_string(merge_commit, "oid", context="merge commit")
    else:
        raise ReleaseCheckoutError("GitHub returned an invalid merge commit")

    return PullRequestSnapshot(
        number=number,
        state=_extract_string(payload, "state", context="release PR state"),
        base_ref=_extract_string(payload, "baseRefName", context="release PR base"),
        head_ref=_extract_string(payload, "headRefName", context="release PR head"),
        owner_login=_extract_string(owner, "login", context="release PR owner"),
        head_sha=_extract_string(payload, "headRefOid", context="release PR head SHA"),
        files=tuple(file_paths),
        merge_state_status=_extract_string(
            payload,
            "mergeStateStatus",
            context="release PR merge state",
        ),
        merge_sha=merge_sha,
        check_outcomes=_check_outcomes_from_payload(payload.get("statusCheckRollup")),
    )


def _read_pull_requests(release_branch: str) -> Sequence[PullRequestSnapshot]:
    command = [
        "gh",
        "pr",
        "list",
        "--head",
        release_branch,
        "--state",
        "all",
        "--json",
        (
            "number,state,baseRefName,headRefName,headRepositoryOwner,headRefOid,changedFiles,"
            "files,mergeStateStatus,mergeCommit,statusCheckRollup"
        ),
    ]
    try:
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )
        payload = json.loads(completed.stdout)
    except (OSError, subprocess.CalledProcessError, json.JSONDecodeError) as error:
        raise ReleaseCheckoutError(
            f"cannot list release PRs for {release_branch!r}: {_command_error(command, error)}"
        ) from error

    if not isinstance(payload, list):
        raise ReleaseCheckoutError("GitHub returned an invalid release PR list")
    return tuple(_snapshot_from_payload(item) for item in payload)


def _merge_pull_request(pull_request_number: int, expected_head_sha: str) -> None:
    command = [
        "gh",
        "pr",
        "merge",
        str(pull_request_number),
        "--squash",
        "--match-head-commit",
        expected_head_sha,
    ]
    try:
        subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise ReleaseCheckoutError(
            f"cannot merge release PR {pull_request_number}: {_command_error(command, error)}"
        ) from error


def _parse_args(arguments: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge and validate an automated release PR merge commit."
    )
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    parser.add_argument("--release-branch", required=True)
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--expected-owner", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=900.0)
    parser.add_argument("--poll-interval-seconds", type=float, default=5.0)
    return parser.parse_args(arguments)


def _fail(message: str) -> NoReturn:
    print(f"release checkout failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def main(arguments: Sequence[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if arguments is None else arguments)
    try:
        merge_sha = checkout_merged_release(
            repository=args.repository,
            release_branch=args.release_branch,
            expected_version=args.expected_version,
            expected_owner=args.expected_owner,
            get_pull_requests=_read_pull_requests,
            merge_pull_request=_merge_pull_request,
            timeout_seconds=args.timeout_seconds,
            poll_interval_seconds=args.poll_interval_seconds,
        )
    except ReleaseCheckoutError as error:
        _fail(str(error))

    print(merge_sha)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
