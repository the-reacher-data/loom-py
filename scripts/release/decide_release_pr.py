"""Decide whether a release version needs a new bump, an existing one, or a rebuild."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, NoReturn

_BASE_BRANCH = "master"


class ReleasePullRequestError(RuntimeError):
    """Raised when the release pull request state cannot be acted on safely."""


class Decision(StrEnum):
    """What the release has to do with the bump for its version."""

    CREATE = "create"
    REUSE = "reuse"
    RECYCLE = "recycle"


@dataclass(frozen=True)
class ReleaseBranchState:
    """Everything the decision needs about one ``docs/release-v<version>`` branch."""

    open_count: int
    merged_count: int
    branch_exists: bool
    merge_base_sha: str | None = None
    base_branch_sha: str | None = None


def decide(state: ReleaseBranchState) -> Decision:
    """Return the action for the release branch, refusing states a human must resolve."""
    if state.open_count > 1 or state.merged_count > 1:
        raise ReleasePullRequestError("multiple open or merged release PRs exist")

    if state.open_count == 1:
        if state.merge_base_sha is None or state.base_branch_sha is None:
            raise ReleasePullRequestError("an open release PR needs its base and tip resolved")
        # A release PR whose base is no longer the tip can never merge: the
        # checkout guard compares the two and refuses, and nothing ages out of
        # that state.
        if state.merge_base_sha != state.base_branch_sha:
            return Decision.RECYCLE
        return Decision.REUSE

    # Only a merged bump means the version already reached the base branch; a
    # closed one was discarded and has to be built again.
    if state.merged_count == 1:
        return Decision.REUSE
    if state.branch_exists:
        raise ReleasePullRequestError("release branch exists without an open or merged release PR")
    return Decision.CREATE


def _run(command: Sequence[str]) -> str:
    try:
        completed = subprocess.run(list(command), check=True, capture_output=True, text=True)
    except (OSError, subprocess.CalledProcessError) as error:
        detail = getattr(error, "stderr", "") or str(error)
        raise ReleasePullRequestError(f"{' '.join(command)} failed: {detail.strip()}") from error
    return completed.stdout


def _json(command: Sequence[str]) -> Any:
    try:
        return json.loads(_run(command))
    except json.JSONDecodeError as error:
        raise ReleasePullRequestError(f"{' '.join(command)} returned invalid JSON") from error


def read_branch_state(release_branch: str, repository_slug: str) -> ReleaseBranchState:
    """Collect the remote state of a release branch through the GitHub CLI."""
    pull_requests = _json(
        ["gh", "pr", "list", "--head", release_branch, "--state", "all", "--json", "number,state"]
    )
    if not isinstance(pull_requests, list):
        raise ReleasePullRequestError("GitHub returned an invalid release PR list")
    states = [str(item.get("state", "")).upper() for item in pull_requests]

    refs = _json(["gh", "api", f"repos/{repository_slug}/git/matching-refs/heads/{release_branch}"])
    if not isinstance(refs, list):
        raise ReleasePullRequestError("GitHub returned an invalid ref list")
    branch_exists = any(item.get("ref") == f"refs/heads/{release_branch}" for item in refs)

    open_count = states.count("OPEN")
    merge_base_sha: str | None = None
    base_branch_sha: str | None = None
    if open_count == 1:
        merge_base_sha = _run(
            [
                "gh",
                "api",
                f"repos/{repository_slug}/compare/{_BASE_BRANCH}...{release_branch}",
                "--jq",
                ".merge_base_commit.sha",
            ]
        ).strip()
        base_branch_sha = _run(
            ["gh", "api", f"repos/{repository_slug}/commits/{_BASE_BRANCH}", "--jq", ".sha"]
        ).strip()

    return ReleaseBranchState(
        open_count=open_count,
        merged_count=states.count("MERGED"),
        branch_exists=branch_exists,
        merge_base_sha=merge_base_sha,
        base_branch_sha=base_branch_sha,
    )


def _parse_args(arguments: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Decide what to do with a release version bump.")
    parser.add_argument("--release-branch", required=True)
    parser.add_argument("--repository-slug", required=True)
    return parser.parse_args(arguments)


def _fail(message: str) -> NoReturn:
    print(f"release PR decision failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def main(arguments: Sequence[str] | None = None) -> int:
    options = _parse_args(arguments)
    try:
        decision = decide(read_branch_state(options.release_branch, options.repository_slug))
    except ReleasePullRequestError as error:
        _fail(str(error))
    print(decision.value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
