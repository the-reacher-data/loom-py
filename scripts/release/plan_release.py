"""Decide which version a labelled merge releases, from the branches it ships."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tomllib
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final, NoReturn

_RELEASE_TAG_GLOB = "v[0-9]*.[0-9]*.[0-9]*"
_RELEASE_TAG = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")
_PARTS: Final[tuple[str, ...]] = ("major", "minor", "patch")

CommitPullRequests = Callable[[str], tuple[str, ...]]


class ReleasePlanError(RuntimeError):
    """Raised when the release a merge would ship cannot be determined."""


@dataclass(frozen=True, slots=True)
class ShippedPullRequest:
    """One pull request the release ships, and the part its branch asks for."""

    sha: str
    head_ref: str
    part: str | None


@dataclass(frozen=True, slots=True)
class ReleasePlan:
    """The release a labelled merge commit produces."""

    last_tag: str | None
    part: str
    version: str
    shipped: tuple[ShippedPullRequest, ...]

    def render(self) -> str:
        """Return the plan as the lines an operator reads before publishing."""
        lines = [
            f"last tag : {self.last_tag or '(none)'}",
            f"part     : {self.part}",
            f"version  : {self.version}",
            "ships    :",
        ]
        for entry in self.shipped:
            part = entry.part or "-"
            lines.append(f"  {entry.sha[:8]}  {part:<5}  {entry.head_ref}")
        return "\n".join(lines) + "\n"


def _run(command: Sequence[str]) -> str:
    try:
        completed = subprocess.run(command, check=True, capture_output=True, text=True)
    except (OSError, subprocess.CalledProcessError) as error:
        detail = getattr(error, "stderr", "") or str(error)
        raise ReleasePlanError(f"{' '.join(command)} failed: {detail.strip()}") from error
    return completed.stdout


def branch_rules(repository: Path) -> Mapping[str, tuple[str, ...]]:
    """Return the branch patterns of every class declared in pyproject.toml."""
    data = tomllib.loads((repository / "pyproject.toml").read_text(encoding="utf-8"))
    section = data.get("tool", {}).get("semantic_branch", {})
    return {
        key: tuple(section.get(key, ())) for key in ("major", "minor", "patch", "release_ignore")
    }


def classify_branch(head_ref: str, rules: Mapping[str, tuple[str, ...]]) -> str | None:
    """Return the part *head_ref* asks for, or None when its class ships nothing.

    Raises:
        ReleasePlanError: When no class in the rules matches *head_ref*.
    """
    for part in _PARTS:
        if any(re.fullmatch(pattern, head_ref) for pattern in rules.get(part, ())):
            return part
    if any(re.fullmatch(pattern, head_ref) for pattern in rules.get("release_ignore", ())):
        return None
    raise ReleasePlanError(
        f"branch '{head_ref}' matches no class in [tool.semantic_branch]: "
        "add its prefix there or rename the branch"
    )


def highest_part(parts: Iterable[str | None]) -> str | None:
    """Return the largest part among *parts*, or None when every one ships nothing."""
    present = {part for part in parts if part is not None}
    for part in _PARTS:
        if part in present:
            return part
    return None


def next_version(last_tag: str | None, part: str) -> str:
    """Return the version that raising *part* from *last_tag* produces."""
    if last_tag is None:
        return {"major": "1.0.0", "minor": "0.1.0", "patch": "0.0.1"}[part]
    matched = _RELEASE_TAG.match(last_tag)
    if matched is None:
        raise ReleasePlanError(f"tag '{last_tag}' is not a release tag")
    major, minor, patch = (int(group) for group in matched.groups())
    if part == "major":
        return f"{major + 1}.0.0"
    if part == "minor":
        return f"{major}.{minor + 1}.0"
    return f"{major}.{minor}.{patch + 1}"


def latest_release_tag(repository: Path, merge_sha: str) -> str | None:
    """Return the highest release tag reachable from *merge_sha*."""
    output = _run(
        (
            "git",
            "-C",
            str(repository),
            "tag",
            "--list",
            _RELEASE_TAG_GLOB,
            "--merged",
            merge_sha,
            "--sort=-v:refname",
        )
    )
    for line in output.splitlines():
        candidate = line.strip()
        if _RELEASE_TAG.match(candidate):
            return candidate
    return None


def shipped_commits(repository: Path, last_tag: str | None, merge_sha: str) -> tuple[str, ...]:
    """Return every commit the release ships, oldest last."""
    revision_range = f"{last_tag}..{merge_sha}" if last_tag else merge_sha
    output = _run(
        ("git", "-C", str(repository), "log", "--no-merges", "--pretty=%H", revision_range)
    )
    return tuple(line.strip() for line in output.splitlines() if line.strip())


def gh_commit_pull_requests(slug: str) -> CommitPullRequests:
    """Return a reader of the head refs of the pull requests a commit came from."""

    def read(sha: str) -> tuple[str, ...]:
        output = _run(("gh", "api", f"repos/{slug}/commits/{sha}/pulls", "--jq", ".[].head.ref"))
        return tuple(line.strip() for line in output.splitlines() if line.strip())

    return read


def plan_release(
    repository: Path,
    merge_sha: str,
    commit_pull_requests: CommitPullRequests,
) -> ReleasePlan:
    """Return the release *merge_sha* ships, from the branches merged since the last tag.

    The part is the highest one any shipped branch asks for, so a batch holding a
    feature never ships as a patch. A commit with no pull request, or one whose
    branch matches no declared class, refuses the release instead of lowering it.

    Args:
        repository:           Checkout to read tags and commits from.
        merge_sha:            Commit the release is cut from.
        commit_pull_requests: Reader of the head refs a commit came from.

    Returns:
        The planned release.

    Raises:
        ReleasePlanError: When the range is empty, a commit has no pull request,
            a branch is unclassified, or nothing in the range ships a version.
    """
    last_tag = latest_release_tag(repository, merge_sha)
    commits = shipped_commits(repository, last_tag, merge_sha)
    if not commits:
        raise ReleasePlanError(
            f"nothing to release: no commits since {last_tag or 'the start of history'}"
        )

    rules = branch_rules(repository)
    shipped: list[ShippedPullRequest] = []
    for sha in commits:
        head_refs = commit_pull_requests(sha)
        if not head_refs:
            raise ReleasePlanError(
                f"commit {sha} belongs to no pull request: a direct push cannot be classified"
            )
        for head_ref in head_refs:
            shipped.append(ShippedPullRequest(sha, head_ref, classify_branch(head_ref, rules)))

    part = highest_part(entry.part for entry in shipped)
    if part is None:
        raise ReleasePlanError(
            "nothing to release: every branch since "
            f"{last_tag or 'the start of history'} belongs to a class that ships no version"
        )
    return ReleasePlan(last_tag, part, next_version(last_tag, part), tuple(shipped))


def _parse_args(arguments: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan the release a labelled merge ships.")
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    parser.add_argument("--merge-sha", required=True)
    parser.add_argument("--slug", required=True, help="owner/repo the pull requests live in")
    parser.add_argument(
        "--format",
        choices=("text", "github"),
        default="text",
        help="text prints the plan for an operator; github writes version and part outputs",
    )
    return parser.parse_args(arguments)


def _fail(message: str) -> NoReturn:
    print(f"release plan failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def main(arguments: Sequence[str] | None = None) -> int:
    options = _parse_args(arguments)
    try:
        plan = plan_release(
            options.repository, options.merge_sha, gh_commit_pull_requests(options.slug)
        )
    except ReleasePlanError as error:
        _fail(str(error))
    if options.format == "github":
        print(json.dumps({"version": plan.version, "part": plan.part}))
    else:
        print(plan.render(), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
