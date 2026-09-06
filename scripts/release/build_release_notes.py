"""Build the release notes for every commit accumulated since the last tag."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import NoReturn

_RELEASE_TAG_GLOB = "v[0-9]*.[0-9]*.[0-9]*"
_RELEASE_TAG = re.compile(r"^v\d+\.\d+\.\d+$")


class ReleaseNotesError(RuntimeError):
    """Raised when the notes for a release cannot be built."""


def _run_git(repository: Path, *arguments: str) -> str:
    command = ("git", "-C", str(repository), *arguments)
    try:
        completed = subprocess.run(command, check=True, capture_output=True, text=True)
    except (OSError, subprocess.CalledProcessError) as error:
        detail = getattr(error, "stderr", "") or str(error)
        raise ReleaseNotesError(f"{' '.join(command)} failed: {detail.strip()}") from error
    return completed.stdout


def latest_release_tag(repository: Path) -> str | None:
    """Return the highest version tag reachable from HEAD, or None when there is none."""
    output = _run_git(
        repository,
        "tag",
        "--list",
        _RELEASE_TAG_GLOB,
        "--merged",
        "HEAD",
        "--sort=-v:refname",
    )
    for line in output.splitlines():
        candidate = line.strip()
        if _RELEASE_TAG.match(candidate):
            return candidate
    return None


def release_entries(repository: Path, last_tag: str | None) -> tuple[str, ...]:
    """Return one entry per non-merge commit that the release ships."""
    revision_range = f"{last_tag}..HEAD" if last_tag else "HEAD"
    output = _run_git(repository, "log", "--no-merges", "--pretty=%s", revision_range)
    return tuple(line.strip() for line in output.splitlines() if line.strip())


def render_release_notes(version: str, last_tag: str | None, entries: Sequence[str]) -> str:
    """Render the notes body, refusing a release that ships no commits."""
    if not entries:
        raise ReleaseNotesError(
            f"nothing to release: no commits since {last_tag or 'the start of history'}"
        )
    since = f"Changes since {last_tag}:" if last_tag else "Changes:"
    listed = "\n".join(f"- {entry}" for entry in entries)
    return f"# 🚀 Release {version}\n\n{since}\n\n{listed}\n"


def build_release_notes(repository: Path, version: str) -> str:
    """Return the notes for every commit between the last reachable tag and HEAD."""
    last_tag = latest_release_tag(repository)
    return render_release_notes(version, last_tag, release_entries(repository, last_tag))


def _parse_args(arguments: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build release notes from the accumulated commits."
    )
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(arguments)


def _fail(message: str) -> NoReturn:
    print(f"release notes failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def main(arguments: Sequence[str] | None = None) -> int:
    options = _parse_args(arguments)
    try:
        notes = build_release_notes(options.repository, options.version)
    except ReleaseNotesError as error:
        _fail(str(error))
    options.output.write_text(notes, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
