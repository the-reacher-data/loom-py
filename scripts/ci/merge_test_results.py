"""Merge the JUnit reports of several test lanes into one document."""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ElementTree
from collections.abc import Sequence
from pathlib import Path
from typing import NoReturn

_SUITE = "testsuite"
_SUITES = "testsuites"


class MergeError(RuntimeError):
    """Raised when the lane reports cannot be merged into one document."""


def _suites_of(path: Path) -> list[ElementTree.Element]:
    """Return every test suite element of one JUnit report."""
    try:
        root = ElementTree.parse(path).getroot()
    except (OSError, ElementTree.ParseError) as error:
        raise MergeError(f"cannot read JUnit report {path}: {error}") from error
    if root.tag == _SUITE:
        return [root]
    if root.tag == _SUITES:
        return list(root.findall(_SUITE))
    raise MergeError(f"{path} is not a JUnit report: its root element is {root.tag!r}")


def merge_junit(paths: Sequence[Path]) -> ElementTree.ElementTree:
    """Return one document holding the suites of every report.

    A report contributing no suite is refused: a lane whose results went missing
    must fail the merge rather than disappear from the totals the gate reads.
    """
    if not paths:
        raise MergeError("no JUnit report to merge")
    merged = ElementTree.Element(_SUITES)
    for path in paths:
        suites = _suites_of(path)
        if not suites:
            raise MergeError(f"{path} reports no test suite")
        merged.extend(suites)
    return ElementTree.ElementTree(merged)


def _parse_args(arguments: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge the JUnit reports of several lanes.")
    parser.add_argument("--junit", action="append", type=Path, default=[], required=True)
    parser.add_argument("--junit-output", type=Path, required=True)
    return parser.parse_args(arguments)


def _fail(message: str) -> NoReturn:
    print(f"merging test results failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def main(arguments: Sequence[str] | None = None) -> int:
    options = _parse_args(arguments)
    try:
        merged = merge_junit(options.junit)
    except MergeError as error:
        _fail(str(error))
    merged.write(options.junit_output, encoding="utf-8", xml_declaration=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
