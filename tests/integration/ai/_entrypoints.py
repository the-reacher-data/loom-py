"""Shared fake ``entry_points()`` for AI integration tests.

``create_app`` resolves both the AI engine provider and the persistence
backend through ``loom.core.plugins.entrypoints.load_entry_point``. A test
that fakes ``entry_points()`` to install an in-process engine must not blind
the persistence lookup: every group other than the one under test has to keep
resolving against the real, installed distributions.
"""

from __future__ import annotations

from collections.abc import Sequence
from importlib.metadata import entry_points


def fake_entry_points(own_group: str, own_entries: Sequence[object]) -> type:
    """Build an ``entry_points()`` replacement scoped to one fake group.

    Args:
        own_group: Entry-point group the caller fakes.
        own_entries: Fake entry points returned for ``own_group``.

    Returns:
        A class assignable to ``entrypoints_module.entry_points``. Calling it
        (as the loader does) returns an object whose ``select(group=...)``
        answers ``own_group`` with ``own_entries`` and delegates every other
        group to the real ``importlib.metadata.entry_points()``.
    """

    class _FakeEntryPoints:
        def select(self, *, group: str) -> Sequence[object]:
            if group == own_group:
                return own_entries
            return tuple(entry_points().select(group=group))

    return _FakeEntryPoints


__all__ = ["fake_entry_points"]
