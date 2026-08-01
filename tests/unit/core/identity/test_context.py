"""The identity context guard: default, restoration and task isolation."""

from __future__ import annotations

import asyncio

from loom.core.identity import (
    ANONYMOUS,
    Identity,
    current_identity,
    reset_identity,
    set_identity,
)

_ALICE = Identity(subject="alice", mechanism="test")
_BOB = Identity(subject="bob", mechanism="test")


def test_defaults_to_the_anonymous_identity() -> None:
    """The context never yields ``None``: the default is the explicit anonymous."""
    assert current_identity() is ANONYMOUS


def test_returns_the_installed_identity() -> None:
    """Between set and reset the context yields exactly what was installed."""
    token = set_identity(_ALICE)
    try:
        assert current_identity() is _ALICE
    finally:
        reset_identity(token)


def test_reset_restores_the_anonymous_default() -> None:
    """After the reset the context is anonymous again, never a stale identity."""
    token = set_identity(_ALICE)
    reset_identity(token)
    assert current_identity() is ANONYMOUS


def test_reset_restores_the_previous_identity() -> None:
    """Nested installs unwind to the outer identity, not to the default."""
    outer = set_identity(_ALICE)
    try:
        inner = set_identity(_BOB)
        reset_identity(inner)
        assert current_identity() is _ALICE
    finally:
        reset_identity(outer)


async def test_concurrent_tasks_never_see_each_others_identity() -> None:
    """Two concurrent callers must not cross: the leak this design exists to prevent."""
    seen: dict[str, str] = {}

    async def _run(identity: Identity) -> None:
        token = set_identity(identity)
        try:
            await asyncio.sleep(0)
            seen[identity.subject] = current_identity().subject
        finally:
            reset_identity(token)

    await asyncio.gather(_run(_ALICE), _run(_BOB))

    assert seen == {"alice": "alice", "bob": "bob"}
