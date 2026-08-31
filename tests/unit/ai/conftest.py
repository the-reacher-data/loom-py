"""Shared fixtures for the ``loom.ai`` unit tests."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _event_loop_before_socket_block(request: pytest.FixtureRequest) -> None:
    """Create the pytest-asyncio runner before module-level autouse fixtures.

    ``test_fake_engine.py`` blocks all socket creation with an autouse
    fixture, but the asyncio event loop itself opens one socketpair (its
    self-pipe) when it is created.  Conftest autouse fixtures resolve before
    module ones, so requesting the runner here creates the loop first and the
    network block then applies only to the code under test.
    """
    if "_function_scoped_runner" in request.fixturenames:
        request.getfixturevalue("_function_scoped_runner")
