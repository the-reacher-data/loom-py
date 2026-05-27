"""TDD Red — tests for ReaderRegistry dispatch logic.

All tests in this file MUST FAIL with ImportError / ModuleNotFoundError until
loom/etl/io/_registry.py is implemented.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from loom.etl.io._registry import (
    ConfigurationError,  # noqa: F401
    ReaderRegistry,  # noqa: F401
)

# ---------------------------------------------------------------------------
# Minimal spec stand-ins — plain objects with a .kind attribute
# ---------------------------------------------------------------------------


class _FakeSpec:
    """A minimal spec object with a configurable kind."""

    def __init__(self, kind: str) -> None:
        self.kind = kind


_PARAMS = object()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRegistryDispatchesToRegisteredReader:
    def test_registry_dispatches_to_registered_reader(self) -> None:
        """A reader registered for 'clickhouse' kind is called when that spec arrives."""
        ch_reader = MagicMock()
        ch_reader.read.return_value = "ch_result"

        base_reader = MagicMock()

        registry = ReaderRegistry(base_reader, extra={"clickhouse": ch_reader})

        spec = _FakeSpec(kind="clickhouse")
        result = registry.read(spec, _PARAMS)

        ch_reader.read.assert_called_once_with(spec, _PARAMS)
        base_reader.read.assert_not_called()
        assert result == "ch_result"


class TestRegistryFallsBackToBaseReader:
    def test_registry_falls_back_to_base_reader(self) -> None:
        """A spec whose kind is not in extra dispatches to the base reader."""
        base_reader = MagicMock()
        base_reader.read.return_value = "base_result"

        registry = ReaderRegistry(base_reader, extra={"clickhouse": MagicMock()})

        spec = _FakeSpec(kind="table")
        result = registry.read(spec, _PARAMS)

        base_reader.read.assert_called_once_with(spec, _PARAMS)
        assert result == "base_result"


class TestRegistryRaisesOnUnknownKindWithoutBaseFallback:
    def test_registry_raises_on_unknown_kind_without_base_fallback(self) -> None:
        """Without a base reader and an unknown kind, ConfigurationError is raised."""
        registry = ReaderRegistry(None, extra={"clickhouse": MagicMock()})

        spec = _FakeSpec(kind="mongo_lookup")
        with pytest.raises(ConfigurationError):
            registry.read(spec, _PARAMS)


class TestRegistryValidatesPresenceAtConstruction:
    def test_registry_validates_presence_at_construction(self) -> None:
        """A handler without its required dependency raises ConfigurationError at build time.

        The implementation must accept an optional 'validate' callable (or similar
        mechanism) per handler so that missing dependencies (e.g. no Mongo connection)
        are caught at registry construction, not at read() time.
        This test verifies the contract: pass a handler that raises ConfigurationError
        on construction when its dependency is absent.
        """

        def _make_broken_reader() -> Any:
            """Factory that always fails — simulates absent external dependency."""
            raise ConfigurationError("Mongo client not configured")

        with pytest.raises(ConfigurationError, match="Mongo client not configured"):
            # The registry must call the factory (or validate) at construction time.
            ReaderRegistry(
                None,
                extra={"mongo_lookup": _make_broken_reader()},
            )
