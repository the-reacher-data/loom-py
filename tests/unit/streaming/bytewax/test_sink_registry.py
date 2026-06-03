"""TDD red tests for SinkRegistry, RegisteredSink and RuntimeSinkBinding."""

from __future__ import annotations

from typing import Any, ClassVar

import msgspec
import pytest

from loom.core.config.context import ConfigContext
from loom.streaming.bytewax._sink_registry import (
    RuntimeSinkBinding,
    SinkRegistry,
)
from loom.streaming.core._errors import ErrorKind
from loom.streaming.core._exceptions import DuplicateErrorSinkError

pytestmark = pytest.mark.bytewax


# ---------------------------------------------------------------------------
# Fake helpers used across multiple tests
# ---------------------------------------------------------------------------


class _FakeConfig(msgspec.Struct, frozen=True):
    some_field: str = "default"


class _FakeSinkObject:
    """Minimal stand-in for a real bytewax sink."""


class _FakeErrorSink:
    """Concrete class that fulfils the RegisteredSink protocol for errors."""

    sink_type: ClassVar[str] = "test_error_sink"
    config_type: ClassVar[type] = _FakeConfig

    @classmethod
    def build_binding(cls, cfg: Any, ctx: ConfigContext) -> RuntimeSinkBinding:
        return RuntimeSinkBinding(
            purpose="errors",
            sink=_FakeSinkObject(),
            kinds=(ErrorKind.TASK,),
        )


class _FakeTerminalSink:
    """Concrete class that fulfils the RegisteredSink protocol for terminal."""

    sink_type: ClassVar[str] = "test_terminal_sink"
    config_type: ClassVar[type] = _FakeConfig

    @classmethod
    def build_binding(cls, cfg: Any, ctx: ConfigContext) -> RuntimeSinkBinding:
        return RuntimeSinkBinding(
            purpose="terminal",
            sink=_FakeSinkObject(),
        )


class _FakeBusinessErrorSink:
    """Error sink using BUSINESS kind — used for duplicate-kind tests."""

    sink_type: ClassVar[str] = "test_business_error_sink"
    config_type: ClassVar[type] = _FakeConfig

    @classmethod
    def build_binding(cls, cfg: Any, ctx: ConfigContext) -> RuntimeSinkBinding:
        return RuntimeSinkBinding(
            purpose="errors",
            sink=_FakeSinkObject(),
            kinds=(ErrorKind.TASK,),  # same kind as _FakeErrorSink → triggers duplicate
        )


# ---------------------------------------------------------------------------
# TestSinkRegistry
# ---------------------------------------------------------------------------


class TestSinkRegistry:
    def test_register_valid_sink_stores_it(self) -> None:
        """AC-1: registering a class that satisfies RegisteredSink works."""
        registry = SinkRegistry()
        registry.register(_FakeErrorSink)
        # No exception means the class was accepted.

    def test_register_invalid_class_raises_type_error(self) -> None:
        """AC-8: registering a class without the required protocol attrs raises TypeError."""
        registry = SinkRegistry()

        class _BadSink:
            """Missing sink_type, config_type and build_binding."""

        with pytest.raises(TypeError):
            registry.register(_BadSink)

    def test_resolve_calls_build_binding_with_parsed_cfg(self) -> None:
        """AC-9: resolve parses each YAML section and passes the config object to build_binding."""
        received: list[Any] = []

        class _CapturingSink:
            sink_type: ClassVar[str] = "capture_sink"
            config_type: ClassVar[type] = _FakeConfig

            @classmethod
            def build_binding(cls, cfg: Any, ctx: ConfigContext) -> RuntimeSinkBinding:
                received.append(cfg)
                return RuntimeSinkBinding(purpose="terminal", sink=_FakeSinkObject())

        registry = SinkRegistry()
        registry.register(_CapturingSink)

        ctx = ConfigContext.from_dict(
            {
                "streaming": {
                    "sinks": {
                        "my_sink": {
                            "type": "capture_sink",
                            "some_field": "hello",
                        }
                    }
                }
            }
        )

        bindings = registry.resolve(ctx)

        assert len(bindings) == 1
        assert len(received) == 1
        captured_cfg = received[0]
        assert isinstance(captured_cfg, _FakeConfig)
        assert captured_cfg.some_field == "hello"

    def test_resolve_raises_on_duplicate_error_kind(self) -> None:
        """AC-3: two error sinks claiming the same ErrorKind raises DuplicateErrorSinkError."""
        registry = SinkRegistry()
        registry.register(_FakeErrorSink)
        registry.register(_FakeBusinessErrorSink)

        ctx = ConfigContext.from_dict(
            {
                "streaming": {
                    "sinks": {
                        "sink_a": {"type": "test_error_sink", "some_field": "a"},
                        "sink_b": {"type": "test_business_error_sink", "some_field": "b"},
                    }
                }
            }
        )

        with pytest.raises(DuplicateErrorSinkError):
            registry.resolve(ctx)

    def test_resolve_empty_registry_returns_empty_list(self) -> None:
        """Base case: no sinks registered → resolve returns an empty list."""
        registry = SinkRegistry()

        ctx = ConfigContext.from_dict(
            {"streaming": {"sinks": {"my_sink": {"type": "test_error_sink", "some_field": "x"}}}}
        )

        bindings = registry.resolve(ctx)

        assert bindings == []

    def test_resolve_skips_yaml_entries_with_unknown_type(self) -> None:
        """A sink entry whose type does not match any registered class is silently skipped."""
        registry = SinkRegistry()
        registry.register(_FakeErrorSink)

        ctx = ConfigContext.from_dict(
            {
                "streaming": {
                    "sinks": {
                        "known": {"type": "test_error_sink", "some_field": "v"},
                        "unknown": {"type": "nonexistent_sink_type", "some_field": "v"},
                    }
                }
            }
        )

        bindings = registry.resolve(ctx)

        assert len(bindings) == 1

    def test_resolve_no_sinks_section_returns_empty(self) -> None:
        """AC-7: when the config has no streaming.sinks key the result is an empty list."""
        registry = SinkRegistry()
        registry.register(_FakeErrorSink)

        ctx = ConfigContext.from_dict({"streaming": {"runtime": {"workers_per_process": 1}}})

        bindings = registry.resolve(ctx)

        assert bindings == []
