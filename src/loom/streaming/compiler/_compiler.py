"""Streaming flow compiler: validates and compiles StreamFlow into CompiledPlan."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from loom.core.config import ConfigContext
from loom.streaming.compiler._bindings import resolve_flow_bindings
from loom.streaming.compiler._plan import CompilationError, CompiledPlan
from loom.streaming.compiler.phases.build_plan import build_plan
from loom.streaming.compiler.phases.validate import (
    validate_kafka,
    validate_outputs,
    validate_resources,
    validate_shapes,
    validate_storage_sinks,
)
from loom.streaming.graph._flow import StreamFlow


def compile_flow(
    flow: StreamFlow[Any, Any],
    *,
    config: ConfigContext | Mapping[str, Any],
) -> CompiledPlan:
    """Compile a flow into an immutable plan.

    Args:
        flow: User-declared streaming flow.
        config: Canonical runtime config context used for binding
            resolution and Kafka settings extraction.

    Returns:
        Immutable :class:`CompiledPlan` ready for adapter wiring.

    Raises:
        CompilationError: If binding resolution or any validation phase fails.
    """
    return _Compiler().compile(flow, _ensure_config_context(config))


class _Compiler:
    """Validates a StreamFlow and produces a CompiledPlan.

    Each validator is a pure function that returns a list of error strings.
    """

    def compile(self, flow: StreamFlow[Any, Any], ctx: ConfigContext) -> CompiledPlan:
        """Run all validation phases then build the compiled plan."""
        errors: list[str] = []

        resolved_flow, binding_errors = resolve_flow_bindings(flow, ctx)
        errors.extend(binding_errors)
        if errors:
            raise CompilationError(errors)

        errors.extend(validate_kafka(resolved_flow, ctx))
        errors.extend(validate_storage_sinks(resolved_flow, ctx))
        errors.extend(validate_resources(resolved_flow))
        errors.extend(validate_shapes(resolved_flow))
        errors.extend(validate_outputs(resolved_flow))

        if errors:
            raise CompilationError(errors)

        return build_plan(resolved_flow, ctx)


def _ensure_config_context(
    source: ConfigContext | Mapping[str, Any],
) -> ConfigContext:
    """Normalize supported config inputs to a :class:`ConfigContext`."""
    if isinstance(source, ConfigContext):
        return source
    return ConfigContext.from_dict(source)
