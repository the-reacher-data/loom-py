"""Streaming flow compiler: validates and compiles StreamFlow into CompiledPlan."""

from __future__ import annotations

from typing import Any

from omegaconf import DictConfig

from loom.streaming.compiler._bindings import resolve_flow_bindings
from loom.streaming.compiler._plan import CompiledPlan
from loom.streaming.compiler.phases.build_plan import build_plan
from loom.streaming.compiler.phases.validate import (
    validate_kafka,
    validate_outputs,
    validate_resources,
    validate_shapes,
)
from loom.streaming.graph._flow import StreamFlow


class CompilationError(Exception):
    """Raised when a StreamFlow fails validation."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__(f"Compilation failed with {len(errors)} error(s): {'; '.join(errors)}")


def compile_flow(flow: StreamFlow[Any, Any], *, runtime_config: DictConfig) -> CompiledPlan:
    """Compile a flow into an immutable plan.

    Raises:
        loom.streaming.compiler._compiler.CompilationError: If any validation fails.
    """
    return _Compiler().compile(flow, runtime_config=runtime_config)


class _Compiler:
    """Validates a StreamFlow and produces a CompiledPlan.

    Each validator is a pure function that returns a list of error strings.
    """

    def compile(self, flow: StreamFlow[Any, Any], *, runtime_config: DictConfig) -> CompiledPlan:
        """Run all validation phases then build the compiled plan."""
        errors: list[str] = []

        resolved_flow, binding_errors = resolve_flow_bindings(flow, runtime_config)
        errors.extend(binding_errors)
        if errors:
            raise CompilationError(errors)

        errors.extend(validate_kafka(resolved_flow, runtime_config))
        errors.extend(validate_resources(resolved_flow))
        errors.extend(validate_shapes(resolved_flow))
        errors.extend(validate_outputs(resolved_flow))

        if errors:
            raise CompilationError(errors)

        return build_plan(resolved_flow, runtime_config)
