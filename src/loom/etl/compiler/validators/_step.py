"""Unified per-step compile-time validation.

Internal module — consumed only by :mod:`loom.etl.compiler._compiler`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from loom.etl.compiler._plan import SourceBinding, TargetBinding
from loom.etl.compiler.validators._param_exprs import validate_param_exprs
from loom.etl.compiler.validators._structural import validate_execute_signature
from loom.etl.compiler.validators._upsert import validate_upsert_spec


@dataclass(frozen=True)
class StepCompilationContext:
    """Carries all resolved step data needed by per-step validators.

    Args:
        step_type:       The concrete ``ETLStep`` subclass being compiled.
        params_type:     The ``ParamsT`` generic argument.
        source_bindings: Resolved source alias → spec bindings.
        target_binding:  Resolved target binding.
    """

    step_type: type[Any]
    params_type: type[Any]
    source_bindings: tuple[SourceBinding, ...]
    target_binding: TargetBinding


def validate_step(ctx: StepCompilationContext) -> None:
    """Run all per-step compile-time validators against *ctx*.

    Validates:
    * ``execute()`` signature matches sources and params type.
    * UPSERT constraints when the target is an upsert spec.

    Args:
        ctx: Fully resolved step compilation context.

    Raises:
        ETLCompilationError: On any structural or constraint violation.
    """
    validate_execute_signature(ctx.step_type, ctx.params_type, ctx.source_bindings)
    validate_upsert_spec(ctx.step_type, ctx.target_binding.spec)
    validate_param_exprs(ctx.step_type, ctx.params_type, ctx.source_bindings, ctx.target_binding)
