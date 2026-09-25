"""Compile-time check of ``FromConfig`` keys against the runner's config.

Internal module — consumed only by :mod:`loom.etl.compiler._compiler`.
"""

from __future__ import annotations

from loom.core.config import ConfigContext
from loom.etl.compiler._errors import ETLCompilationError
from loom.etl.compiler._plan import (
    PipelinePlan,
    ProcessPlan,
    StepPlan,
    iter_all_steps,
    iter_steps_in_process,
)
from loom.etl.runtime._config_values import ConfigValueError, resolve_config_value


def validate_step_config(plan: StepPlan, context: ConfigContext) -> None:
    """Check that every ``FromConfig`` key of *plan* resolves and validates.

    Values are resolved and discarded; only the key and type reach an error.

    Args:
        plan: Compiled step plan.
        context: Config the runner was built from.

    Raises:
        ETLCompilationError: When a key is missing, cannot be resolved, or its
            value does not validate as the declared type.
    """
    for binding in plan.config_bindings:
        try:
            resolve_config_value(context, binding.key, binding.value_type)
        except ConfigValueError as exc:
            raise ETLCompilationError.unresolved_config_value(
                plan.step_type, binding.alias, str(exc)
            ) from None


def validate_process_config(plan: ProcessPlan, context: ConfigContext) -> None:
    """Run :func:`validate_step_config` on every step of a process plan."""
    for step in iter_steps_in_process(plan):
        validate_step_config(step, context)


def validate_plan_config(plan: PipelinePlan, context: ConfigContext) -> None:
    """Run :func:`validate_step_config` on every step of a pipeline plan."""
    for step in iter_all_steps(plan):
        validate_step_config(step, context)
