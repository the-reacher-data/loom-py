"""Source and target binding resolution for ETL step compilation.

Internal module — consumed only by :mod:`loom.etl.compiler._compiler`.
"""

from __future__ import annotations

from typing import Any

from loom.etl.compiler._errors import ETLCompilationError
from loom.etl.compiler._plan import SourceBinding, TargetBinding
from loom.etl.io._source import Sources, SourceSet
from loom.etl.io._target import IntoFile, IntoTable, IntoTemp
from loom.etl.pipeline._step import ETLStep, _SourceForm

_VALID_TARGET_TYPES = (IntoTable, IntoFile, IntoTemp)


def resolve_source_bindings(step_type: type[ETLStep[Any]]) -> tuple[SourceBinding, ...]:
    """Build source bindings from the step class declaration.

    Handles all three source forms: ``NONE``, ``INLINE``, and ``GROUPED``.

    Args:
        step_type: Concrete ``ETLStep`` subclass.

    Returns:
        Tuple of :class:`~loom.etl.compiler._plan.SourceBinding` instances.

    Raises:
        ETLCompilationError: When the sources declaration is invalid.
    """
    match step_type._source_form:
        case _SourceForm.NONE:
            return ()
        case _SourceForm.INLINE:
            return _bindings_from_inline(step_type)
        case _SourceForm.GROUPED:
            return _bindings_from_grouped(step_type)
        case _:  # pragma: no cover
            raise AssertionError(f"Unhandled source form: {step_type._source_form!r}")


def resolve_target_binding(step_type: type[ETLStep[Any]]) -> TargetBinding:
    """Build the target binding from the step class declaration.

    Args:
        step_type: Concrete ``ETLStep`` subclass.

    Returns:
        :class:`~loom.etl.compiler._plan.TargetBinding` with the normalized spec.

    Raises:
        ETLCompilationError: When ``target`` is missing or has an invalid type.
    """
    target = step_type.target
    if target is None:
        raise ETLCompilationError.missing_target(step_type)
    if not isinstance(target, _VALID_TARGET_TYPES):
        raise ETLCompilationError.invalid_target_type(step_type)
    return TargetBinding(spec=target._to_spec())


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _bindings_from_inline(step_type: type[ETLStep[Any]]) -> tuple[SourceBinding, ...]:
    return tuple(
        SourceBinding(alias=alias, spec=src._to_spec(alias))
        for alias, src in step_type._inline_sources.items()
    )


def _bindings_from_grouped(step_type: type[ETLStep[Any]]) -> tuple[SourceBinding, ...]:
    match step_type.sources:
        case Sources() | SourceSet() as grouped:
            return tuple(SourceBinding(alias=spec.alias, spec=spec) for spec in grouped._to_specs())
        case _:
            raise ETLCompilationError.invalid_sources_type(step_type)
