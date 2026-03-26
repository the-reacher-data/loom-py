"""Structural validation — execute() signature and params type compatibility."""

from __future__ import annotations

import inspect
from typing import Any

import msgspec

from loom.etl.compiler._errors import ETLCompilationError
from loom.etl.compiler._plan import SourceBinding


def validate_execute_signature(
    step_type: type[Any],
    params_type: type[Any],
    source_bindings: tuple[SourceBinding, ...],
) -> None:
    """Validate that execute() has the correct params arg and matching keyword frames.

    Args:
        step_type:       The ETLStep subclass being compiled.
        params_type:     The expected params type (ParamsT).
        source_bindings: Compiled source bindings for the step.

    Raises:
        ETLCompilationError: When the signature is structurally invalid.
    """
    sig = inspect.signature(step_type.execute)
    params = list(sig.parameters.values())
    _validate_params_arg(step_type, params, params_type)
    kw_only = _collect_kw_only_frames(params)
    source_aliases = {b.alias for b in source_bindings}
    _check_missing_frames(step_type, source_aliases, kw_only)
    _check_extra_frames(step_type, source_aliases, kw_only)


def validate_params_compat(
    component_type: type[Any],
    component_params: type[Any],
    context_params: type[Any],
) -> None:
    """Raise if component_params requires fields absent from context_params.

    Passes when:
    * Both types are the same class.
    * context_params is a subclass of component_params.
    * Every field in component_params is present in context_params (duck-type).

    Args:
        component_type:   The step or process class being validated (for error messages).
        component_params: The params type declared on the step/process.
        context_params:   The params type declared on the enclosing process/pipeline.

    Raises:
        ETLCompilationError: When required fields are missing from context_params.
    """
    if component_params is context_params:
        return
    if issubclass(context_params, component_params):
        return
    component_fields = {f.name for f in msgspec.structs.fields(component_params)}
    context_fields = {f.name for f in msgspec.structs.fields(context_params)}
    missing = component_fields - context_fields
    if missing:
        raise ETLCompilationError(
            f"{component_type.__qualname__} requires params fields "
            f"{sorted(missing)} not present in {context_params.__name__}"
        )


def _validate_params_arg(
    step_type: type[Any],
    params: list[inspect.Parameter],
    params_type: type[Any],
) -> None:
    positional_kinds = {
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.POSITIONAL_ONLY,
    }
    positional = [p for p in params if p.kind in positional_kinds and p.name != "self"]
    if not positional:
        raise ETLCompilationError(
            f"{step_type.__qualname__}.execute: first parameter must be "
            f"'params: {params_type.__name__}'"
        )
    first = positional[0]
    if first.name != "params":
        raise ETLCompilationError(
            f"{step_type.__qualname__}.execute: first parameter must be named 'params', "
            f"got '{first.name}'"
        )


def _collect_kw_only_frames(
    params: list[inspect.Parameter],
) -> dict[str, inspect.Parameter]:
    return {p.name: p for p in params if p.kind is inspect.Parameter.KEYWORD_ONLY}


def _check_missing_frames(
    step_type: type[Any],
    source_aliases: set[str],
    kw_only: dict[str, inspect.Parameter],
) -> None:
    missing = source_aliases - set(kw_only)
    if missing:
        raise ETLCompilationError(
            f"{step_type.__qualname__}.execute: source(s) {sorted(missing)} "
            f"declared in sources but missing as keyword-only parameter(s) after '*'"
        )


def _check_extra_frames(
    step_type: type[Any],
    source_aliases: set[str],
    kw_only: dict[str, inspect.Parameter],
) -> None:
    extra = set(kw_only) - source_aliases
    if extra:
        raise ETLCompilationError(
            f"{step_type.__qualname__}.execute: parameter(s) {sorted(extra)} "
            f"declared after '*' but not found in sources"
        )
