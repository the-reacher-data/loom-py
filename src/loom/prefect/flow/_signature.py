"""Build a Prefect flow's parameter signature from a ``msgspec.Struct``.

Prefect 3 displays a typed parameter form in the UI for each flow run.
For loom ETLs the parameter set is described by the pipeline's
``ParamsT`` ``msgspec.Struct``, so the factory synthesises an
``inspect.Signature`` that mirrors its fields.

This module also handles the naive→UTC datetime coercion needed when
Prefect (or a CLI/UI user) submits a parameter as a naive ISO string
that has to be compared against UTC-aware Polars columns inside the
runner.
"""

from __future__ import annotations

import inspect
from datetime import UTC, datetime
from types import UnionType
from typing import Any, Union, get_args, get_origin, get_type_hints

import msgspec


def signature_from_params_type(
    params_type: type[msgspec.Struct],
) -> list[inspect.Parameter]:
    """Return one ``inspect.Parameter`` per field of *params_type*.

    All fields are emitted as keyword-only without defaults so Prefect's
    form forces the operator to fill them (or accept the defaults that
    the YAML pre-binds at deploy time).

    Args:
        params_type: ``msgspec.Struct`` describing the ETL's parameters.

    Returns:
        Ordered list of parameters matching the struct's field order.
    """
    hints = get_type_hints(params_type)
    return [
        inspect.Parameter(
            name=field.name,
            kind=inspect.Parameter.KEYWORD_ONLY,
            default=inspect.Parameter.empty,
            annotation=hints.get(field.name, Any),
        )
        for field in msgspec.structs.fields(params_type)
    ]


def is_datetime_annotation(annotation: Any) -> bool:
    """Return ``True`` iff *annotation* is ``datetime`` or a union including it.

    Strictly rejects ``date`` (which is a supertype of ``datetime``) so we
    only touch fields explicitly typed as ``datetime``.
    """
    if annotation is datetime:
        return True
    if get_origin(annotation) in (Union, UnionType):
        return any(arg is datetime for arg in get_args(annotation))
    return False


def coerce_to_utc(value: Any) -> Any:
    """Promote a naive ``datetime`` (or naive ISO string) to UTC-aware.

    Returns *value* unchanged when it is already tz-aware, when it is
    ``None``, or when it is a string that cannot be parsed as a naive
    ISO datetime.
    """
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return value
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else value
    return value


def normalize_datetime_fields(
    resolved: dict[str, Any],
    params_type: type[msgspec.Struct],
) -> dict[str, Any]:
    """Promote naive datetime values to UTC-aware where the schema expects it.

    Args:
        resolved: Parameter mapping with placeholders already resolved.
        params_type: ``msgspec.Struct`` describing the expected field types.

    Returns:
        A new dict with naive datetime values promoted to UTC-aware; all
        other values pass through unchanged.
    """
    hints = get_type_hints(params_type)
    datetime_fields = {
        field.name
        for field in msgspec.structs.fields(params_type)
        if is_datetime_annotation(hints.get(field.name))
    }
    if not datetime_fields:
        return resolved
    return {
        key: coerce_to_utc(value) if key in datetime_fields else value
        for key, value in resolved.items()
    }


__all__ = [
    "coerce_to_utc",
    "is_datetime_annotation",
    "normalize_datetime_fields",
    "signature_from_params_type",
]
