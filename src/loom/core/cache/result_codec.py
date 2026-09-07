"""Result codecs for repository reads marked with ``@cache_query``.

A cached custom read has to answer with the same type whether the value came
from the repository or from the cache. The stored payload is builtins, so
without a codec the first call returns whatever the method built and every
later call returns the decoded payload — a struct on the miss, a ``dict`` on
the hit.

The codec is derived from the method's declared return annotation and applied
on both paths, so the two are indistinguishable and a value that does not
survive the round trip fails on the very first call. The supported grammar is
a :class:`msgspec.Struct`, a scalar, or a ``list``, ``tuple`` or optional of
those; ``tuple`` is restored as a tuple, which builtins conversion flattens
into a list. An annotation outside the grammar has no codec and the caller
falls back to :class:`PassthroughResultCodec`.

The lenient builtins rendering every cache payload goes through lives here
too, because the pass-through codec is its main caller.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, time
from decimal import Decimal
from types import GenericAlias, NoneType, UnionType
from typing import Any, NamedTuple, Protocol, TypeVar, Union, get_args, get_origin
from uuid import UUID

import msgspec

from loom.core.model.introspection import resolve_type_hints

_SCALAR_TYPES: frozenset[type] = frozenset(
    {bool, int, float, str, bytes, UUID, datetime, date, time, Decimal}
)
_UNION_ORIGINS: frozenset[Any] = frozenset({Union, UnionType})


def to_payload(value: Any) -> Any:
    """Render *value* as builtins, leaving what it cannot describe untouched.

    Unlike :func:`msgspec.to_builtins` this never raises: a value outside the
    struct, sequence and mapping shapes it knows is returned as it is, and the
    backend serializer decides whether it can store it. That leniency is what
    lets an unannotated cached read keep working.

    Args:
        value: Value on its way to the cache backend.

    Returns:
        The builtins rendering of *value*.
    """
    if isinstance(value, msgspec.Struct):
        return msgspec.to_builtins(value)
    if isinstance(value, list | tuple):
        return [to_payload(item) for item in value]
    if isinstance(value, dict):
        return {str(key): to_payload(item) for key, item in value.items()}
    return value


class EncodedResult(NamedTuple):
    """A freshly loaded result, split into what to store and what to return.

    Attributes:
        payload: Builtins form written to the cache backend.
        value: Value handed back to the caller of the cached read.
    """

    payload: Any
    value: Any


class ResultCodec(Protocol):
    """Translation between a cached read's payload and its declared type."""

    def encode(self, result: Any) -> EncodedResult:
        """Split a freshly loaded *result* into its payload and return value."""
        ...

    def decode(self, payload: Any) -> Any:
        """Rebuild the return value from a *payload* read out of the cache."""
        ...


class PassthroughResultCodec:
    """Codec for a read whose declared type cannot drive a conversion.

    It reproduces the behaviour that predates typed codecs: the loaded result
    is returned untouched and a cached payload is returned as it was decoded.
    Hit and miss can therefore disagree, which is why the wrapper deprecates
    the methods that land here.
    """

    __slots__ = ()

    def encode(self, result: Any) -> EncodedResult:
        """Store *result* as builtins and return it unchanged."""
        return EncodedResult(to_payload(result), result)

    def decode(self, payload: Any) -> Any:
        """Return *payload* as the backend decoded it."""
        return payload


class _TypedResultCodec:
    """Codec that converts both paths to one resolved return type."""

    __slots__ = ("_return_type",)

    def __init__(self, return_type: Any) -> None:
        """Bind the codec to an already resolved return type.

        Args:
            return_type: Type inside the supported grammar, with every type
                variable already substituted.
        """
        self._return_type = return_type

    def encode(self, result: Any) -> EncodedResult:
        """Render *result* as builtins and decode it back.

        Returning the decoded value rather than *result* is what makes a miss
        and a hit indistinguishable, and it surfaces a result that cannot be
        stored on the first call instead of the second.

        Raises:
            TypeError: The result cannot be rendered as builtins at all.
            msgspec.ValidationError: The rendered result does not match the
                declared return type.
        """
        payload = msgspec.to_builtins(result)
        return EncodedResult(payload, self.decode(payload))

    def decode(self, payload: Any) -> Any:
        """Convert *payload* to the declared return type.

        Raises:
            msgspec.ValidationError: The payload does not match the declared
                return type.
        """
        return msgspec.convert(payload, self._return_type)


def build_result_codec(method: Callable[..., Any], *, model: object | None) -> ResultCodec | None:
    """Build the codec declared by the return annotation of *method*.

    The annotation is resolved against the module that defines the method, so
    a string annotation left by ``from __future__ import annotations`` and a
    forward reference resolvable from there both work. A type variable is
    substituted by *model*, mirroring how the wrapper rebuilds an entity read
    from its cached payload.

    Args:
        method: Function decorated with ``@cache_query``, taken from the class
            dictionary rather than from an instance.
        model: Model of the repository owning the method, used to resolve a
            type variable in the annotation.

    Returns:
        The codec for the declared type, or ``None`` when the annotation is
        missing, unresolvable or outside the supported grammar.
    """
    annotation = resolve_type_hints(method).get("return")
    if annotation is None:
        return None
    return_type = _resolve_return_type(annotation, model)
    if return_type is None:
        return None
    return _TypedResultCodec(return_type)


def _resolve_return_type(annotation: Any, model: object | None) -> Any | None:
    optional = _resolve_optional(annotation, model)
    if optional is not None:
        return optional
    return _resolve_concrete(annotation, model)


def _resolve_optional(annotation: Any, model: object | None) -> Any | None:
    if get_origin(annotation) not in _UNION_ORIGINS:
        return None
    members = get_args(annotation)
    if len(members) != 2 or NoneType not in members:
        return None
    declared = next(member for member in members if member is not NoneType)
    inner = _resolve_concrete(declared, model)
    if inner is None:
        return None
    return inner | None


def _resolve_concrete(annotation: Any, model: object | None) -> Any | None:
    origin = get_origin(annotation)
    if origin is list:
        return _resolve_list(get_args(annotation), model)
    if origin is tuple:
        return _resolve_tuple(get_args(annotation), model)
    return _resolve_element(annotation, model)


def _resolve_list(args: tuple[Any, ...], model: object | None) -> Any | None:
    if len(args) != 1:
        return None
    element = _resolve_element(args[0], model)
    if element is None:
        return None
    return GenericAlias(list, (element,))


def _resolve_tuple(args: tuple[Any, ...], model: object | None) -> Any | None:
    if not args:
        return None
    if len(args) == 2 and args[1] is Ellipsis:
        element = _resolve_element(args[0], model)
        return None if element is None else GenericAlias(tuple, (element, Ellipsis))
    elements = [_resolve_element(arg, model) for arg in args]
    if any(element is None for element in elements):
        return None
    return GenericAlias(tuple, tuple(elements))


def _resolve_element(annotation: Any, model: object | None) -> type | None:
    declared = model if isinstance(annotation, TypeVar) else annotation
    if not isinstance(declared, type):
        return None
    if issubclass(declared, msgspec.Struct):
        return declared
    if declared in _SCALAR_TYPES:
        return declared
    return None
