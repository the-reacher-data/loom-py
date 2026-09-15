"""Loom-owned boundary type: one contract over msgspec and pydantic.

A :class:`LoomType` is what every boundary (agent output, agent state, cache,
REST) carries once it has been compiled: it validates JSON strictly, builds the
typed value in the same pass and turns a value back into JSON-mode builtins.
The two implementations are private to this module, so no consumer branches on
the library at run time and no other module calls ``msgspec.json.Decoder`` or
``pydantic.TypeAdapter`` on a boundary value.

Pydantic stays optional: the factory only looks at ``sys.modules`` and imports
``pydantic`` locally, so a process that never imported it keeps working with
msgspec alone. That is also why the pydantic implementation receives its model
class untyped: this module never names ``pydantic.BaseModel``, and loom's own
``loom.core.model.BaseModel`` lives next door.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Mapping
from typing import Any, Literal, Protocol

import msgspec

from loom.core.errors.errors import LoomError

_LAX_TYPE_REASON = (
    "the type must reject unknown fields: forbid_unknown_fields=True or "
    "model_config['extra'] == 'forbid' (invariant 5)"
)
_NOT_A_MODEL_REASON = "only msgspec.Struct or pydantic.BaseModel subclasses are supported"


class LoomType(Protocol):
    """Boundary contract a compiled type exposes to its consumers.

    Attributes:
        type: The wrapped class or annotation, kept for introspection.
        library: Closed tag naming the implementation behind the value.
    """

    type: type
    library: Literal["msgspec", "pydantic"]

    def schema(self) -> Mapping[str, Any]:
        """Return the JSON Schema of the wrapped type."""
        ...

    def decode_json(self, data: bytes | str, /) -> Any:
        """Validate ``data`` strictly and build the typed value in one pass.

        Raises:
            BoundaryValidationError: ``data`` is malformed JSON or does not
                satisfy the type.
        """
        ...

    def to_builtins(self, obj: Any, /) -> Any:
        """Turn a typed value into JSON-mode builtins."""
        ...


class BoundaryValidationError(LoomError):
    """A boundary value failed validation.

    Raised by every :class:`LoomType` implementation with the library error as
    ``__cause__``. The message is one line, names the field path and never
    echoes the input or a documentation URL.

    Args:
        message: One-line description of the failure.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message, code="boundary_validation")


class UnsupportedBoundaryType(LoomError):
    """A symbol cannot be compiled into a :class:`LoomType`.

    Args:
        reason: Why the symbol is rejected, shared by every boundary's issue.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason, code="boundary_type_unsupported")


def loom_type(symbol: type) -> LoomType:
    """Compile an authored class into a :class:`LoomType`.

    The library is decided once, by ``issubclass``, and strictness is checked
    here for both: a ``msgspec.Struct`` must declare
    ``forbid_unknown_fields=True`` and a ``pydantic.BaseModel`` must resolve
    ``model_config["extra"] == "forbid"``.

    Args:
        symbol: A strict ``msgspec.Struct`` or strict ``pydantic.BaseModel``
            subclass.

    Returns:
        The boundary type wrapping ``symbol``.

    Raises:
        UnsupportedBoundaryType: ``symbol`` is neither library's model class or
            does not reject unknown fields.
    """
    if not isinstance(symbol, type):
        raise UnsupportedBoundaryType(_NOT_A_MODEL_REASON)
    if issubclass(symbol, msgspec.Struct):
        _require_strict(symbol)
        return _MsgspecType(symbol)
    if _is_pydantic_model(symbol):
        _require_strict(symbol)
        return _PydanticType(symbol)
    raise UnsupportedBoundaryType(_NOT_A_MODEL_REASON)


def msgspec_type(annotation: Any) -> LoomType:
    """Wrap a compiler-generated annotation in a msgspec-backed :class:`LoomType`.

    Accepts whatever ``msgspec.json.Decoder`` accepts: a ``defstruct`` result,
    ``dict[str, Any]``, ``list[...]`` or a scalar. Strictness is not checked
    because the annotation is not authored by a user.

    Args:
        annotation: Any annotation ``msgspec.json.Decoder`` can build.

    Returns:
        The boundary type wrapping ``annotation``.
    """
    return _MsgspecType(annotation)


def _require_strict(symbol: type) -> None:
    if not _is_strict(symbol):
        raise UnsupportedBoundaryType(_LAX_TYPE_REASON)


def _is_strict(symbol: type) -> bool:
    if issubclass(symbol, msgspec.Struct):
        return symbol.__struct_config__.forbid_unknown_fields
    config: Mapping[str, Any] = getattr(symbol, "model_config", {})
    return config.get("extra") == "forbid"


def _is_pydantic_model(symbol: type) -> bool:
    pydantic = sys.modules.get("pydantic")
    return pydantic is not None and issubclass(symbol, pydantic.BaseModel)


class _MsgspecType:
    def __init__(self, annotation: Any) -> None:
        self.type: Any = annotation
        self.library: Literal["msgspec", "pydantic"] = "msgspec"
        self._decoder: msgspec.json.Decoder[Any] = msgspec.json.Decoder(annotation)

    def schema(self) -> Mapping[str, Any]:
        return msgspec.json.schema(self.type)

    def decode_json(self, data: bytes | str, /) -> Any:
        try:
            return self._decoder.decode(data)
        except (msgspec.ValidationError, msgspec.DecodeError) as exc:
            raise BoundaryValidationError(str(exc)) from exc

    def to_builtins(self, obj: Any, /) -> Any:
        return msgspec.to_builtins(obj)


class _PydanticType:
    def __init__(self, symbol: Any) -> None:
        from pydantic import TypeAdapter, ValidationError

        self.type: type = symbol
        self.library: Literal["msgspec", "pydantic"] = "pydantic"
        self._json_schema: Callable[[], dict[str, Any]] = symbol.model_json_schema
        self._adapter: TypeAdapter[Any] = TypeAdapter(symbol)
        self._validation_error: type[ValidationError] = ValidationError

    def schema(self) -> Mapping[str, Any]:
        schema = self._json_schema()
        schema.pop("title", None)
        return schema

    def decode_json(self, data: bytes | str, /) -> Any:
        try:
            return self._adapter.validate_json(data, strict=True)
        except self._validation_error as exc:
            raise BoundaryValidationError(_pydantic_message(exc)) from exc

    def to_builtins(self, obj: Any, /) -> Any:
        return obj.model_dump(mode="json", by_alias=True)


def _pydantic_message(exc: Any) -> str:
    return "; ".join(
        f"{'.'.join(map(str, error['loc'])) or '$'}: {error['msg']}"
        for error in exc.errors(include_url=False, include_input=False)
    )
