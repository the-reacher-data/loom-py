from __future__ import annotations

from typing import (
    Annotated,
    Any,
    ClassVar,
    Self,
    get_args,
    get_origin,
    get_type_hints,
)

import msgspec

from loom.core.command.field import CommandField


class Command(msgspec.Struct, frozen=True, kw_only=True, omit_defaults=True):
    """Base for all command structs."""

    __command_fields__: ClassVar[dict[str, CommandField]] = {}
    __patch__: ClassVar[bool] = False

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls.__command_fields__ = _extract_command_fields(cls)

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> tuple[Self, frozenset[str]]:
        normalized = dict(payload)
        struct_fields = {field.name: field for field in msgspec.structs.fields(cls)}
        fields_set = frozenset(normalized.keys() & struct_fields.keys())

        for name, meta in cls.__command_fields__.items():
            if name in normalized:
                continue
            field = struct_fields.get(name)
            if field is None:
                continue
            if field.default is msgspec.NODEFAULT and (
                meta.patch or meta.internal or meta.calculated
            ):
                normalized[name] = None

        instance = msgspec.convert(normalized, cls)
        return instance, fields_set


def _extract_command_fields(cls: type[Any]) -> dict[str, CommandField]:
    result: dict[str, CommandField] = {}
    type_hints = get_type_hints(cls, include_extras=True)
    for name, annotation in type_hints.items():
        if get_origin(annotation) is not Annotated:
            continue
        args = get_args(annotation)
        for metadata in args[1:]:
            if isinstance(metadata, CommandField):
                result[name] = metadata
                break
    return result
