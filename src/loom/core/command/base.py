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


class Command(msgspec.Struct, frozen=True, kw_only=True, omit_defaults=True, rename="camel"):
    """Base for all command structs."""

    __command_fields__: ClassVar[dict[str, CommandField]] = {}
    __patch__: ClassVar[bool] = False

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls.__command_fields__ = _extract_command_fields(cls)

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> tuple[Self, frozenset[str]]:
        struct_fields = {field.name: field for field in msgspec.structs.fields(cls)}
        internal_to_external, external_to_internal = _build_field_mappings(struct_fields)
        normalized, seen_internal_keys = _normalize_payload(
            payload=payload,
            struct_fields=struct_fields,
            internal_to_external=internal_to_external,
            external_to_internal=external_to_internal,
        )
        fields_set = frozenset(seen_internal_keys)

        _fill_metadata_required_defaults(
            normalized=normalized,
            seen_internal_keys=seen_internal_keys,
            command_fields=cls.__command_fields__,
            struct_fields=struct_fields,
            internal_to_external=internal_to_external,
        )

        instance = msgspec.convert(normalized, cls)
        return instance, fields_set


def _build_field_mappings(
    struct_fields: dict[str, msgspec.structs.FieldInfo],
) -> tuple[dict[str, str], dict[str, str]]:
    internal_to_external = {
        name: field.encode_name or name for name, field in struct_fields.items()
    }
    external_to_internal = {
        field.encode_name: field.name
        for field in struct_fields.values()
        if field.encode_name and field.encode_name != field.name
    }
    return internal_to_external, external_to_internal


def _normalize_payload(
    *,
    payload: dict[str, Any],
    struct_fields: dict[str, msgspec.structs.FieldInfo],
    internal_to_external: dict[str, str],
    external_to_internal: dict[str, str],
) -> tuple[dict[str, Any], set[str]]:
    normalized: dict[str, Any] = {}
    seen_internal_keys: set[str] = set()

    for raw_key, value in payload.items():
        if raw_key in external_to_internal:
            normalized[raw_key] = value
            seen_internal_keys.add(external_to_internal[raw_key])
            continue
        if raw_key in struct_fields:
            normalized[internal_to_external[raw_key]] = value
            seen_internal_keys.add(raw_key)
            continue
        normalized[raw_key] = value

    return normalized, seen_internal_keys


def _fill_metadata_required_defaults(
    *,
    normalized: dict[str, Any],
    seen_internal_keys: set[str],
    command_fields: dict[str, CommandField],
    struct_fields: dict[str, msgspec.structs.FieldInfo],
    internal_to_external: dict[str, str],
) -> None:
    for name, meta in command_fields.items():
        if name in seen_internal_keys:
            continue
        field = struct_fields.get(name)
        if field is None:
            continue
        if field.default is not msgspec.NODEFAULT:
            continue
        if not (meta.patch or meta.internal or meta.calculated):
            continue
        normalized[internal_to_external[name]] = None


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
