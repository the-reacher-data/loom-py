from __future__ import annotations

from loom.core.command.field import CommandField


def get_command_fields(cls: type) -> dict[str, CommandField]:
    """Return all ``CommandField`` descriptors declared on a command class.

    Args:
        cls: A Command subclass.

    Returns:
        Mapping of field name to ``CommandField`` metadata.
    """
    return dict(getattr(cls, "__command_fields__", {}))


def get_input_fields(cls: type) -> dict[str, CommandField]:
    """Return ``CommandField`` descriptors that are user-supplied.

    Excludes fields marked as ``internal`` or ``calculated``.

    Args:
        cls: A Command subclass.

    Returns:
        Mapping of field name to ``CommandField`` metadata.
    """
    return {
        name: cf
        for name, cf in get_command_fields(cls).items()
        if not cf.internal and not cf.calculated
    }


def get_calculated_fields(cls: type) -> dict[str, CommandField]:
    """Return ``CommandField`` descriptors marked as ``calculated``.

    Args:
        cls: A Command subclass.

    Returns:
        Mapping of field name to ``CommandField`` metadata.
    """
    return {
        name: cf
        for name, cf in get_command_fields(cls).items()
        if cf.calculated
    }


def get_internal_fields(cls: type) -> dict[str, CommandField]:
    """Return ``CommandField`` descriptors marked as ``internal``.

    Args:
        cls: A Command subclass.

    Returns:
        Mapping of field name to ``CommandField`` metadata.
    """
    return {
        name: cf
        for name, cf in get_command_fields(cls).items()
        if cf.internal
    }


def get_patch_fields(cls: type) -> dict[str, CommandField]:
    """Return ``CommandField`` descriptors marked as ``patch``."""
    return {
        name: cf
        for name, cf in get_command_fields(cls).items()
        if cf.patch
    }


def is_patch_command(cls: type) -> bool:
    """Return whether the command class is a patch command.

    Args:
        cls: A Command subclass.

    Returns:
        True when at least one command field is marked as patch.
    """
    return bool(get_patch_fields(cls))
