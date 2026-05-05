"""Framework-wide base struct types."""

from __future__ import annotations

import msgspec


class _MessageTypeContractMixin:
    """Shared contract-name helper for Loom struct types."""

    @classmethod
    def loom_message_type(cls) -> str:
        """Return the stable logical message type for this model class."""
        override = getattr(cls, "__loom_message_type__", None)
        if override is not None:
            return str(override)
        return f"{cls.__module__}.{cls.__qualname__}"


class LoomStruct(_MessageTypeContractMixin, msgspec.Struct):
    """Base struct for Loom logical data types.

    ``LoomStruct`` is intentionally neutral. Specialised framework types such
    as responses or persistence models define their own serialisation
    behaviour.
    """


class LoomFrozenStruct(_MessageTypeContractMixin, msgspec.Struct, frozen=True):
    """Base immutable struct for Loom value contracts.

    Use this for configuration, DSL declarations, wire envelopes, and other
    value objects that should not expose mutable assignment after creation.
    """
