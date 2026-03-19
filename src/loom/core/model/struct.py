"""Framework-wide base struct types."""

from __future__ import annotations

import msgspec


class LoomStruct(msgspec.Struct):
    """Base struct for Loom logical data types.

    ``LoomStruct`` is intentionally neutral. Specialised framework types such
    as responses or persistence models define their own serialisation
    behaviour.
    """
