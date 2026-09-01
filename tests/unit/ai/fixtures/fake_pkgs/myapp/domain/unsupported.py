"""Importable symbols that are NOT valid ``type_ref`` targets.

The compiler accepts ``msgspec.Struct`` subclasses only; everything here
resolves fine but must be rejected with ``OUTPUT_TYPE_REF_UNSUPPORTED``.
"""

from __future__ import annotations

from typing import Any

import msgspec


class PlainModel:
    """Pydantic-like plain class: importable, but not a ``msgspec.Struct``."""

    issuer: str = ""


NOT_A_TYPE: dict[str, Any] = {"type": "object"}
"""A value that is not a type at all."""


class LaxStruct(msgspec.Struct, frozen=True, kw_only=True):
    """A ``msgspec.Struct`` without ``forbid_unknown_fields``: not strict.

    Pass-through of the validated bytes is only safe under a strict decode,
    so the compiler must reject this type with ``OUTPUT_TYPE_REF_UNSUPPORTED``.
    """

    issuer: str = ""
