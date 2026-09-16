"""Importable symbols that are NOT valid ``type_ref`` targets.

The compiler accepts a strict ``msgspec.Struct`` or a strict
``pydantic.BaseModel`` subclass; everything here resolves fine but must be
rejected with ``OUTPUT_TYPE_REF_UNSUPPORTED``.
"""

from __future__ import annotations

from typing import Any

import msgspec
from pydantic import BaseModel


class PlainModel:
    """Plain class: importable, but neither a ``msgspec.Struct`` nor a ``BaseModel``."""

    issuer: str = ""


NOT_A_TYPE: dict[str, Any] = {"type": "object"}
"""A value that is not a type at all."""


class LaxStruct(msgspec.Struct, frozen=True, kw_only=True):
    """A ``msgspec.Struct`` without ``forbid_unknown_fields``: not strict.

    Pass-through of the validated bytes is only safe under a strict decode,
    so the compiler must reject this type with ``OUTPUT_TYPE_REF_UNSUPPORTED``.
    """

    issuer: str = ""


class LaxModel(BaseModel):
    """A pydantic model that does not forbid extra fields: not strict either.

    Pass-through of the validated bytes is only safe under a strict decode,
    so the compiler must reject this type with ``OUTPUT_TYPE_REF_UNSUPPORTED``.
    """

    issuer: str = ""
