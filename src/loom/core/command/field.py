from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any, TypeAlias, TypeVar

import msgspec

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class CommandField:
    """Metadata descriptor for command fields.

    Args:
        calculated: Field is computed by the engine, not user-supplied.
        internal: Field is set by infrastructure, not user-supplied.
        patch: Field supports patch semantics (missing vs explicit null/value).
        default: Optional field default.
    """

    calculated: bool = False
    internal: bool = False
    patch: bool = False
    default: Any = msgspec.UNSET


Patch: TypeAlias = Annotated[
    T | None,
    CommandField(patch=True),
]
Internal: TypeAlias = Annotated[
    T | None,
    CommandField(internal=True),
]
Computed: TypeAlias = Annotated[
    T | None,
    CommandField(calculated=True),
]
