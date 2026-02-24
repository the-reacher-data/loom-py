from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class Projection:
    """Derived-field metadata assigned as a class attribute on a ``BaseModel``."""

    loader: Any
    profiles: tuple[str, ...] = ("default",)
    depends_on: tuple[str, ...] = ()
    default: Any = None
