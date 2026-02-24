from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any

from loom.core.model.enums import OnDelete, ServerDefault


@dataclass(frozen=True, slots=True)
class ColumnType:
    """Backend column type descriptor stored inside ``Annotated[...]``."""

    type_name: str
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = dataclass_field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Field:
    """Column constraint metadata stored inside ``Annotated[...]``."""

    primary_key: bool = False
    unique: bool = False
    index: bool = False
    nullable: bool = False
    autoincrement: bool = False
    server_default: ServerDefault | None = None
    server_onupdate: str | None = None
    foreign_key: str | None = None
    on_delete: OnDelete | None = None
    default: Any = None
