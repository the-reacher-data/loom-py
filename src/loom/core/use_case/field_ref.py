from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from loom.core.command.base import Command


@dataclass(frozen=True, slots=True)
class FieldRef:
    """Declarative reference to a field path on a command (or loaded alias)."""

    root: type[Command] | str
    path: tuple[str, ...]

    def __getattr__(self, item: str) -> FieldRef:
        if item.startswith("_"):
            raise AttributeError(item)
        return FieldRef(root=self.root, path=(*self.path, item))

    @property
    def leaf(self) -> str:
        return self.path[-1]


class _FieldRefFactory:
    __slots__ = ("_root",)

    def __init__(self, root: type[Command] | str) -> None:
        self._root = root

    def __getattr__(self, item: str) -> FieldRef:
        if item.startswith("_"):
            raise AttributeError(item)
        return FieldRef(root=self._root, path=(item,))


def F(root: type[Command] | str) -> Any:
    """Build a typed field-reference factory.

    Example:
        ``F(UpdateUserCommand).birthdate``
    """
    return _FieldRefFactory(root)

