from __future__ import annotations

from loom.core.command import Command


class CreateCategory(Command, frozen=True):
    name: str
