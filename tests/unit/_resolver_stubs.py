"""Config resolver stubs shared by the factory ``resolvers=`` tests."""

from __future__ import annotations

from collections.abc import Mapping


class MappingResolver:
    """Resolve ``${<name>:key}`` placeholders from a fixed mapping."""

    def __init__(self, name: str, values: Mapping[str, object]) -> None:
        self._name = name
        self._values = dict(values)

    @property
    def name(self) -> str:
        return self._name

    def resolve(self, key: str) -> object:
        return self._values[key]
