from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

import msgspec

from loom.core.command.base import Command
from loom.core.command.introspection import get_patch_fields
from loom.core.use_case.field_ref import FieldRef

CommandT = TypeVar("CommandT", bound=Command)


ComputeFn = Callable[[CommandT, frozenset[str]], CommandT]


def _resolve_from_command(command: Command, ref: FieldRef) -> Any:
    current: Any = command
    for item in ref.path:
        current = getattr(current, item)
    return current


def _is_present(command: Command, fields_set: frozenset[str], ref: FieldRef) -> bool:
    if (
        isinstance(ref.root, type)
        and issubclass(ref.root, Command)
        and ref.leaf in get_patch_fields(ref.root)
    ):
        return ref.leaf in fields_set
    return _resolve_from_command(command, ref) is not None


@dataclass(frozen=True, slots=True)
class _ComputeSpec:
    target: FieldRef
    sources: tuple[FieldRef, ...]
    via: Callable[..., Any] | None
    predicate: FieldRef | None = None

    def __call__(self, command: Command, fields_set: frozenset[str]) -> Command:
        if self.predicate is not None and not _is_present(
            command, fields_set, self.predicate
        ):
            return command

        values = tuple(_resolve_from_command(command, source) for source in self.sources)
        next_value = values[0] if self.via is None else self.via(*values)
        return msgspec.structs.replace(command, **{self.target.leaf: next_value})


class _ComputeWhenBuilder:
    __slots__ = ("_spec",)

    def __init__(self, spec: _ComputeSpec) -> None:
        self._spec = spec

    def when_present(self, predicate: FieldRef) -> ComputeFn[Any]:
        return _ComputeSpec(
            target=self._spec.target,
            sources=self._spec.sources,
            via=self._spec.via,
            predicate=predicate,
        )

    def build(self) -> ComputeFn[Any]:
        return self._spec


class _ComputeSetBuilder:
    __slots__ = ("_target",)

    def __init__(self, target: FieldRef) -> None:
        self._target = target

    def from_fields(
        self,
        *sources: FieldRef,
        via: Callable[..., Any] | None = None,
    ) -> _ComputeWhenBuilder:
        if not sources:
            raise ValueError(
                "Compute.set(...).from_fields(...) requires at least one source."
            )
        if len(sources) > 1 and via is None:
            raise ValueError(
                "A 'via' callable is required when multiple sources are provided."
            )
        return _ComputeWhenBuilder(
            _ComputeSpec(
                target=self._target,
                sources=tuple(sources),
                via=via,
            )
        )


class Compute:
    """Compute DSL namespace.

    Example:
        ``Compute.set(F(UpdateUser).slug).from_fields(F(UpdateUser).name, via=slugify)``
    """

    @staticmethod
    def set(target: FieldRef) -> _ComputeSetBuilder:
        return _ComputeSetBuilder(target)
