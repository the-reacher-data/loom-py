from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

import msgspec

from loom.core.command.base import Command
from loom.core.command.introspection import get_patch_fields
from loom.core.use_case.field_ref import FieldExpr, FieldRef

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


def _predicate_is_present(
    command: Command,
    fields_set: frozenset[str],
    predicate: FieldRef | FieldExpr,
) -> bool:
    if isinstance(predicate, FieldRef):
        return _is_present(command, fields_set, predicate)
    if predicate.op == "or":
        return _predicate_is_present(command, fields_set, predicate.left) or _predicate_is_present(
            command, fields_set, predicate.right
        )
    if predicate.op == "and":
        return _predicate_is_present(command, fields_set, predicate.left) and _predicate_is_present(
            command, fields_set, predicate.right
        )
    raise ValueError(f"Unsupported predicate op: {predicate.op}")


@dataclass(frozen=True, slots=True)
class _ComputeSpec:
    target: FieldRef
    command_sources: tuple[FieldRef, ...] = ()
    param_names: tuple[str, ...] = ()
    include_command: bool = False
    via: Callable[..., Any] | None = None
    predicate: FieldRef | FieldExpr | None = None

    def from_params(self, *names: str) -> _ComputeSpec:
        if not names:
            raise ValueError("Compute.from_params(...) requires at least one param name.")
        return _ComputeSpec(
            target=self.target,
            command_sources=self.command_sources,
            param_names=(*self.param_names, *names),
            include_command=self.include_command,
            via=self.via,
            predicate=self.predicate,
        )

    def when_present(self, predicate: FieldRef | FieldExpr) -> _ComputeSpec:
        return _ComputeSpec(
            target=self.target,
            command_sources=self.command_sources,
            param_names=self.param_names,
            include_command=self.include_command,
            via=self.via,
            predicate=predicate,
        )

    def __call__(
        self,
        command: Command,
        fields_set: frozenset[str],
        context: dict[str, object] | None = None,
    ) -> Command:
        if self.predicate is not None and not _predicate_is_present(
            command, fields_set, self.predicate
        ):
            return command

        args: list[Any] = []
        if self.include_command:
            args.append(command)
        for source in self.command_sources:
            args.append(_resolve_from_command(command, source))

        if self.param_names:
            if context is None:
                raise ValueError("Compute.from_params(...) requires runtime context.")
            missing = [name for name in self.param_names if name not in context]
            if missing:
                missing_str = ", ".join(missing)
                raise ValueError(f"Missing runtime context keys for compute: {missing_str}")
            args.extend(context[name] for name in self.param_names)

        if self.via is None:
            if len(args) != 1:
                raise ValueError(
                    "Compute without via requires exactly one source. "
                    "Use from_command(F(...)) or provide via=..."
                )
            next_value = args[0]
        else:
            next_value = self.via(*args)

        return msgspec.structs.replace(command, **{self.target.leaf: next_value})


class _ComputeSetBuilder:
    __slots__ = ("_target",)

    def __init__(self, target: FieldRef) -> None:
        self._target = target

    def from_command(
        self,
        *sources: FieldRef,
        via: Callable[..., Any] | None = None,
    ) -> _ComputeSpec:
        if not sources:
            if via is None:
                raise ValueError(
                    "Compute.from_command() without fields requires via=... "
                    "to derive the target value."
                )
            return _ComputeSpec(
                target=self._target,
                include_command=True,
                via=via,
            )

        if len(sources) > 1 and via is None:
            raise ValueError("A via callable is required when multiple command fields are used.")

        return _ComputeSpec(
            target=self._target,
            command_sources=tuple(sources),
            via=via,
        )

    def from_fields(
        self,
        *sources: FieldRef,
        via: Callable[..., Any] | None = None,
    ) -> _ComputeSpec:
        """Backward-compatible alias for from_command(...)."""
        return self.from_command(*sources, via=via)


class Compute:
    """Compute DSL namespace.

    Example:
        ``Compute.set(F(UpdateUser).slug).from_fields(F(UpdateUser).name, via=slugify)``
    """

    @staticmethod
    def set(target: FieldRef) -> _ComputeSetBuilder:
        return _ComputeSetBuilder(target)
