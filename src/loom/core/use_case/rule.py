from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from loom.core.command.base import Command
from loom.core.command.introspection import get_patch_fields
from loom.core.errors.errors import RuleViolation, RuleViolations
from loom.core.use_case.field_ref import FieldRef

# Re-exported for backward compatibility — canonical home is loom.core.errors.
__all__ = [
    "Rule",
    "RuleFn",
    "RuleViolation",
    "RuleViolations",
]

CommandT = TypeVar("CommandT", bound=Command)

RuleFn = Callable[[CommandT, frozenset[str]], None]


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
class _RuleSpec:
    evaluator: Callable[[Command, frozenset[str]], None]
    predicate: FieldRef | None = None

    def __call__(self, command: Command, fields_set: frozenset[str]) -> None:
        if self.predicate is not None and not _is_present(
            command, fields_set, self.predicate
        ):
            return
        self.evaluator(command, fields_set)


class _RuleWhenBuilder:
    __slots__ = ("_spec",)

    def __init__(self, spec: _RuleSpec) -> None:
        self._spec = spec

    def when_present(self, predicate: FieldRef) -> RuleFn[Any]:
        return _RuleSpec(
            evaluator=self._spec.evaluator,
            predicate=predicate,
        )

    def build(self) -> RuleFn[Any]:
        return self._spec


@dataclass(frozen=True, slots=True)
class _CheckEvaluator:
    targets: tuple[FieldRef, ...]
    validator: Callable[..., Any]

    def __call__(self, command: Command, fields_set: frozenset[str]) -> None:
        del fields_set
        values = tuple(_resolve_from_command(command, target) for target in self.targets)
        result = self.validator(*values)
        if isinstance(result, RuleViolation):
            raise result
        if isinstance(result, str):
            raise RuleViolation(self.targets[0].leaf, result)
        if result is False:
            raise RuleViolation(self.targets[0].leaf, "Rule check failed")


@dataclass(frozen=True, slots=True)
class _ForbidEvaluator:
    condition: Callable[[Command, frozenset[str]], bool]
    field: str
    message: str

    def __call__(self, command: Command, fields_set: frozenset[str]) -> None:
        if self.condition(command, fields_set):
            raise RuleViolation(self.field, self.message)


@dataclass(frozen=True, slots=True)
class _RequirePresentEvaluator:
    target: FieldRef
    field: str
    message: str

    def __call__(self, command: Command, fields_set: frozenset[str]) -> None:
        if not _is_present(command, fields_set, self.target):
            raise RuleViolation(self.field, self.message)


class Rule:
    """Rule DSL namespace."""

    @staticmethod
    def check(
        *targets: FieldRef,
        via: Callable[..., Any],
    ) -> _RuleWhenBuilder:
        if not targets:
            raise ValueError("Rule.check(...) requires at least one target.")
        evaluator = _CheckEvaluator(targets=targets, validator=via)
        return _RuleWhenBuilder(_RuleSpec(evaluator=evaluator))

    @staticmethod
    def forbid(
        condition: Callable[[Command, frozenset[str]], bool],
        *,
        field: str,
        message: str,
    ) -> _RuleWhenBuilder:
        evaluator = _ForbidEvaluator(condition=condition, field=field, message=message)
        return _RuleWhenBuilder(_RuleSpec(evaluator=evaluator))

    @staticmethod
    def require_present(
        target: FieldRef,
        *,
        field: str | None = None,
        message: str | None = None,
    ) -> RuleFn[Any]:
        resolved_field = field or target.leaf
        resolved_message = message or f"{target.leaf} is required"
        evaluator = _RequirePresentEvaluator(
            target=target,
            field=resolved_field,
            message=resolved_message,
        )
        return _RuleSpec(evaluator=evaluator)
