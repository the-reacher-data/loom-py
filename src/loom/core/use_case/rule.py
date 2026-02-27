from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from loom.core.command.base import Command
from loom.core.command.introspection import get_patch_fields
from loom.core.errors.errors import RuleViolation, RuleViolations
from loom.core.use_case.field_ref import FieldExpr, FieldRef

# Re-exported for backward compatibility — canonical home is loom.core.errors.
__all__ = [
    "Rule",
    "RuleFn",
    "RuleViolation",
    "RuleViolations",
]

CommandT = TypeVar("CommandT", bound=Command)

RuleFn = Callable[..., None]


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
class _RuleSpec:
    evaluator: Callable[..., Any]
    field: str | None = None
    message: str | None = None
    command_sources: tuple[FieldRef, ...] = ()
    param_names: tuple[str, ...] = ()
    include_command: bool = False
    include_fields_set: bool = False
    predicate: FieldRef | FieldExpr | None = None

    def from_command(self, *sources: FieldRef) -> _RuleSpec:
        if not sources:
            # Full command mode: provide both command and fields_set.
            return _RuleSpec(
                evaluator=self.evaluator,
                field=self.field,
                message=self.message,
                command_sources=(),
                param_names=self.param_names,
                include_command=True,
                include_fields_set=True,
                predicate=self.predicate,
            )
        return _RuleSpec(
            evaluator=self.evaluator,
            field=self.field,
            message=self.message,
            command_sources=tuple(sources),
            param_names=self.param_names,
            include_command=False,
            include_fields_set=False,
            predicate=self.predicate,
        )

    def from_params(self, *names: str) -> _RuleSpec:
        if not names:
            raise ValueError("Rule.from_params(...) requires at least one parameter name.")
        return _RuleSpec(
            evaluator=self.evaluator,
            field=self.field,
            message=self.message,
            command_sources=self.command_sources,
            param_names=(*self.param_names, *names),
            include_command=self.include_command,
            include_fields_set=self.include_fields_set,
            predicate=self.predicate,
        )

    def when_present(self, predicate: FieldRef | FieldExpr) -> _RuleSpec:
        return _RuleSpec(
            evaluator=self.evaluator,
            field=self.field,
            message=self.message,
            command_sources=self.command_sources,
            param_names=self.param_names,
            include_command=self.include_command,
            include_fields_set=self.include_fields_set,
            predicate=predicate,
        )

    def __call__(
        self,
        command: Command,
        fields_set: frozenset[str],
        context: dict[str, object] | None = None,
    ) -> None:
        if self.predicate is not None and not _predicate_is_present(
            command, fields_set, self.predicate
        ):
            return

        args: list[Any] = []
        if self.include_command:
            args.append(command)
        if self.include_fields_set:
            args.append(fields_set)
        for source in self.command_sources:
            args.append(_resolve_from_command(command, source))

        if self.param_names:
            if context is None:
                raise ValueError("Rule.from_params(...) requires runtime context.")
            missing = [name for name in self.param_names if name not in context]
            if missing:
                missing_str = ", ".join(missing)
                raise ValueError(f"Missing runtime context keys for rule: {missing_str}")
            args.extend(context[name] for name in self.param_names)

        result = self.evaluator(*args)
        if isinstance(result, RuleViolation):
            raise result
        if isinstance(result, str):
            raise RuleViolation(self._resolved_field(command), result)
        if result is False:
            raise RuleViolation(self._resolved_field(command), self._resolved_message())
        if result is True:
            raise RuleViolation(self._resolved_field(command), self._resolved_message())

    def _resolved_field(self, command: Command) -> str:
        if self.field is not None:
            return self.field
        if self.command_sources:
            return self.command_sources[0].leaf
        return type(command).__name__

    def _resolved_message(self) -> str:
        return self.message or "Rule check failed"


@dataclass(frozen=True, slots=True)
class _RequirePresentSpec:
    target: FieldRef
    field: str
    message: str
    predicate: FieldRef | FieldExpr | None = None

    def when_present(self, predicate: FieldRef | FieldExpr) -> _RequirePresentSpec:
        return _RequirePresentSpec(
            target=self.target,
            field=self.field,
            message=self.message,
            predicate=predicate,
        )

    def __call__(
        self,
        command: Command,
        fields_set: frozenset[str],
        context: dict[str, object] | None = None,
    ) -> None:
        del context
        if self.predicate is not None and not _predicate_is_present(
            command, fields_set, self.predicate
        ):
            return
        if not _is_present(command, fields_set, self.target):
            raise RuleViolation(self.field, self.message)


class Rule:
    """Rule DSL namespace."""

    @staticmethod
    def check(
        *targets: FieldRef,
        via: Callable[..., Any],
        message: str | None = None,
    ) -> RuleFn:
        if not targets:
            raise ValueError("Rule.check(...) requires at least one target.")
        return _RuleSpec(
            evaluator=via,
            field=targets[0].leaf,
            message=message,
            command_sources=tuple(targets),
        )

    @staticmethod
    def forbid(
        condition: Callable[..., bool],
        *,
        field: str | FieldRef | None = None,
        message: str,
    ) -> RuleFn:
        resolved_field = field.leaf if isinstance(field, FieldRef) else field
        return _RuleSpec(
            evaluator=condition,
            field=resolved_field,
            message=message,
            include_command=True,
            include_fields_set=True,
        )

    @staticmethod
    def require_present(
        target: FieldRef,
        *,
        field: str | None = None,
        message: str | None = None,
    ) -> RuleFn:
        resolved_field = field or target.leaf
        resolved_message = message or f"{target.leaf} is required"
        return _RequirePresentSpec(
            target=target,
            field=resolved_field,
            message=resolved_message,
        )
