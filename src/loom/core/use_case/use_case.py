from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Generic, TypeVar

from loom.core.command.base import Command
from loom.core.use_case.compute import ComputeFn
from loom.core.use_case.rule import RuleFn, RuleViolation, RuleViolations

CommandT = TypeVar("CommandT", bound=Command)
ResultT = TypeVar("ResultT")


class UseCase(Generic[CommandT, ResultT]):
    """Orchestrates the command pipeline: compute -> rules -> execute.

    Rules are evaluated exhaustively — all violations are accumulated
    and raised together as ``RuleViolations``.

    Args:
        execute: Async callable that performs the actual operation.
        computes: Sequence of compute functions applied in order.
        rules: Sequence of validation rules applied in order.

    Example::

        use_case = UseCase(
            execute=create_user_handler,
            computes=[set_created_at],
            rules=[email_not_disposable],
        )
        result = await use_case(command, fields_set)
    """

    def __init__(
        self,
        execute: Callable[[CommandT, frozenset[str]], Awaitable[ResultT]],
        computes: Sequence[ComputeFn[CommandT]] = (),
        rules: Sequence[RuleFn[CommandT]] = (),
    ) -> None:
        self._execute = execute
        self._computes = computes
        self._rules = rules

    async def __call__(
        self, command: CommandT, fields_set: frozenset[str]
    ) -> ResultT:
        """Run the full pipeline: computes, then rules, then execute.

        Args:
            command: The command to process.
            fields_set: Fields explicitly provided in the original payload.

        Returns:
            The result produced by the execute callable.

        Raises:
            RuleViolations: If any rules fail validation.
        """
        for compute in self._computes:
            command = compute(command, fields_set)

        violations: list[RuleViolation] = []
        for rule in self._rules:
            try:
                rule(command, fields_set)
            except RuleViolation as exc:
                violations.append(exc)
        if violations:
            raise RuleViolations(violations)

        return await self._execute(command, fields_set)
