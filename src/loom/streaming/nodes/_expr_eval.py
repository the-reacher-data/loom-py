"""Expression helpers for routing and terminal branching."""

from __future__ import annotations

from typing import TypeAlias, TypeVar

from loom.core.expr import ExprNode, PathRef, evaluate_expr
from loom.streaming.core._message import Message, StreamPayload
from loom.streaming.nodes._protocols import Predicate, Selector

PayloadT = TypeVar("PayloadT", bound=StreamPayload)

PredicateSpec: TypeAlias = ExprNode | Predicate[PayloadT]
SelectorSpec: TypeAlias = PathRef | Selector[PayloadT]


def select_value(selector: SelectorSpec[PayloadT], message: Message[PayloadT]) -> object:
    """Evaluate a selector for one message."""
    if isinstance(selector, PathRef):
        return evaluate_expr(selector, {"message": message, "payload": message.payload})
    return selector.select(message)


def evaluate_predicate(predicate: PredicateSpec[PayloadT], message: Message[PayloadT]) -> bool:
    """Evaluate a predicate for one message."""
    if isinstance(predicate, Predicate):
        return predicate.matches(message)
    return bool(evaluate_expr(predicate, {"message": message, "payload": message.payload}))


__all__ = ["PredicateSpec", "SelectorSpec", "evaluate_predicate", "select_value"]
