"""Graph-level fork declarations for terminal branching flows."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from loom.core.expr import ExprNode, PathRef, evaluate_expr
from loom.core.model import LoomFrozenStruct
from loom.streaming.core._message import Message, StreamPayload
from loom.streaming.nodes._protocols import Predicate, Selector

if TYPE_CHECKING:
    from loom.streaming.graph._flow import Process

InT = TypeVar("InT", bound=StreamPayload)

ForkPredicateSpec = ExprNode | Predicate[InT]
ForkSelectorSpec = PathRef | Selector[InT]


class ForkKind(StrEnum):
    """Routing family for a terminal fork declaration."""

    KEYED = "keyed"
    PREDICATE = "predicate"


class ForkRoute(LoomFrozenStruct, Generic[InT], frozen=True):
    """One predicate branch of a :class:`Fork` node.

    Args:
        when: Predicate expression or custom predicate.
        process: Terminal process executed when the predicate matches.
    """

    when: ForkPredicateSpec[InT]
    process: Process[InT, Any]


class Fork(Generic[InT]):
    """Terminal graph-level routing node that physically splits the stream.

    Unlike :class:`loom.streaming.nodes.Router`, branches are full
    independent sub-graphs. Each branch may contain any process node,
    including ``With`` and ``WithAsync``. ``Fork`` is terminal in the
    parent process: no nodes may follow it.
    """

    __slots__ = ("_default", "_kind", "_predicate_routes", "_routes", "_selector")

    def __init__(
        self,
        *,
        kind: ForkKind,
        selector: ForkSelectorSpec[InT] | None = None,
        routes: Mapping[object, Process[InT, Any]] | None = None,
        predicate_routes: Sequence[ForkRoute[InT]] = (),
        default: Process[InT, Any] | None = None,
    ) -> None:
        self._kind = kind
        self._selector = selector
        self._routes = dict(routes or {})
        self._predicate_routes = tuple(predicate_routes)
        self._default = default

    @classmethod
    def by(
        cls,
        selector: ForkSelectorSpec[InT],
        branches: Mapping[object, Process[InT, Any]],
        *,
        default: Process[InT, Any] | None = None,
    ) -> Fork[InT]:
        """Declare key-based terminal branching.

        Args:
            selector: Path expression or custom selector.
            branches: Terminal processes keyed by selector result.
            default: Optional fallback process.

        Returns:
            Fork declaration.
        """
        if not branches:
            raise ValueError("Fork.by requires at least one keyed route.")
        return cls(kind=ForkKind.KEYED, selector=selector, routes=branches, default=default)

    @classmethod
    def when(
        cls,
        routes: Sequence[ForkRoute[InT]],
        *,
        default: Process[InT, Any] | None = None,
    ) -> Fork[InT]:
        """Declare ordered first-match terminal branching.

        Args:
            routes: Ordered predicate routes.
            default: Optional fallback process.

        Returns:
            Fork declaration.
        """
        if not routes:
            raise ValueError("Fork.when requires at least one predicate route.")
        return cls(
            kind=ForkKind.PREDICATE,
            predicate_routes=routes,
            default=default,
        )

    @property
    def kind(self) -> ForkKind:
        """Routing family for this fork."""
        return self._kind

    @property
    def selector(self) -> ForkSelectorSpec[InT] | None:
        """Key selector for ``Fork.by`` branches."""
        return self._selector

    @property
    def routes(self) -> Mapping[object, Process[InT, Any]]:
        """Keyed branches for ``Fork.by``."""
        return dict(self._routes)

    @property
    def predicate_routes(self) -> tuple[ForkRoute[InT], ...]:
        """Ordered predicate routes for ``Fork.when``."""
        return self._predicate_routes

    @property
    def default(self) -> Process[InT, Any] | None:
        """Fallback process."""
        return self._default


def select_value(selector: ForkSelectorSpec[InT], message: Message[InT]) -> object:
    """Evaluate a selector for one message."""
    if isinstance(selector, PathRef):
        return evaluate_expr(selector, {"message": message, "payload": message.payload})
    return selector.select(message)


def evaluate_predicate(predicate: ForkPredicateSpec[InT], message: Message[InT]) -> bool:
    """Evaluate a predicate for one message."""
    if isinstance(predicate, Predicate):
        return predicate.matches(message)
    return bool(evaluate_expr(predicate, {"message": message, "payload": message.payload}))


__all__ = ["Fork", "ForkKind", "ForkRoute", "evaluate_predicate", "select_value"]
