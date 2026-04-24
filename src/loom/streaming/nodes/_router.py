"""Streaming router declarations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, ClassVar, Generic, TypeVar

from loom.core.expr import ExprNode, PathRef, evaluate_expr
from loom.core.model import LoomFrozenStruct
from loom.streaming.core._message import Message, StreamPayload
from loom.streaming.nodes._protocols import Predicate, Selector

if TYPE_CHECKING:
    from loom.streaming.graph._flow import Process

InT = TypeVar("InT", bound=StreamPayload)
OutT = TypeVar("OutT", bound=StreamPayload)

PredicateSpec = ExprNode | Predicate[InT]
SelectorSpec = PathRef | Selector[InT]


class Route(LoomFrozenStruct, Generic[InT, OutT], frozen=True):
    """One predicate route branch.

    Args:
        when: Predicate expression or custom predicate.
        process: Process executed when the predicate matches.
    """

    when: PredicateSpec[InT]
    process: Process[InT, OutT]


class Router(Generic[InT, OutT]):
    """Declarative streaming router.

    Use :meth:`by` for key-based dispatch and :meth:`when` for ordered
    predicate dispatch. The router only declares graph shape; runtime
    execution belongs to backend adapters.
    """

    __slots__ = ("_selector", "_routes", "_predicate_routes", "_default")
    router_branch_safe: ClassVar[bool] = True

    def __init__(
        self,
        *,
        selector: SelectorSpec[InT] | None = None,
        routes: Mapping[object, Process[InT, OutT]] | None = None,
        predicate_routes: Sequence[Route[InT, OutT]] = (),
        default: Process[InT, OutT] | None = None,
    ) -> None:
        if selector is None and not predicate_routes:
            raise ValueError("Router requires a selector or at least one predicate route.")
        if selector is not None and not routes:
            raise ValueError("Router.by requires at least one keyed route.")
        self._selector = selector
        self._routes = dict(routes or {})
        self._predicate_routes = tuple(predicate_routes)
        self._default = default

    @classmethod
    def by(
        cls,
        selector: SelectorSpec[InT],
        routes: Mapping[object, Process[InT, OutT]],
        *,
        default: Process[InT, OutT] | None = None,
    ) -> Router[InT, OutT]:
        """Declare key-based routing.

        Args:
            selector: Path expression or custom selector.
            routes: Processes keyed by selector result.
            default: Optional fallback process.

        Returns:
            Router declaration.
        """

        return cls(selector=selector, routes=routes, default=default)

    @classmethod
    def when(
        cls,
        routes: Sequence[Route[InT, OutT]],
        *,
        default: Process[InT, OutT] | None = None,
    ) -> Router[InT, OutT]:
        """Declare ordered first-match predicate routing.

        Args:
            routes: Ordered predicate routes.
            default: Optional fallback process.

        Returns:
            Router declaration.
        """

        return cls(predicate_routes=routes, default=default)

    @property
    def selector(self) -> SelectorSpec[InT] | None:
        """Key selector for ``Router.by`` routes."""
        return self._selector

    @property
    def routes(self) -> Mapping[object, Process[InT, OutT]]:
        """Keyed routes for ``Router.by``."""
        return dict(self._routes)

    @property
    def predicate_routes(self) -> tuple[Route[InT, OutT], ...]:
        """Ordered predicate routes for ``Router.when``."""
        return self._predicate_routes

    @property
    def default(self) -> Process[InT, OutT] | None:
        """Fallback process."""
        return self._default


def select_value(selector: SelectorSpec[InT], message: Message[InT]) -> object:
    """Evaluate a selector for one message.

    Args:
        selector: Path expression or custom selector.
        message: Streaming message.

    Returns:
        Route key.
    """

    if isinstance(selector, PathRef):
        return evaluate_expr(selector, {"message": message, "payload": message.payload})
    return selector.select(message)


def evaluate_predicate(predicate: PredicateSpec[InT], message: Message[InT]) -> bool:
    """Evaluate a predicate for one message.

    Args:
        predicate: Expression predicate or custom predicate.
        message: Streaming message.

    Returns:
        Whether the predicate matched.
    """

    if isinstance(predicate, Predicate):
        return predicate.matches(message)
    return bool(evaluate_expr(predicate, {"message": message, "payload": message.payload}))


__all__ = ["Route", "Router", "evaluate_predicate", "select_value"]
