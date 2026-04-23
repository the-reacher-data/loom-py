from __future__ import annotations

import pytest

from loom.core.model import LoomFrozenStruct
from loom.streaming import Message, MessageMeta, Process, Route, Router, msg
from loom.streaming.nodes._router import evaluate_predicate, select_value


class _Order(LoomFrozenStruct, frozen=True):
    country: str
    amount: int
    event_type: str
    tags: tuple[str, ...] = ()


class _ValidatedOrder(LoomFrozenStruct, frozen=True):
    order_id: str = "ok"


class _ManualReview:
    pass


class _CountrySelector:
    def select(self, message: Message[_Order]) -> object:
        return message.payload.country


class _HighRiskPredicate:
    def matches(self, message: Message[_Order]) -> bool:
        return message.payload.amount > 1000 and message.meta.headers.get("risk") == b"high"


def _message(
    *,
    country: str = "ES",
    amount: int = 1500,
    event_type: str = "created",
    headers: dict[str, bytes] | None = None,
) -> Message[_Order]:
    return Message(
        payload=_Order(country=country, amount=amount, event_type=event_type),
        meta=MessageMeta(message_id="msg-1", headers=headers or {}),
    )


def test_msg_expression_can_select_message_payload_fields() -> None:
    message = _message(country="FR")

    assert select_value(msg.payload.country, message) == "FR"
    assert select_value(_CountrySelector(), message) == "FR"


def test_msg_expression_can_evaluate_composed_predicates() -> None:
    message = _message(headers={"risk": b"high"})
    predicate = (
        (msg.payload.country == "ES")
        & (msg.payload.amount >= 1000)
        & msg.payload.amount.between(1000, 2000)
        & (msg.meta.headers["risk"] == b"high")
    )

    assert evaluate_predicate(predicate, message) is True


def test_msg_expression_supports_in_or_and_not() -> None:
    message = _message(country="FR", amount=20)
    predicate = (msg.payload.country.isin({"ES", "FR"})) & ~(msg.payload.amount > 100)

    assert evaluate_predicate(predicate, message) is True


def test_custom_predicate_can_match_message() -> None:
    message = _message(headers={"risk": b"high"})

    assert evaluate_predicate(_HighRiskPredicate(), message) is True


def test_router_by_declares_keyed_routes() -> None:
    created = Process[_Order, _ValidatedOrder](_ValidatedOrder)
    cancelled = Process[_Order, _ValidatedOrder](_ManualReview)
    default = Process[_Order, _ValidatedOrder](object())

    router = Router.by(
        msg.payload.event_type,
        routes={"created": created, "cancelled": cancelled},
        default=default,
    )

    assert router.selector == msg.payload.event_type
    assert router.routes == {"created": created, "cancelled": cancelled}
    assert router.default is default


def test_router_when_declares_ordered_predicate_routes() -> None:
    manual = Process[_Order, _ValidatedOrder](_ManualReview)
    normal = Process[_Order, _ValidatedOrder](_ValidatedOrder)
    route = Route(when=msg.payload.amount > 1000, process=manual)

    router = Router.when((route,), default=normal)

    assert router.predicate_routes == (route,)
    assert router.default is normal
    assert evaluate_predicate(router.predicate_routes[0].when, _message()) is True


def test_router_rejects_empty_declarations() -> None:
    with pytest.raises(ValueError, match="keyed route"):
        Router.by(msg.payload.event_type, routes={})

    with pytest.raises(ValueError, match="selector or at least one predicate"):
        Router.when(())
