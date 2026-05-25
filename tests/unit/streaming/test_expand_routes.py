"""Unit tests for the ExpandRoutes fan-out node."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, ClassVar

import pytest

from loom.core.model import LoomStruct
from loom.streaming import Process
from loom.streaming.nodes._decompose import PayloadExpander
from loom.streaming.nodes._expand_routes import ExpandRoutes
from loom.streaming.nodes._shape import Drain


class _StoreEvent(LoomStruct):
    store_id: str
    name: str


class _StoreRow(LoomStruct):
    store_id: str


class _AuditRow(LoomStruct):
    name: str


class _StoreExpander(PayloadExpander[_StoreEvent]):
    outputs: ClassVar[tuple[type, ...]] = (_StoreRow, _AuditRow)

    @classmethod
    def expand(cls, event: _StoreEvent) -> dict[type, list[Any]]:
        return {
            _StoreRow: [_StoreRow(store_id=event.store_id)],
            _AuditRow: [_AuditRow(name=event.name)],
        }


def _drain_process() -> Process[Any, Any]:
    return Process(Drain())


def test_expand_routes_requires_routes_or_default() -> None:
    with pytest.raises(ValueError, match="at least one route"):
        ExpandRoutes(expander=_StoreExpander, routes={}, default=None)


def test_expand_routes_with_default_only() -> None:
    default = _drain_process()
    node: ExpandRoutes[_StoreEvent] = ExpandRoutes(
        expander=_StoreExpander,
        routes={},
        default=default,
    )
    assert node.default is default
    assert len(node.routes) == 0


def test_expand_routes_routes_are_frozen() -> None:
    node: ExpandRoutes[_StoreEvent] = ExpandRoutes(
        expander=_StoreExpander,
        routes={_StoreRow: _drain_process()},
    )
    assert isinstance(node.routes, MappingProxyType)
    with pytest.raises((TypeError, AttributeError)):
        node.routes[_AuditRow] = _drain_process()  # type: ignore[index]


def test_expand_routes_stores_expander_and_routes() -> None:
    store_process = _drain_process()
    audit_process = _drain_process()
    node: ExpandRoutes[_StoreEvent] = ExpandRoutes(
        expander=_StoreExpander,
        routes={_StoreRow: store_process, _AuditRow: audit_process},
    )
    assert node.expander is _StoreExpander
    assert node.routes[_StoreRow] is store_process
    assert node.routes[_AuditRow] is audit_process
    assert node.default is None


def test_expand_routes_expander_outputs_accessible_via_node() -> None:
    node: ExpandRoutes[_StoreEvent] = ExpandRoutes(
        expander=_StoreExpander,
        routes={_StoreRow: _drain_process()},
        default=_drain_process(),
    )
    outputs = node.expander.outputs
    assert _StoreRow in outputs
    assert _AuditRow in outputs
