"""Lifecycle event model for unified observability across Loom modules."""

from __future__ import annotations

from enum import StrEnum

import msgspec


class EventKind(StrEnum):
    """Lifecycle phase emitted by an ``ObservabilityRuntime`` span."""

    START = "start"
    END = "end"
    ERROR = "error"
    COLLECT = "collect"


class LifecycleEvent(msgspec.Struct, frozen=True, kw_only=True):
    """Immutable lifecycle event emitted to all registered observers.

    All fields except ``scope``, ``name``, and ``kind`` are optional — observers
    must tolerate ``None`` for any contextual field.

    Args:
        scope: Logical unit of work (``"use_case"``, ``"node"``, ``"job"``,
            ``"poll_cycle"``, ``"batch_collect"``, ``"transport"`` …).
        name: Human-readable label for the operation within the scope.
        kind: Lifecycle phase — ``START``, ``END``, ``ERROR``, or ``COLLECT``.
        trace_id: Trace identifier for the current unit of work. Generated
            once per root span and propagated to all child spans.
        correlation_id: Business lineage identifier (saga, request, batch
            group). Always propagated from the upstream context.
        id: Optional stable identifier for the specific operation instance.
        duration_ms: Wall-clock duration in milliseconds. Set on ``END``
            and ``ERROR`` events only.
        status: Short outcome label (``"ok"``, ``"error"``). Set on ``END``.
        error: Human-readable error description. Set on ``ERROR`` only.
        meta: Arbitrary key-value pairs for observer-specific enrichment.
    """

    scope: str
    name: str
    kind: EventKind
    trace_id: str | None = None
    correlation_id: str | None = None
    id: str | None = None
    duration_ms: float | None = None
    status: str | None = None
    error: str | None = None
    meta: dict[str, object] = msgspec.field(default_factory=dict)


__all__ = ["EventKind", "LifecycleEvent"]
