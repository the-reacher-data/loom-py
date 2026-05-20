"""Decompose DSL node for typed multi-target event transformation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, Generic, Protocol, TypeVar

from loom.streaming.core._message import StreamPayload

PayloadT_contra = TypeVar("PayloadT_contra", bound=StreamPayload, contravariant=True)
InT = TypeVar("InT", bound=StreamPayload)


class PayloadExpander(Protocol[PayloadT_contra]):
    """Expand one typed stream event into typed sub-event collections.

    Each key in the returned dict is the concrete type that the downstream
    Router uses to dispatch to the correct branch — type-keyed, not
    string-keyed. The expand() method must be pure: no I/O, no side effects,
    no knowledge of downstream sinks or storage backends.

    Declare ``outputs`` to allow the compiler to validate that every produced
    type has a matching Router branch.

    Example::

        class StoreEventExpander(PayloadExpander[StoreEvent]):
            outputs: ClassVar[tuple[type, ...]] = (StoreRow, LanguageRow)

            @classmethod
            def expand(cls, event: StoreEvent) -> dict[type, list[Any]]:
                return {
                    StoreRow:    [StoreRow(id=s.id, name=s.name) for s in event.stores],
                    LanguageRow: [LanguageRow(code=l.code) for s in event.stores
                                  for l in s.languages],
                }

    Args:
        PayloadT_contra: Contravariant type of the incoming stream event.
    """

    outputs: ClassVar[tuple[type, ...]]

    @classmethod
    def expand(cls, event: PayloadT_contra) -> dict[type, list[Any]]:
        """Expand one event into typed sub-event collections.

        Args:
            event: Incoming stream event to expand.

        Returns:
            Mapping from output type to list of event instances of that type.
            Every type in ``outputs`` must appear as a key.
        """
        ...


@dataclass(frozen=True)
class Decompose(Generic[InT]):
    """Transformation step that fans one typed event into N typed sub-events.

    Decompose sits upstream of a Router and has no knowledge of downstream
    sinks. The Router dispatches each emitted type to the matching branch —
    the same mechanism used for any multi-type stream. This keeps expanders
    and sinks fully decoupled.

    Can appear in Router branches when nested expansion is needed
    (e.g. OrderEvent → OrderLineEvent → OrderLineRow).

    Args:
        expander: Class implementing PayloadExpander[InT].

    Example::

        Process(
            Decompose(StoreEventExpander),
            Router({
                StoreRow:    Process(IntoTable(payload=StoreRow, ...)),
                LanguageRow: Process(IntoTable(payload=LanguageRow, ...)),
            }),
        )
    """

    expander: type[PayloadExpander[InT]]
    router_branch_safe: ClassVar[bool] = True


__all__ = ["Decompose", "PayloadExpander"]
