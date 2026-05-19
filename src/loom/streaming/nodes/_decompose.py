"""Decompose DSL node for typed multi-target event transformation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, Generic, Protocol, TypeVar

from loom.streaming.core._message import StreamPayload

PayloadT_contra = TypeVar("PayloadT_contra", bound=StreamPayload, contravariant=True)
InT = TypeVar("InT", bound=StreamPayload)


class EntityDecomposer(Protocol[PayloadT_contra]):
    """Map one typed stream event into typed row collections for multiple targets.

    Each key in the returned dict is the concrete type that the downstream
    Router uses to dispatch to the correct IntoSink branch — type-keyed, not
    string-keyed. The decompose() method must be pure: no I/O, no side effects,
    no knowledge of downstream sinks or storage backends.

    Declare ``targets`` to allow the compiler to validate that every produced
    type has a matching Router branch with an IntoSink node.

    Example::

        class StoreEventDecomposer(EntityDecomposer[StoreEvent]):
            targets: ClassVar[tuple[type, ...]] = (StoreRow, LanguageRow)

            @classmethod
            def decompose(cls, event: StoreEvent) -> dict[type, list[Any]]:
                return {
                    StoreRow:    [StoreRow(id=s.id, name=s.name) for s in event.stores],
                    LanguageRow: [LanguageRow(code=l.code) for s in event.stores
                                  for l in s.languages],
                }

    Args:
        PayloadT_contra: Contravariant type of the incoming stream event.
    """

    targets: ClassVar[tuple[type, ...]]

    @classmethod
    def decompose(cls, event: PayloadT_contra) -> dict[type, list[Any]]:
        """Decompose one event into typed row collections.

        Args:
            event: Incoming stream event to decompose.

        Returns:
            Mapping from target type to list of row instances of that type.
            Every type in ``targets`` must appear as a key.
        """
        ...


@dataclass(frozen=True)
class Decompose(Generic[InT]):
    """Transformation step that fans one typed event into N typed sub-events.

    Decompose sits upstream of a Router and has no knowledge of downstream
    sinks. The Router dispatches each emitted type to the matching IntoSink
    branch — the same mechanism used for any multi-type stream. This keeps
    decomposers and sinks fully decoupled.

    Can appear in Router branches when nested decomposition is needed
    (e.g. OrderEvent → OrderLineEvent → OrderLineRow).

    Args:
        decomposer: Class implementing EntityDecomposer[InT].

    Example::

        Process(
            Decompose(StoreEventDecomposer),
            Router({
                StoreRow:    Process(IntoTable(payload=StoreRow, ...)),
                LanguageRow: Process(IntoTable(payload=LanguageRow, ...)),
            }),
        )
    """

    decomposer: type[EntityDecomposer[InT]]
    router_branch_safe: ClassVar[bool] = True


__all__ = ["Decompose", "EntityDecomposer"]
