from __future__ import annotations

from typing import Generic, TypeVar

EntityT = TypeVar("EntityT")


class Input:
    """Marks a parameter as the command payload input.

    Used to annotate ``execute`` signatures for declarative dispatch.
    Carries no configuration — it is a pure marker.

    Example::

        async def execute(self, command: Input[CreateUser], ...) -> User: ...
    """

    __slots__ = ()


class Load(Generic[EntityT]):
    """Marks a parameter as a prefetched entity.

    The orchestrator resolves the entity before calling ``execute``,
    using ``entity_type`` and the field indicated by ``by``.

    Args:
        entity_type: The domain entity type to load.
        by: Name of the command field whose value is used as the lookup key.

    Example::

        async def execute(
            self,
            command: Input[UpdateUser],
            user: Load[User],
        ) -> User: ...
    """

    __slots__ = ("entity_type", "by")

    def __init__(self, entity_type: type[EntityT], *, by: str = "id") -> None:
        self.entity_type = entity_type
        self.by = by
