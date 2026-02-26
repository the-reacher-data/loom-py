from __future__ import annotations

from typing import Any, Generic, TypeVar

EntityT = TypeVar("EntityT")


class _InputMarker:
    """Marks a parameter as the command payload input.

    Used to annotate ``execute`` signatures for declarative dispatch.
    Carries no configuration — it is a pure marker.

    Example::

        async def execute(self, command: Input[CreateUser], ...) -> User: ...
    """

    __slots__ = ()


class _LoadMarker(Generic[EntityT]):
    """Marks a parameter as a prefetched entity.

    The orchestrator resolves the entity before calling ``execute``,
    using ``entity_type`` and the field indicated by ``by``.

    Args:
        entity_type: The domain entity type to load.
        by: Name of the command field whose value is used as the lookup key.
        profile: Loading profile passed to ``repo.get_by_id``.  Controls
            which eager-load options are applied (e.g. joined relations).
            Defaults to ``"default"``.

    Example::

        async def execute(
            self,
            command: Input[UpdateUser],
            user: Load[User, profile="detail"],
        ) -> User: ...
    """

    __slots__ = ("entity_type", "by", "profile")

    def __init__(
        self, entity_type: type[EntityT], *, by: str = "id", profile: str = "default"
    ) -> None:
        self.entity_type = entity_type
        self.by = by
        self.profile = profile


def Input() -> Any:
    """Factory returning the runtime marker for command payload parameters.

    Returned value is intentionally typed as ``Any`` in overloads to avoid
    ``mypy`` default-argument incompatibility in signatures like:
    ``cmd: Command = Input()``.
    """
    return _InputMarker()


def Load(entity_type: type[EntityT], *, by: str = "id", profile: str = "default") -> Any:
    """Factory returning the runtime marker for preloaded entity parameters.

    Returned value is intentionally typed as ``Any`` in overloads to avoid
    ``mypy`` default-argument incompatibility in signatures like:
    ``entity: User = Load(User, by="id")``.

    Args:
        entity_type: Domain entity type the repository should load.
        by: Name of the primitive parameter used as the lookup key.
            Defaults to ``"id"``.
        profile: Loading profile forwarded to ``repo.get_by_id``.
            Defaults to ``"default"``.
    """
    return _LoadMarker(entity_type, by=by, profile=profile)
