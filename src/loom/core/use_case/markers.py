from __future__ import annotations

from enum import StrEnum
from typing import Any, Generic, TypeVar

EntityT = TypeVar("EntityT")


class SourceKind(StrEnum):
    """Origin of a lookup value used by Load/Exists markers."""

    PARAM = "param"
    COMMAND = "command"


class LookupKind(StrEnum):
    """Lookup strategy used by marker-driven prefetch."""

    BY_ID = "by_id"
    BY_FIELD = "by_field"


class OnMissing(StrEnum):
    """Policy applied when a marker lookup does not resolve an entity."""

    RAISE = "raise"
    RETURN_NONE = "return_none"
    RETURN_FALSE = "return_false"


class _InputMarker:
    """Marks a parameter as the command payload input.

    Used to annotate ``execute`` signatures for declarative dispatch.
    Carries no configuration — it is a pure marker.

    Example::

        async def execute(self, command: Input[CreateUser], ...) -> User: ...
    """

    __slots__ = ()


class _LoadByIdMarker(Generic[EntityT]):
    """Marks a parameter as a prefetched entity loaded by id.

    The orchestrator resolves the entity before calling ``execute``,
    using ``entity_type`` and the value from a primitive execute parameter.

    Args:
        entity_type: The domain entity type to load.
        by: Name of the primitive execute parameter used as lookup id.
        profile: Loading profile passed to ``repo.get_by_id``.  Controls
            which eager-load options are applied (e.g. joined relations).
            Defaults to ``"default"``.
        on_missing: Missing-entity policy. Defaults to ``OnMissing.RAISE``.

    Example::

        async def execute(
            self,
            command: Input[UpdateUser],
            user: User = LoadById(User, by="user_id"),
        ) -> User: ...
    """

    __slots__ = ("entity_type", "by", "profile", "on_missing")

    def __init__(
        self,
        entity_type: type[EntityT],
        *,
        by: str = "id",
        profile: str = "default",
        on_missing: OnMissing = OnMissing.RAISE,
    ) -> None:
        self.entity_type = entity_type
        self.by = by
        self.profile = profile
        self.on_missing = on_missing


class _LoadMarker(Generic[EntityT]):
    """Marks a parameter as a prefetched entity loaded by an arbitrary field."""

    __slots__ = (
        "entity_type",
        "from_kind",
        "from_name",
        "against",
        "profile",
        "on_missing",
    )

    def __init__(
        self,
        entity_type: type[EntityT],
        *,
        from_kind: SourceKind,
        from_name: str,
        against: str,
        profile: str = "default",
        on_missing: OnMissing = OnMissing.RAISE,
    ) -> None:
        self.entity_type = entity_type
        self.from_kind = from_kind
        self.from_name = from_name
        self.against = against
        self.profile = profile
        self.on_missing = on_missing


class _ExistsMarker(Generic[EntityT]):
    """Marks a parameter as a boolean existence check."""

    __slots__ = ("entity_type", "from_kind", "from_name", "against", "on_missing")

    def __init__(
        self,
        entity_type: type[EntityT],
        *,
        from_kind: SourceKind,
        from_name: str,
        against: str,
        on_missing: OnMissing = OnMissing.RETURN_FALSE,
    ) -> None:
        self.entity_type = entity_type
        self.from_kind = from_kind
        self.from_name = from_name
        self.against = against
        self.on_missing = on_missing


def Input() -> Any:
    """Factory returning the runtime marker for command payload parameters.

    Returned value is intentionally typed as ``Any`` in overloads to avoid
    ``mypy`` default-argument incompatibility in signatures like:
    ``cmd: Command = Input()``.
    """
    return _InputMarker()


def LoadById(
    entity_type: type[EntityT],
    *,
    by: str = "id",
    profile: str = "default",
    on_missing: OnMissing = OnMissing.RAISE,
) -> Any:
    """Factory returning marker for preloaded entity parameters by id.

    Returned value is intentionally typed as ``Any`` in overloads to avoid
    ``mypy`` default-argument incompatibility in signatures like:
    ``entity: User = LoadById(User, by="id")``.

    Args:
        entity_type: Domain entity type the repository should load.
        by: Name of the primitive parameter used as the lookup key.
            Defaults to ``"id"``.
        profile: Loading profile forwarded to ``repo.get_by_id``.
            Defaults to ``"default"``.
        on_missing: Missing-entity policy. Defaults to ``OnMissing.RAISE``.
    """
    return _LoadByIdMarker(
        entity_type,
        by=by,
        profile=profile,
        on_missing=on_missing,
    )


def Load(
    entity_type: type[EntityT],
    *,
    from_param: str | None = None,
    from_command: str | None = None,
    against: str,
    profile: str = "default",
    on_missing: OnMissing = OnMissing.RAISE,
) -> Any:
    """Factory returning marker for preloaded entity parameters by field."""

    if (from_param is None) == (from_command is None):
        raise ValueError("Load() requires exactly one of from_param or from_command")

    if from_param is not None:
        return _LoadMarker(
            entity_type,
            from_kind=SourceKind.PARAM,
            from_name=from_param,
            against=against,
            profile=profile,
            on_missing=on_missing,
        )

    return _LoadMarker(
        entity_type,
        from_kind=SourceKind.COMMAND,
        from_name=from_command or "",
        against=against,
        profile=profile,
        on_missing=on_missing,
    )


def Exists(
    entity_type: type[EntityT],
    *,
    from_param: str | None = None,
    from_command: str | None = None,
    against: str,
    on_missing: OnMissing = OnMissing.RETURN_FALSE,
) -> Any:
    """Factory returning marker for boolean existence checks."""
    if (from_param is None) == (from_command is None):
        raise ValueError("Exists() requires exactly one of from_param or from_command")

    if from_param is not None:
        return _ExistsMarker(
            entity_type,
            from_kind=SourceKind.PARAM,
            from_name=from_param,
            against=against,
            on_missing=on_missing,
        )

    return _ExistsMarker(
        entity_type,
        from_kind=SourceKind.COMMAND,
        from_name=from_command or "",
        against=against,
        on_missing=on_missing,
    )
