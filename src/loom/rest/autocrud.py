"""Auto-CRUD route and UseCase generators.

Generates concrete :class:`~loom.core.use_case.use_case.UseCase` subclasses and
:class:`~loom.rest.model.RestRoute` objects for the five standard CRUD operations,
given a domain model type.

The generated classes are cached per model — multiple interfaces sharing the same
model reuse the same UseCase classes.

NOTE: This module intentionally omits ``from __future__ import annotations`` so
that annotation expressions are evaluated eagerly at class-definition time.
The factory functions capture ``model`` as a closure variable and embed it
directly in the ``execute`` signature, making ``typing.get_type_hints`` work
without requiring the model name to be present in the module's global namespace.

Usage::

    from loom.rest.autocrud import build_auto_routes

    routes = build_auto_routes(User, include=("create", "get", "list"))
"""

from collections.abc import Callable
from typing import Any, cast, get_type_hints

from loom.core.repository.abc.query import QuerySpec
from loom.core.use_case.markers import Input
from loom.core.use_case.use_case import UseCase

__all__ = ["build_auto_routes"]

# ---------------------------------------------------------------------------
# Operation metadata
# ---------------------------------------------------------------------------

_ALL_OPS: tuple[str, ...] = ("create", "get", "list", "update", "delete")

_OP_METHOD: dict[str, str] = {
    "create": "POST",
    "get": "GET",
    "list": "GET",
    "update": "PATCH",
    "delete": "DELETE",
}
_OP_PATH: dict[str, str] = {
    "create": "/",
    "get": "/{id}",
    "list": "/",
    "update": "/{id}",
    "delete": "/{id}",
}
_OP_STATUS: dict[str, int] = {"create": 201}

# ---------------------------------------------------------------------------
# ID coercion
# ---------------------------------------------------------------------------


def _id_coerce(model: type[Any]) -> Callable[[str], Any]:
    """Return ``int`` coercer when model declares ``id: int``, else identity.

    Args:
        model: Domain model type to inspect.

    Returns:
        ``int`` builtin if the ``id`` field annotation is ``int``,
        otherwise an identity callable that returns the value unchanged.
    """
    try:
        hints = get_type_hints(model)
    except Exception:
        return lambda x: x
    return int if hints.get("id") is int else (lambda x: x)


# ---------------------------------------------------------------------------
# UseCase class factories
# ---------------------------------------------------------------------------


def _make_create(model: type[Any]) -> type[UseCase[Any, Any]]:
    """Generate a UseCase that creates a new entity via the main repository.

    Args:
        model: Domain model type used as both TModel and TResult.

    Returns:
        A concrete UseCase subclass named ``AutoCreate<ModelName>``.
    """

    class AutoCreate(UseCase[model, model]):  # type: ignore[valid-type]
        async def execute(self, cmd: model = Input()) -> Any:  # type: ignore[valid-type]
            return cast(Any, await self.main_repo.create(cmd))

    AutoCreate.__name__ = f"AutoCreate{model.__name__}"
    AutoCreate.__qualname__ = f"AutoCreate{model.__name__}"
    AutoCreate.__module__ = __name__
    return AutoCreate


def _make_get(model: type[Any], coerce: Callable[[str], Any]) -> type[UseCase[Any, Any]]:
    """Generate a UseCase that fetches a single entity by id.

    Args:
        model: Domain model type.
        coerce: Callable that converts the raw path-param string to the id type.

    Returns:
        A concrete UseCase subclass named ``AutoGet<ModelName>``.
    """

    class AutoGet(UseCase[model, model]):  # type: ignore[valid-type]
        async def execute(self, id: str, profile: str = "default") -> Any:
            return await self.main_repo.get_by_id(coerce(id), profile=profile)

    AutoGet.__name__ = f"AutoGet{model.__name__}"
    AutoGet.__qualname__ = f"AutoGet{model.__name__}"
    AutoGet.__module__ = __name__
    return AutoGet


def _make_list(model: type[Any]) -> type[UseCase[Any, Any]]:
    """Generate a UseCase that lists entities via a QuerySpec.

    Args:
        model: Domain model type.

    Returns:
        A concrete UseCase subclass named ``AutoList<ModelName>``.
    """

    class AutoList(UseCase[model, model]):  # type: ignore[valid-type]
        async def execute(self, query: QuerySpec, profile: str = "default") -> Any:
            repo: Any = self.main_repo
            return await repo.list_with_query(query, profile=profile)

    AutoList.__name__ = f"AutoList{model.__name__}"
    AutoList.__qualname__ = f"AutoList{model.__name__}"
    AutoList.__module__ = __name__
    return AutoList


def _make_update(model: type[Any], coerce: Callable[[str], Any]) -> type[UseCase[Any, Any]]:
    """Generate a UseCase that applies a partial update to an entity.

    Args:
        model: Domain model type.
        coerce: Callable that converts the raw path-param string to the id type.

    Returns:
        A concrete UseCase subclass named ``AutoUpdate<ModelName>``.
    """

    class AutoUpdate(UseCase[model, model]):  # type: ignore[valid-type]
        async def execute(self, id: str, cmd: model = Input()) -> Any:  # type: ignore[valid-type]
            return await self.main_repo.update(coerce(id), cmd)

    AutoUpdate.__name__ = f"AutoUpdate{model.__name__}"
    AutoUpdate.__qualname__ = f"AutoUpdate{model.__name__}"
    AutoUpdate.__module__ = __name__
    return AutoUpdate


def _make_delete(model: type[Any], coerce: Callable[[str], Any]) -> type[UseCase[Any, Any]]:
    """Generate a UseCase that deletes an entity by id.

    Args:
        model: Domain model type.
        coerce: Callable that converts the raw path-param string to the id type.

    Returns:
        A concrete UseCase subclass named ``AutoDelete<ModelName>``.
    """

    class AutoDelete(UseCase[model, model]):  # type: ignore[valid-type]
        async def execute(self, id: str) -> Any:
            return await self.main_repo.delete(coerce(id))

    AutoDelete.__name__ = f"AutoDelete{model.__name__}"
    AutoDelete.__qualname__ = f"AutoDelete{model.__name__}"
    AutoDelete.__module__ = __name__
    return AutoDelete


# ---------------------------------------------------------------------------
# Per-model cache
# ---------------------------------------------------------------------------

_UC_CACHE: dict[type[Any], dict[str, type[UseCase[Any, Any]]]] = {}


def _get_or_create(model: type[Any]) -> dict[str, type[UseCase[Any, Any]]]:
    """Return cached (or freshly built) UseCase classes for all five operations.

    Args:
        model: Domain model type.

    Returns:
        Mapping of operation name to UseCase class.
    """
    if model in _UC_CACHE:
        return _UC_CACHE[model]
    coerce = _id_coerce(model)
    result: dict[str, type[UseCase[Any, Any]]] = {
        "create": _make_create(model),
        "get": _make_get(model, coerce),
        "list": _make_list(model),
        "update": _make_update(model, coerce),
        "delete": _make_delete(model, coerce),
    }
    _UC_CACHE[model] = result
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_auto_routes(
    model: type[Any],
    include: tuple[str, ...],
) -> "tuple[Any, ...]":
    """Return ``RestRoute`` objects for the requested CRUD operations.

    Lazily imports :class:`~loom.rest.model.RestRoute` to avoid circular
    imports at module load time.

    Args:
        model: Domain model type for which routes should be generated.
        include: Subset of operation names to expose.  If empty, all five
            standard operations are included.

    Returns:
        Tuple of :class:`~loom.rest.model.RestRoute` instances, one per
        requested operation.

    Example::

        routes = build_auto_routes(User, include=("create", "get", "list"))
    """
    from loom.rest.model import RestRoute  # lazy — avoids circular import

    ops = include or _ALL_OPS
    use_cases = _get_or_create(model)
    return tuple(
        RestRoute(
            use_case=use_cases[op],
            method=_OP_METHOD[op],
            path=_OP_PATH[op],
            status_code=_OP_STATUS.get(op, 200),
        )
        for op in ops
        if op in use_cases
    )
