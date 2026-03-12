from __future__ import annotations

from typing import Any, Protocol, TypeVar

import msgspec

from loom.core.model import LoomStruct
from loom.core.repository.abc.query import (
    CursorResult,
    FilterParams,
    PageParams,
    PageResult,
    QuerySpec,
)

ModelT = TypeVar("ModelT", bound=LoomStruct, covariant=True)


class RepoFor(Protocol[ModelT]):
    """Model-centric repository protocol used for UseCase dependency injection.

    This protocol intentionally focuses on the generic CRUD surface used by
    most UseCases. Infrastructure bindings may provide a
    ``RepositorySQLAlchemy`` instance or any compatible implementation.
    """

    async def get_by_id(self, obj_id: Any, profile: str = "default") -> ModelT | None:
        """Fetch one entity by id."""
        ...

    async def get_by(
        self,
        field: str,
        value: Any,
        profile: str = "default",
    ) -> ModelT | None:
        """Fetch one entity by arbitrary field."""
        ...

    async def exists_by(self, field: str, value: Any) -> bool:
        """Check whether any entity exists matching ``field == value``."""
        ...

    async def count(self) -> int:
        """Return the total number of entities in the repository."""
        ...

    async def list_paginated(
        self,
        page_params: PageParams,
        filter_params: FilterParams | None = None,
        profile: str = "default",
    ) -> PageResult[ModelT]:
        """Fetch entities with pagination."""
        ...

    async def list_with_query(
        self,
        query: QuerySpec,
        profile: str = "default",
    ) -> PageResult[ModelT] | CursorResult[ModelT]:
        """Fetch entities using a structured query (offset or cursor mode)."""
        ...

    async def create(self, data: msgspec.Struct) -> ModelT:
        """Persist one entity."""
        ...

    async def update(self, obj_id: Any, data: msgspec.Struct) -> ModelT | None:
        """Update one entity."""
        ...

    async def delete(self, obj_id: Any) -> bool:
        """Delete one entity."""
        ...
