"""Counting doubles for the cached repository suites.

The cache read path is judged by how many round trips it makes, which cannot
be observed from its return values, so both collaborators count their calls.
The cache backend serializes with msgpack exactly like the production
serializer: a cached index round-trips its ids through it, which turns a
``UUID`` or a ``date`` into a string, and the read path has to cope with that.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import date
from typing import Any, Generic, NamedTuple, TypeVar
from uuid import UUID

import msgspec

from loom.core.cache import CacheConfig, CachedRepository, GenerationalDependencyResolver
from loom.core.cache.decorators import cache_query
from loom.core.model import BaseModel, ColumnField
from loom.core.repository import FilterParams, PageParams, PageResult, Repository
from loom.core.repository.abc.query import (
    CursorResult,
    FilterOp,
    PaginationMode,
    QuerySpec,
    build_page_result,
)
from loom.core.repository.sqlalchemy.query_compiler import UnsafeFilterError

T = TypeVar("T")

TAG_KEY_PREFIX = "tag:"


class Widget(BaseModel):
    """Row with an integer primary key."""

    __tablename__ = "widgets"

    id: int = ColumnField(primary_key=True)
    name: str = ColumnField(length=32)


class UuidWidget(BaseModel):
    """Row with a UUID primary key, which msgpack renders as a string."""

    __tablename__ = "uuid_widgets"

    id: UUID = ColumnField(primary_key=True)
    name: str = ColumnField(length=32)


class DateWidget(BaseModel):
    """Row with a date primary key, which msgpack also renders as a string."""

    __tablename__ = "date_widgets"

    id: date = ColumnField(primary_key=True)
    name: str = ColumnField(length=32)


RowT = TypeVar("RowT", Widget, UuidWidget, DateWidget)


class WidgetCreate(msgspec.Struct):
    """Creation payload; the doubles are read-only and never apply it."""

    name: str


class WidgetUpdate(msgspec.Struct, kw_only=True):
    """Update payload; the doubles are read-only and never apply it."""

    name: str | msgspec.UnsetType = msgspec.UNSET


class CountingCacheBackend:
    """In-memory cache backend that serializes values and counts its batches.

    Attributes:
        data: Raw msgpack storage keyed by cache key; tests delete entries from
            it to simulate an eviction.
        multi_get_batches: Keys requested by each ``multi_get_values`` call.
        multi_set_batches: Keys written by each ``multi_set_values`` call.
        set_ttls: TTL received by each ``set_value`` call, in order.
        multi_set_ttls: TTL received by each ``multi_set_values`` call, in order.
    """

    def __init__(self) -> None:
        self.data: dict[str, bytes] = {}
        self.multi_get_batches: list[list[str]] = []
        self.multi_set_batches: list[list[str]] = []
        self.set_ttls: list[int | None] = []
        self.multi_set_ttls: list[int | None] = []

    @property
    def tag_multi_get_calls(self) -> int:
        """Number of ``multi_get_values`` calls that read generation counters."""
        return sum(
            1
            for keys in self.multi_get_batches
            if keys and all(key.startswith(TAG_KEY_PREFIX) for key in keys)
        )

    def reset_counters(self) -> None:
        """Forget the recorded batches, keeping the stored values."""
        self.multi_get_batches.clear()
        self.multi_set_batches.clear()
        self.set_ttls.clear()
        self.multi_set_ttls.clear()

    def _decode(self, key: str, target: type[T] | None) -> T | Any | None:
        raw = self.data.get(key)
        if raw is None:
            return None
        if target is None:
            return msgspec.msgpack.decode(raw)
        return msgspec.msgpack.decode(raw, type=target)

    async def get_value(self, key: str, *, type: type[T] | None = None) -> T | Any | None:
        return self._decode(key, type)

    async def set_value(self, key: str, value: Any, ttl: int | None = None) -> None:
        self.set_ttls.append(ttl)
        self.data[key] = msgspec.msgpack.encode(value)

    async def multi_get_values(
        self,
        keys: list[str],
        *,
        type: type[T] | None = None,
    ) -> list[T | Any | None]:
        self.multi_get_batches.append(list(keys))
        return [self._decode(key, type) for key in keys]

    async def multi_set_values(self, pairs: list[tuple[str, Any]], ttl: int | None = None) -> None:
        self.multi_set_ttls.append(ttl)
        self.multi_set_batches.append([key for key, _value in pairs])
        for key, value in pairs:
            self.data[key] = msgspec.msgpack.encode(value)

    async def exists(self, key: str) -> bool:
        return key in self.data

    async def delete(self, key: str) -> int:
        if key not in self.data:
            return 0
        del self.data[key]
        return 1

    async def delete_many(self, keys: list[str]) -> int:
        return sum([await self.delete(key) for key in keys])

    async def incr(self, key: str, delta: int = 1) -> int:
        current = int(self._decode(key, int) or 0) + delta
        self.data[key] = msgspec.msgpack.encode(current)
        return current

    async def close(self) -> None:
        return None


class CountingRepository(Repository[RowT, WidgetCreate, WidgetUpdate, Any], Generic[RowT]):
    """Read-only repository double that counts reads and honours ``id IN (...)``.

    ``list_with_query`` matches ids by equality, as a typed backend does — a
    SQLAlchemy ``DateTime`` column filtered with strings returns no rows — and
    returns the matches in reverse insertion order so a test can tell whether
    the caller re-orders them instead of trusting the backend order.  Offset
    queries build a :class:`PageParams` like the SQLAlchemy and Mongo
    repositories do, so an out-of-range limit raises here too.

    Attributes:
        model: Loom model of the rows, read by the wrapper to resolve the
            primary-key type.
        storage: Rows keyed by primary key.
        get_by_id_calls: Number of ``get_by_id`` calls received.
        list_paginated_calls: Number of ``list_paginated`` calls received.
        list_with_query_calls: Number of ``list_with_query`` calls received.
        queries: Every ``QuerySpec`` received, in order.
    """

    entity_name = "widget"

    def __init__(self, rows: Sequence[RowT], row_type: type[RowT]) -> None:
        self.storage: dict[Any, RowT] = {row.id: row for row in rows}
        self.model = row_type
        self.get_by_id_calls = 0
        self.list_paginated_calls = 0
        self.list_with_query_calls = 0
        self.queries: list[QuerySpec] = []

    def reset_counters(self) -> None:
        """Zero every call counter and forget the recorded queries."""
        self.get_by_id_calls = 0
        self.list_paginated_calls = 0
        self.list_with_query_calls = 0
        self.queries.clear()

    def to_output_from_payload(self, payload: dict[str, Any]) -> RowT:
        """Rebuild a row from a cached builtins payload."""
        return msgspec.convert(payload, self.model)

    async def get_by_id(self, obj_id: Any, profile: str = "default") -> RowT | None:
        _ = profile
        self.get_by_id_calls += 1
        return self.storage.get(obj_id)

    async def get_by(self, field: str, value: Any, profile: str = "default") -> RowT | None:
        _ = profile
        if field != "id":
            raise ValueError(f"unsupported field: {field}")
        return self.storage.get(value)

    async def exists_by(self, field: str, value: Any) -> bool:
        return await self.get_by(field, value) is not None

    async def count(self) -> int:
        return len(self.storage)

    async def list_paginated(
        self,
        page_params: PageParams,
        filter_params: FilterParams | None = None,
        profile: str = "default",
    ) -> PageResult[RowT]:
        _ = filter_params
        _ = profile
        self.list_paginated_calls += 1
        values = list(self.storage.values())
        window = values[page_params.offset : page_params.offset + page_params.limit]
        return build_page_result(window, len(values), page_params)

    async def list_with_query(
        self,
        query: QuerySpec,
        profile: str = "default",
    ) -> PageResult[RowT] | CursorResult[RowT]:
        _ = profile
        self.list_with_query_calls += 1
        self.queries.append(query)
        matched = self._matching_rows(query)
        if query.pagination is PaginationMode.CURSOR:
            return CursorResult(items=tuple(matched), next_cursor=None, has_next=False)
        page_params = PageParams(page=query.page, limit=query.limit)
        return build_page_result(matched, len(matched), page_params)

    def _matching_rows(self, query: QuerySpec) -> list[RowT]:
        rows = list(reversed(list(self.storage.values())))
        if query.filters is None:
            return rows[: query.limit]
        wanted: set[Any] = set()
        for spec in query.filters.filters:
            if spec.field != "id" or spec.op is not FilterOp.IN:
                raise ValueError(f"unsupported filter: {spec}")
            wanted.update(spec.value)
        return [row for row in rows if row.id in wanted][: query.limit]

    async def create(self, data: WidgetCreate) -> RowT:
        raise NotImplementedError("the counting double is read-only")

    async def update(self, obj_id: Any, data: WidgetUpdate) -> RowT | None:
        raise NotImplementedError("the counting double is read-only")

    async def delete(self, obj_id: Any) -> bool:
        raise NotImplementedError("the counting double is read-only")


class RestrictedFilterRepository(CountingRepository[Widget]):
    """Repository double whose filter allowlist does not include ``id``.

    Filtering outside the allowlist raises, exactly as the SQLAlchemy query
    compiler does, so a caller cannot satisfy this double by catching the
    error and retrying.
    """

    allowed_filter_fields = frozenset({"name"})

    async def list_with_query(
        self,
        query: QuerySpec,
        profile: str = "default",
    ) -> PageResult[Widget] | CursorResult[Widget]:
        for spec in query.filters.filters if query.filters else ():
            if spec.field.split(".")[0] not in self.allowed_filter_fields:
                raise UnsafeFilterError(spec.field)
        return await super().list_with_query(query, profile=profile)


class GatedRepository(CountingRepository[Widget]):
    """Repository double whose reads block until the test opens the gate.

    Holding the reads inside the wrapper is what makes a stampede observable:
    several callers can be in flight for the same key at the same time, which
    a repository answering immediately never allows.

    Attributes:
        gate: Cleared at construction; every read waits on it.
        failure: Raised by the reads once the gate opens, when set.
        custom_calls: Number of ``find_names`` calls received.
        completed_calls: Number of reads that got past the gate, which tells a
            cancelled read apart from one that outlived its caller.
        caller_scoped_session: Reported by ``has_caller_scoped_session``, as a
            SQLAlchemy repository does inside a transaction.
    """

    def __init__(self, rows: Sequence[Widget]) -> None:
        super().__init__(rows, Widget)
        self.gate = asyncio.Event()
        self.failure: Exception | None = None
        self.custom_calls = 0
        self.completed_calls = 0
        self.caller_scoped_session = False

    def has_caller_scoped_session(self) -> bool:
        """Whether a read would run inside a session owned by the caller."""
        return self.caller_scoped_session

    async def get_by_id(self, obj_id: Any, profile: str = "default") -> Widget | None:
        _ = profile
        self.get_by_id_calls += 1
        await self._pass_gate()
        return self.storage.get(obj_id)

    @cache_query(scope="list")
    async def find_names(self, prefix: str) -> list[str]:
        """Custom cached read, gated like ``get_by_id``."""
        self.custom_calls += 1
        await self._pass_gate()
        return [row.name for row in self.storage.values() if row.name.startswith(prefix)]

    async def _pass_gate(self) -> None:
        await self.gate.wait()
        if self.failure is not None:
            raise self.failure
        self.completed_calls += 1


class CachedEnv(NamedTuple, Generic[RowT]):
    """A cached repository together with the doubles it was built on."""

    repository: CountingRepository[RowT]
    backend: CountingCacheBackend
    resolver: GenerationalDependencyResolver
    wrapper: CachedRepository[RowT, WidgetCreate, WidgetUpdate, Any]


def wrap_with_cache(
    repository: CountingRepository[RowT],
    config: CacheConfig,
) -> CachedEnv[RowT]:
    """Wrap *repository* in a cached repository over a counting backend.

    Args:
        repository: Inner repository double.
        config: Cache configuration under test.

    Returns:
        The wrapper and every double it talks to.
    """
    backend = CountingCacheBackend()
    resolver = GenerationalDependencyResolver(backend)
    wrapper: CachedRepository[RowT, WidgetCreate, WidgetUpdate, Any] = CachedRepository(
        repository,
        config=config,
        cache=backend,
        dependency_resolver=resolver,
    )
    return CachedEnv(repository, backend, resolver, wrapper)
