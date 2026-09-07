"""Counting doubles for the cached repository suites.

The cache read path is judged by how many round trips it makes, which cannot
be observed from its return values, so both collaborators count their calls.
The cache backend serializes with msgpack exactly like the production
serializer: a cached index round-trips its ids through it, which turns a
``UUID`` or a ``date`` into a string, and the read path has to cope with that.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Sequence
from datetime import date
from typing import Any, Generic, NamedTuple, TypeVar, cast
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


class Stats(msgspec.Struct):
    """Aggregate returned by the cached custom reads under test."""

    total: int
    label: str


class CustomCallCountingRepository(CountingRepository[RowT], Generic[RowT]):
    """Repository double that counts its custom reads by method name.

    Attributes:
        custom_calls: Number of calls received per method name.
    """

    def __init__(self, rows: Sequence[RowT], row_type: type[RowT]) -> None:
        super().__init__(rows, row_type)
        self.custom_calls: Counter[str] = Counter()

    def reset_counters(self) -> None:
        """Zero the inherited counters and the custom read counters."""
        super().reset_counters()
        self.custom_calls.clear()


class CodecRepository(CustomCallCountingRepository[RowT], Generic[RowT]):
    """Repository double whose cached custom reads cover the codec grammar.

    Each read counts itself so a test can tell a cache hit from a second
    repository call, and returns a value derived from the stored rows so the
    assertions do not depend on a constant.
    """

    @cache_query(scope="list")
    async def stats(self) -> Stats:
        """Return one struct."""
        self.custom_calls["stats"] += 1
        return Stats(total=len(self.storage), label="all")

    @cache_query(scope="list")
    async def stats_list(self) -> list[Stats]:
        """Return a list of structs."""
        self.custom_calls["stats_list"] += 1
        return [
            Stats(total=index, label=str(row.id)) for index, row in enumerate(self.storage.values())
        ]

    @cache_query(scope="list")
    async def stats_tuple(self) -> tuple[Stats, ...]:
        """Return a tuple of structs, which builtins conversion flattens."""
        self.custom_calls["stats_tuple"] += 1
        return tuple(
            Stats(total=index, label=str(row.id)) for index, row in enumerate(self.storage.values())
        )

    @cache_query(scope="list")
    async def stats_for(self, label: str) -> Stats | None:
        """Return a struct for a known label, ``None`` otherwise."""
        self.custom_calls["stats_for"] += 1
        if label != "known":
            return None
        return Stats(total=len(self.storage), label=label)

    @cache_query(scope="list")
    async def total(self) -> int:
        """Return a scalar."""
        self.custom_calls["total"] += 1
        return len(self.storage)

    @cache_query(scope="list")
    async def newest(self) -> RowT:
        """Return the model itself, declared through the class type variable."""
        self.custom_calls["newest"] += 1
        return list(self.storage.values())[-1]

    @cache_query(scope="list")
    async def late_stats(self) -> LateStats:
        """Return a struct declared after this class, a real forward reference."""
        self.custom_calls["late_stats"] += 1
        return LateStats(count=len(self.storage))

    @cache_query(scope="list")
    async def unresolvable(
        self,
    ) -> UnknownStats:  # noqa: F821  # pyright: ignore[reportUndefinedVariable]
        """Return a value whose annotation names nothing importable here."""
        self.custom_calls["unresolvable"] += 1
        return Stats(total=len(self.storage), label="unresolvable")

    @cache_query(scope="list")
    async def out_of_grammar(self) -> dict[str, int]:
        """Return a mapping, which is outside the supported grammar."""
        self.custom_calls["out_of_grammar"] += 1
        return {"total": len(self.storage)}

    @cache_query(scope="list")
    async def unannotated(self):  # type: ignore[no-untyped-def]
        """Return a struct without declaring it."""
        self.custom_calls["unannotated"] += 1
        return Stats(total=len(self.storage), label="unannotated")


class LateStats(msgspec.Struct):
    """Struct declared after the repository that returns it."""

    count: int


class DetailedStats(Stats):
    """Subclass of the declared return type, narrowed away by the codec."""

    source: str = "detail"


class EvolvedStats(msgspec.Struct):
    """Return type of a later deployment: one more required field.

    A payload written against :class:`Stats` cannot be decoded into it, which
    is what a cached entry looks like after a struct gains a field.
    """

    total: int
    label: str
    source: str


class LyingRepository(CodecRepository[RowT], Generic[RowT]):
    """Repository double whose cached reads contradict their annotations.

    One returns a value the declared struct cannot validate, the other a value
    that cannot be rendered as builtins at all.  Both lie on purpose, so the
    casts are the point rather than an oversight.
    """

    @cache_query(scope="list")
    async def wrong_shape(self) -> Stats:
        """Return a mapping that is missing a field of the declared struct."""
        self.custom_calls["wrong_shape"] += 1
        return cast(Stats, {"total": len(self.storage)})

    @cache_query(scope="list")
    async def unrenderable(self) -> Stats:
        """Return a value no builtins rendering can describe."""
        self.custom_calls["unrenderable"] += 1
        return cast(Stats, object())

    @cache_query(scope="list")
    async def narrowing(self) -> Stats:
        """Return a subclass of the declared struct."""
        self.custom_calls["narrowing"] += 1
        return DetailedStats(total=len(self.storage), label="detail")


class EvolvedRepository(CustomCallCountingRepository[RowT], Generic[RowT]):
    """Later deployment of :class:`CodecRepository`: ``stats`` gained a field.

    It keeps the entity name, the method name and the arguments of the older
    class, so it computes the same cache key and reads the payload the older
    one wrote.
    """

    @cache_query(scope="list")
    async def stats(self) -> EvolvedStats:
        """Return the struct of the later deployment."""
        self.custom_calls["stats"] += 1
        return EvolvedStats(total=len(self.storage), label="all", source="v2")


class UnmarkedOverrideRepository(CodecRepository[RowT], Generic[RowT]):
    """Subclass that overrides cached reads and drops the ``@cache_query`` marker.

    The overridden reads are no longer cached, so the base class's codecs — and
    the deprecation its annotations would earn — must not be registered for
    them.
    """

    async def stats(self) -> Stats:
        """Return the struct, uncached."""
        self.custom_calls["stats"] += 1
        return Stats(total=len(self.storage), label="override")

    async def out_of_grammar(self) -> dict[str, int]:
        """Return a mapping, uncached, so its annotation deprecates nothing."""
        self.custom_calls["out_of_grammar"] += 1
        return {"total": len(self.storage)}


def rewrap_with_cache(
    env: CachedEnv[RowT],
    repository: CountingRepository[RowT],
    config: CacheConfig,
) -> CachedRepository[RowT, WidgetCreate, WidgetUpdate, Any]:
    """Wrap *repository* over the backend and resolver of an existing env.

    Sharing the collaborators is what puts two deployments in front of one
    cache: the entries the first wrapper wrote are the ones the second reads.

    Args:
        env: Environment whose backend and resolver are reused.
        repository: Inner repository of the second wrapper.
        config: Cache configuration under test.

    Returns:
        The second cached repository.
    """
    return CachedRepository(
        repository,
        config=config,
        cache=env.backend,
        dependency_resolver=env.resolver,
    )
