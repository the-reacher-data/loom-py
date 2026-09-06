"""In-memory stand-in for pymongo's asynchronous collection and client.

Scope is deliberately narrow (plan decision 10): the calls the Mongo
repository and unit of work issue, over the operators
:class:`~loom.core.repository.mongo.query_compiler.MongoQueryCompiler` emits.
Any other query or update operator raises :class:`NotImplementedError` naming
it, so a drift between compiler and fake is visible instead of silently
matching nothing.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from types import TracebackType
from typing import Any

from bson import ObjectId
from pymongo.errors import BulkWriteError, DuplicateKeyError
from pymongo.results import DeleteResult, InsertManyResult, InsertOneResult, UpdateResult

_DUPLICATE_KEY = 11000
_Document = dict[str, Any]


def _ordered(value: Any, other: Any, verdict: Callable[[Any, Any], bool]) -> bool:
    """Mongo type bracketing: an ordering comparison never matches ``null``."""
    return value is not None and verdict(value, other)


_FIELD_OPS: dict[str, Callable[[Any, Any], bool]] = {
    "$ne": lambda value, other: value != other,
    "$gt": lambda value, other: _ordered(value, other, lambda a, b: a > b),
    "$gte": lambda value, other: _ordered(value, other, lambda a, b: a >= b),
    "$lt": lambda value, other: _ordered(value, other, lambda a, b: a < b),
    "$lte": lambda value, other: _ordered(value, other, lambda a, b: a <= b),
    "$in": lambda value, other: value in other,
}


def _regex_matches(value: Any, condition: Mapping[str, Any]) -> bool:
    flags = re.IGNORECASE if "i" in condition.get("$options", "") else 0
    return isinstance(value, str) and re.search(condition["$regex"], value, flags) is not None


def _field_matches(value: Any, condition: Any) -> bool:
    if not isinstance(condition, Mapping):
        return value == condition
    for operator in condition:
        if operator == "$options":
            continue
        if operator == "$regex":
            if not _regex_matches(value, condition):
                return False
            continue
        verdict = _FIELD_OPS.get(operator)
        if verdict is None:
            raise NotImplementedError(
                f"Fake collection does not evaluate field operator {operator!r}"
            )
        if not verdict(value, condition[operator]):
            return False
    return True


def _matches(document: _Document, query: Mapping[str, Any]) -> bool:
    for key, condition in query.items():
        if key == "$and":
            if not all(_matches(document, clause) for clause in condition):
                return False
        elif key == "$or":
            if not any(_matches(document, clause) for clause in condition):
                return False
        elif key.startswith("$"):
            raise NotImplementedError(f"Fake collection does not evaluate operator {key!r}")
        elif not _field_matches(document.get(key), condition):
            return False
    return True


def _sort_key(field: str, document: _Document) -> tuple[bool, Any]:
    """Order ``null`` before every other value, as MongoDB does."""
    value = document.get(field)
    return (value is not None, value)


class FakeCursor:
    """Result of :meth:`FakeCollection.find`; evaluated lazily by :meth:`to_list`."""

    def __init__(self, documents: list[_Document]) -> None:
        self._documents = documents
        self._sort: list[tuple[str, int]] = []
        self._skip = 0
        self._limit = 0

    def sort(
        self, key_or_list: str | list[tuple[str, int]], direction: int | None = None
    ) -> FakeCursor:
        if isinstance(key_or_list, str):
            self._sort = [(key_or_list, 1 if direction is None else direction)]
        else:
            self._sort = list(key_or_list)
        return self

    def skip(self, skip: int) -> FakeCursor:
        self._skip = skip
        return self

    def limit(self, limit: int) -> FakeCursor:
        self._limit = limit
        return self

    async def to_list(self, length: int | None = None) -> list[_Document]:
        documents = list(self._documents)
        for field, direction in reversed(self._sort):
            documents.sort(key=lambda doc: _sort_key(field, doc), reverse=direction == -1)
        documents = documents[self._skip :]
        if self._limit:
            documents = documents[: self._limit]
        if length is not None:
            documents = documents[:length]
        return [dict(doc) for doc in documents]


class FakeCollection:
    """In-memory stand-in for :class:`pymongo.asynchronous.collection.AsyncCollection`.

    Documents are keyed by ``_id``; inserting a duplicate raises a real
    :class:`pymongo.errors.DuplicateKeyError` (``insert_one``) or
    :class:`pymongo.errors.BulkWriteError` (``insert_many``, ordered: earlier
    documents stay inserted), exactly as the driver does. ``session``
    arguments are accepted and ignored.
    """

    def __init__(self) -> None:
        self.documents: dict[Any, _Document] = {}
        self.last_session: object | None = None

    async def insert_one(
        self, document: _Document, session: object | None = None
    ) -> InsertOneResult:
        self.last_session = session
        return InsertOneResult(self._store(document), acknowledged=True)

    async def insert_many(
        self, documents: Iterable[_Document], ordered: bool = True, session: object | None = None
    ) -> InsertManyResult:
        self.last_session = session
        if not ordered:
            raise NotImplementedError("fake insert_many supports ordered inserts only")
        inserted: list[Any] = []
        for index, document in enumerate(documents):
            try:
                inserted.append(self._store(document))
            except DuplicateKeyError as exc:
                raise self._bulk_error(index, exc, inserted) from exc
        return InsertManyResult(inserted, acknowledged=True)

    async def find_one(
        self, filter: Mapping[str, Any] | None = None, session: object | None = None
    ) -> _Document | None:
        self.last_session = session
        found = await self.find(filter, session).limit(1).to_list()
        return found[0] if found else None

    def find(
        self, filter: Mapping[str, Any] | None = None, session: object | None = None
    ) -> FakeCursor:
        self.last_session = session
        return FakeCursor(self._select(filter or {}))

    async def count_documents(
        self, filter: Mapping[str, Any], session: object | None = None
    ) -> int:
        self.last_session = session
        return len(self._select(filter))

    async def update_one(
        self, filter: Mapping[str, Any], update: Mapping[str, Any], session: object | None = None
    ) -> UpdateResult:
        self.last_session = session
        unknown = [operator for operator in update if operator != "$set"]
        if unknown:
            raise NotImplementedError(
                f"Fake collection does not apply update operator {unknown[0]!r}"
            )
        matched = self._select(filter)[:1]
        for document in matched:
            document.update(update.get("$set", {}))
        return UpdateResult({"n": len(matched), "nModified": len(matched)}, acknowledged=True)

    async def delete_one(
        self, filter: Mapping[str, Any], session: object | None = None
    ) -> DeleteResult:
        return await self._delete(self._select(filter)[:1], session)

    async def delete_many(
        self, filter: Mapping[str, Any], session: object | None = None
    ) -> DeleteResult:
        return await self._delete(self._select(filter), session)

    async def _delete(self, matched: list[_Document], session: object | None) -> DeleteResult:
        self.last_session = session
        for document in matched:
            del self.documents[document["_id"]]
        return DeleteResult({"n": len(matched)}, acknowledged=True)

    def _select(self, query: Mapping[str, Any]) -> list[_Document]:
        return [doc for doc in self.documents.values() if _matches(doc, query)]

    def _store(self, document: _Document) -> Any:
        document.setdefault("_id", ObjectId())
        key = document["_id"]
        if key in self.documents:
            raise DuplicateKeyError(f"E11000 duplicate key error: _id {key!r}", code=_DUPLICATE_KEY)
        self.documents[key] = document
        return key

    @staticmethod
    def _bulk_error(index: int, exc: DuplicateKeyError, inserted: list[Any]) -> BulkWriteError:
        write_error = {"index": index, "code": _DUPLICATE_KEY, "errmsg": str(exc)}
        return BulkWriteError({"nInserted": len(inserted), "writeErrors": [write_error]})


class FakeTransaction:
    """Block entered via ``async with await session.start_transaction()``.

    Delegates to the session: commits on clean exit, aborts when an
    exception propagates.
    """

    def __init__(self, session: FakeSession) -> None:
        self._session = session

    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc_type is None:
            await self._session.commit_transaction()
        else:
            await self._session.abort_transaction()


class FakeSession:
    """Stand-in for :class:`pymongo.asynchronous.client_session.AsyncClientSession`.

    Supports both driver shapes: the ``async with await start_transaction()``
    block, and the explicit ``commit_transaction`` / ``abort_transaction`` /
    ``end_session`` calls the unit of work issues.
    """

    def __init__(self, client: FakeMongoClient) -> None:
        self._client = client
        self.in_transaction = False
        self.ended = False
        self._snapshot: dict[str, dict[Any, _Document]] = {}

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None

    async def start_transaction(self) -> FakeTransaction:
        if self._client.start_transaction_error is not None:
            raise self._client.start_transaction_error
        self.in_transaction = True
        self._snapshot = {
            name: dict(collection.documents)
            for name, collection in self._client.collections.items()
        }
        return FakeTransaction(self)

    async def commit_transaction(self) -> None:
        self._client.committed += 1
        self.in_transaction = False

    async def abort_transaction(self) -> None:
        """Abort, restoring every collection to its state at ``start_transaction``."""
        if self._client.abort_error is not None:
            raise self._client.abort_error
        for name, collection in self._client.collections.items():
            collection.documents = dict(self._snapshot.get(name, {}))
        self._client.aborted += 1
        self.in_transaction = False

    async def end_session(self) -> None:
        self.ended = True
        if self._client.end_error is not None:
            raise self._client.end_error


class FakeAdmin:
    """The ``admin`` database: answers ``ping`` unless the client is told to fail."""

    def __init__(self, client: FakeMongoClient) -> None:
        self._client = client

    async def command(self, command: str) -> dict[str, Any]:
        if self._client.ping_error is not None:
            raise self._client.ping_error
        return {"ok": 1.0}


class FakeDatabase:
    """Named database of a :class:`FakeMongoClient`; collections are shared per client."""

    def __init__(self, client: FakeMongoClient, name: str) -> None:
        self._client = client
        self.name = name

    def __getitem__(self, name: str) -> FakeCollection:
        return self._client.collection(name)


class FakeMongoClient:
    """Stand-in for :class:`pymongo.asynchronous.mongo_client.AsyncMongoClient`.

    Mirrors the driver's shapes: ``start_session()`` is synchronous and
    returns an async context manager; ``start_transaction()`` is awaited and
    returns one; ``client[database][collection]`` addresses a collection;
    ``admin.command("ping")`` answers the readiness probe. Commits and aborts
    are counted; an explicit ``abort_transaction`` restores the documents
    present when the transaction started.
    """

    def __init__(self) -> None:
        self.collections: dict[str, FakeCollection] = {}
        self.sessions: list[FakeSession] = []
        self.committed = 0
        self.aborted = 0
        self.closed = False
        self.ping_error: Exception | None = None
        self.start_transaction_error: Exception | None = None
        self.abort_error: Exception | None = None
        self.end_error: Exception | None = None

    @property
    def admin(self) -> FakeAdmin:
        return FakeAdmin(self)

    def __getitem__(self, name: str) -> FakeDatabase:
        return FakeDatabase(self, name)

    def collection(self, name: str) -> FakeCollection:
        return self.collections.setdefault(name, FakeCollection())

    def start_session(self) -> FakeSession:
        session = FakeSession(self)
        self.sessions.append(session)
        return session

    async def close(self) -> None:
        self.closed = True
