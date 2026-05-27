"""MongoDB source — FromMongo builder and MongoLookupSourceSpec."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from loom.etl.declarative.source._specs import SourceKind

_EXTRA_FIELDS_MODES = frozenset({"ignore", "warn", "capture", "error"})


@dataclass(frozen=True)
class MongoLookupSourceSpec:
    """Compiled spec for a MongoDB id-based lookup source.

    Produced by :meth:`FromMongo._to_spec`. Consumed by the executor
    and :class:`MongoSourceReader` — never constructed directly in user code.
    """

    alias: str
    collection: str
    id_source: Any
    id_col: str = "_id"
    schema_type: type | None = None
    extra_fields_mode: Literal["ignore", "warn", "capture", "error"] = "ignore"
    batch_size: int = field(default=1000)

    @property
    def kind(self) -> SourceKind:
        """Source kind — always :attr:`SourceKind.MONGO_LOOKUP`."""
        return SourceKind.MONGO_LOOKUP


class FromMongo:
    """Declare a MongoDB collection as an ETL source via id-based lookup.

    The lookup ids are driven by a prior ETL result (temp table, ClickHouse
    query, etc.) passed to :meth:`where_id_in`.

    Args:
        collection: MongoDB collection name.

    Example::

        ids = FromMongo("motos")
            .where_id_in(FromTemp("cdc_ids"), id_col="document_id")
            .with_schema(MotoSchema)
    """

    __slots__ = (
        "_collection",
        "_id_source",
        "_id_col",
        "_schema_type",
        "_extra_fields_mode",
        "_batch_size",
    )

    def __init__(self, collection: str) -> None:
        self._collection: str = collection
        self._id_source: Any = None
        self._id_col: str = "_id"
        self._schema_type: type | None = None
        self._extra_fields_mode: Literal["ignore", "warn", "capture", "error"] = "ignore"
        self._batch_size: int = 1000

    def where_id_in(self, source: Any, *, id_col: str = "_id") -> FromMongo:
        """Filter the collection to documents whose id appears in *source*.

        Args:
            source: A builder (e.g. ``FromTemp``) or compiled spec that
                    produces the set of ids to look up.  Must NOT be a
                    :class:`MongoLookupSourceSpec` — that would create
                    unbounded recursive lookups (C2 guard).
            id_col: Column in *source* that holds the document ids.
        """
        if isinstance(source, MongoLookupSourceSpec):
            raise TypeError(
                "MongoLookupSourceSpec cannot be used as id_source — "
                "recursive Mongo lookups are not supported."
            )
        if id_col.startswith("$"):
            raise ValueError(
                f"id_col {id_col!r} is not a valid field name — "
                "MongoDB operator expressions are not allowed."
            )
        return self._clone(_id_source=source, _id_col=id_col)

    def with_schema(self, schema_type: type) -> FromMongo:
        """Attach a msgspec.Struct schema for decoding documents."""
        return self._clone(_schema_type=schema_type)

    def on_extra_fields(self, mode: Literal["ignore", "warn", "capture", "error"]) -> FromMongo:
        """Control how fields absent from the schema are handled."""
        if mode not in _EXTRA_FIELDS_MODES:
            raise ValueError(
                f"on_extra_fields mode {mode!r} is not valid. "
                f"Choose one of: {sorted(_EXTRA_FIELDS_MODES)}"
            )
        return self._clone(_extra_fields_mode=mode)

    def batch_size(self, n: int) -> FromMongo:
        """Override the number of ids sent per MongoDB $in query."""
        return self._clone(_batch_size=n)

    def _to_spec(self, alias: str) -> MongoLookupSourceSpec:
        """Compile to a frozen spec for the executor."""
        id_source = self._id_source
        if hasattr(id_source, "_to_spec"):
            id_source = id_source._to_spec("__id_source__")
        return MongoLookupSourceSpec(
            alias=alias,
            collection=self._collection,
            id_source=id_source,
            id_col=self._id_col,
            schema_type=self._schema_type,
            extra_fields_mode=self._extra_fields_mode,
            batch_size=self._batch_size,
        )

    def _clone(self, **overrides: Any) -> FromMongo:
        new = object.__new__(FromMongo)
        for slot in self.__slots__:
            object.__setattr__(new, slot, overrides.get(slot, getattr(self, slot)))
        return new

    def __repr__(self) -> str:
        return f"FromMongo({self._collection!r})"


class MongoSourceReader:
    """Reads a MongoLookupSourceSpec into a Polars DataFrame via PyMongo.

    Stub implementation — full batch id-lookup will be wired in T4.
    """

    def read(self, spec: Any, params_instance: Any, /) -> Any:
        raise NotImplementedError("MongoSourceReader not yet implemented")


__all__ = ["FromMongo", "MongoLookupSourceSpec", "MongoSourceReader"]
