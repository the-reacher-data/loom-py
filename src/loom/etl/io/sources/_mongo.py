"""MongoDB source — FromMongo builder, SourceRef, and MongoSourceReader."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Literal

import msgspec
import polars as pl

from loom.core.expr.nodes import AndExpr, ExprNode
from loom.etl.declarative.source._specs import MongoSourceSpec
from loom.etl.io.sources._mongo_bson import normalize_bson_doc
from loom.etl.io.sources._mongo_predicate import predicate_to_mongo

_log = logging.getLogger(__name__)

_COLLECTION_RE = re.compile(r"^[a-zA-Z0-9_\-]+$")
_EXTRA_FIELDS_MODES = frozenset({"ignore", "warn", "capture", "error"})


@dataclass(frozen=True)
class SourceRef:
    """Reference to a column in another step's output, used inside ``isin()``.

    When used as ``InExpr.values``, the executor resolves this to a concrete
    tuple of values via ``materialize_filter()`` before ``MongoSourceReader.read()``
    is called — the reader never sees a ``SourceRef`` in the filter.

    Args:
        source: A builder (e.g. ``FromTemp``) or compiled spec that produces
                the DataFrame containing the values.
        col:    Column name to extract from the resolved DataFrame.

    Example::

        FromMongo("orders").where(col("_id").isin(SourceRef(FromTemp("ids"), col="order_id")))
    """

    source: Any
    col: str


class FromMongo:
    """Declare a MongoDB collection as an ETL source.

    Supports full-collection snapshots and id-based lookups — the lookup
    pattern is simply a ``where(col("_id").isin(SourceRef(...)))`` filter.

    Args:
        collection: MongoDB collection name.  Must match ``^[a-zA-Z0-9_\\-]+$``.

    Examples::

        # Full snapshot with param-driven filter
        FromMongo("orders").where(col("status") == "active")

        # Id-based lookup from another step's output
        FromMongo("orders").where(col("_id").isin(SourceRef(FromTemp("order_ids"), col="id")))

        # Snapshot with schema
        class BuildOrders(ETLStep[P]):
            orders = FromMongo("orders").with_schema(OrderDoc)
            target = IntoTable("landing.orders").replace_partitions("year", "month")
    """

    __slots__ = (
        "_collection",
        "_filter",
        "_projection",
        "_schema_type",
        "_extra_fields_mode",
        "_batch_size",
        "_limit",
    )

    def __init__(self, collection: str) -> None:
        if not _COLLECTION_RE.match(collection):
            raise ValueError(
                f"Invalid MongoDB collection name {collection!r}. "
                "Only letters, digits, underscores and hyphens are allowed."
            )
        self._collection: str = collection
        self._filter: ExprNode | None = None
        self._projection: tuple[str, ...] | None = None
        self._schema_type: type | None = None
        self._extra_fields_mode: Literal["ignore", "warn", "capture", "error"] = "error"
        self._batch_size: int = 10_000
        self._limit: int | None = None

    def where(self, filter: ExprNode) -> FromMongo:
        """Filter documents using the col()/params DSL.

        Multiple ``.where()`` calls are AND-ed together.
        Values may reference params (``params.run_date``) or another step's
        output (``SourceRef(FromTemp("ids"), col="id")``).
        """
        combined = AndExpr(left=self._filter, right=filter) if self._filter is not None else filter
        return self._clone(_filter=combined)

    def project(self, *fields: str) -> FromMongo:
        """Server-side projection — include only these fields.

        Args:
            fields: Field names to include.  ``$`` operator expressions are not allowed.
        """
        for f in fields:
            if "$" in f:
                raise ValueError(
                    f"Projection field {f!r} contains '$' — operator expressions "
                    "are not allowed in projection."
                )
        return self._clone(_projection=fields)

    def with_schema(self, schema_type: type) -> FromMongo:
        """Attach a msgspec.Struct schema for document field validation."""
        return self._clone(_schema_type=schema_type)

    def on_extra_fields(self, mode: Literal["ignore", "warn", "capture", "error"]) -> FromMongo:
        """Control how document fields absent from the schema are handled."""
        if mode not in _EXTRA_FIELDS_MODES:
            raise ValueError(
                f"on_extra_fields mode {mode!r} is not valid. "
                f"Choose one of: {sorted(_EXTRA_FIELDS_MODES)}"
            )
        return self._clone(_extra_fields_mode=mode)

    def batch_size(self, n: int) -> FromMongo:
        """Override the pymongo cursor batch size (1–50 000)."""
        if not 1 <= n <= 50_000:
            raise ValueError(f"batch_size must be between 1 and 50000, got {n}")
        return self._clone(_batch_size=n)

    def limit(self, n: int) -> FromMongo:
        """Limit the number of documents returned — for dev/CI only."""
        if n < 1:
            raise ValueError(f"limit must be >= 1, got {n}")
        return self._clone(_limit=n)

    def _to_spec(self, alias: str) -> MongoSourceSpec:
        return MongoSourceSpec(
            alias=alias,
            collection=self._collection,
            filter=self._filter,
            projection=self._projection,
            schema_type=self._schema_type,
            extra_fields_mode=self._extra_fields_mode,
            batch_size=self._batch_size,
            limit=self._limit,
        )

    def _clone(self, **overrides: Any) -> FromMongo:
        new = object.__new__(FromMongo)
        for slot in self.__slots__:
            object.__setattr__(new, slot, overrides.get(slot, getattr(self, slot)))
        return new

    def __repr__(self) -> str:
        return f"FromMongo({self._collection!r})"


class MongoSourceReader:
    """Reads a :class:`~loom.etl.declarative.source.MongoSourceSpec` into a
    Polars ``DataFrame`` via pymongo.

    The *client* and *database* are injected at construction time so the reader
    is fully testable without a live MongoDB server.

    Args:
        client:   A pymongo ``MongoClient`` (or any object supporting
                  ``client[db][collection]`` access).  Pass ``None`` when
                  registering in a provider — ``read()`` will raise if called
                  without a real client.
        database: MongoDB database name.

    Example::

        from pymongo import MongoClient
        reader = MongoSourceReader(MongoClient(uri), database="app")
    """

    def __init__(self, client: Any = None, database: str = "") -> None:
        self._client = client
        self._database = database

    def read(self, spec: Any, params_instance: Any, /) -> pl.DataFrame:
        """Read the collection described by *spec* and return a Polars DataFrame.

        Args:
            spec:            A :class:`~loom.etl.declarative.source.MongoSourceSpec`.
            params_instance: ETL params instance for resolving param expressions.
                             All ``SourceRef`` nodes must be resolved by
                             ``materialize_filter()`` before this call.
        """
        if self._client is None:
            raise RuntimeError(
                "MongoSourceReader: no client configured. "
                "Pass a pymongo MongoClient and database name at construction time."
            )
        collection = self._client[self._database][spec.collection]
        filter_dict = predicate_to_mongo(spec.filter, params_instance) if spec.filter else {}
        projection = dict.fromkeys(spec.projection, 1) if spec.projection else None

        cursor = collection.find(filter_dict, projection, batch_size=spec.batch_size)
        if spec.limit is not None:
            cursor = cursor.limit(spec.limit)

        batches: list[pl.DataFrame] = []
        batch: list[dict[str, Any]] = []
        for doc in cursor:
            batch.append(normalize_bson_doc(doc))
            if len(batch) >= spec.batch_size:
                batches.append(pl.from_dicts(batch))
                batch = []
        if batch:
            batches.append(pl.from_dicts(batch))

        if not batches:
            return _empty_frame(spec)

        df = pl.concat(batches, rechunk=False) if len(batches) > 1 else batches[0]
        if spec.schema_type is not None:
            df = _apply_schema(df, spec.schema_type, spec.extra_fields_mode)
        return df


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _empty_frame(spec: Any) -> pl.DataFrame:
    if spec.schema_type is None:
        return pl.DataFrame()
    try:
        fields = msgspec.structs.fields(spec.schema_type)
        return pl.DataFrame(schema={f.name: pl.String for f in fields})
    except Exception:
        return pl.DataFrame()


def _apply_schema(df: pl.DataFrame, schema_type: type, mode: str) -> pl.DataFrame:
    """Drop or capture document fields not declared in *schema_type*."""
    try:
        declared = {f.name for f in msgspec.structs.fields(schema_type)}
    except Exception:
        return df

    extra_cols = [c for c in df.columns if c not in declared]
    if not extra_cols:
        return df

    if mode == "error":
        raise ValueError(
            f"MongoSourceReader: documents contain fields not declared in "
            f"{schema_type.__name__}: {extra_cols}. "
            "Use on_extra_fields('ignore'), ('warn'), or ('capture') to suppress."
        )
    if mode == "warn":
        _log.warning(
            "MongoSourceReader: dropping %d undeclared field(s) from %s: %s",
            len(extra_cols),
            schema_type.__name__,
            extra_cols,
        )
        return df.drop(extra_cols)
    if mode == "capture":
        extra_series = df.select(extra_cols).map_rows(
            lambda row: json.dumps(dict(zip(extra_cols, row, strict=False))),
            return_dtype=pl.String,
        )["map"]
        return df.drop(extra_cols).with_columns(extra_series.alias("_extra"))
    # "ignore"
    return df.drop(extra_cols)


__all__ = ["FromMongo", "MongoSourceReader", "MongoSourceSpec", "SourceRef"]
