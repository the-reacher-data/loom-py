"""MongoDB source reader."""

from __future__ import annotations

import json
import logging
from typing import Any

import msgspec
import polars as pl

from loom.etl.declarative.source._from_mongo import FromMongo, SourceRef
from loom.etl.declarative.source._specs import MongoSourceSpec
from loom.etl.io.sources._mongo_bson import normalize_bson_doc
from loom.etl.io.sources._mongo_predicate import predicate_to_mongo

_log = logging.getLogger(__name__)


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
