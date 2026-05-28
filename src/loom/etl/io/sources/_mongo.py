"""MongoDB source reader — orchestrator."""

from __future__ import annotations

from typing import Any

import polars as pl

from loom.etl.backends.polars._dtype import loom_type_to_polars
from loom.etl.declarative.source._from_mongo import FromMongo, SourceRef
from loom.etl.declarative.source._specs import MongoSourceSpec
from loom.etl.io.sources._mongo_batch import (
    MongoBatchProcessor,
    align_to_schema,
    apply_declared_schema,
)
from loom.etl.io.sources._mongo_bson import normalize_bson_doc
from loom.etl.io.sources._mongo_predicate import predicate_to_mongo
from loom.etl.schema._schema import LoomDtype

_SCHEMA_INFERENCE_SAMPLE = 100


class MongoSourceReader:
    """Read a :class:`~loom.etl.declarative.source.MongoSourceSpec` into a Polars LazyFrame.

    MongoDB I/O is deferred until ``.collect()`` is called. With ``schema``
    declared, zero queries are issued during ``read()`` — the cursor opens lazily
    inside the scan plugin. Without ``schema``, up to
    :data:`_SCHEMA_INFERENCE_SAMPLE` documents are sampled eagerly to infer
    the registered schema; the full scan is still deferred.

    Args:
        client:   pymongo ``MongoClient`` (or compatible).
        database: MongoDB database name.
    """

    def __init__(
        self,
        client: Any = None,
        database: str = "",
    ) -> None:
        self._client = client
        self._database = database

    def read(self, spec: Any, params_instance: Any, /) -> pl.LazyFrame:
        from polars.io.plugins import register_io_source  # unstable in Polars 1.x

        if self._client is None:
            raise RuntimeError(
                "MongoSourceReader: no client configured. "
                "Pass a pymongo MongoClient and database name at construction time."
            )

        collection = self._client[self._database][spec.collection]
        filter_dict = predicate_to_mongo(spec.filter, params_instance) if spec.filter else {}
        projection = dict.fromkeys(spec.projection, 1) if spec.projection else None

        declared_schema, schema_str_fields = self._resolve_declared_schema(spec)
        registered_schema = self._registered_schema(
            spec, declared_schema, collection, filter_dict, projection, schema_str_fields
        )

        batch_processor = MongoBatchProcessor(
            schema_str_fields=schema_str_fields,
        )

        def _io_source(
            with_columns: list[str] | None,
            predicate: pl.Expr | None,
            n_rows: int | None,
            batch_size_hint: int | None,
        ) -> Any:
            yield from _scan(
                collection,
                filter_dict,
                projection,
                spec,
                batch_processor,
                declared_schema,
                registered_schema,
                with_columns,
                n_rows,
            )

        return register_io_source(
            _io_source,
            schema=registered_schema,
            # validate_schema=False: batch alignment already handles type coercion per batch.
            validate_schema=False,
            is_pure=False,
        )

    def _resolve_declared_schema(self, spec: Any) -> tuple[dict[str, pl.DataType], frozenset[str]]:
        if not spec.schema:
            return {}, frozenset()
        field_dtypes = {col.name: loom_type_to_polars(col.dtype) for col in spec.schema}
        str_field_names = frozenset(col.name for col in spec.schema if col.dtype == LoomDtype.UTF8)
        return (field_dtypes, str_field_names)

    def _registered_schema(
        self,
        spec: Any,
        declared_schema: dict[str, pl.DataType],
        collection: Any,
        filter_dict: dict[str, Any],
        projection: dict[str, Any] | None,
        schema_str_fields: frozenset[str],
    ) -> dict[str, pl.DataType]:
        if declared_schema:
            schema = dict(declared_schema)
            if spec.extra_fields_mode == "capture":
                schema["_extra"] = pl.String()
            return schema
        return _infer_schema_from_sample(
            collection, filter_dict, projection, spec, schema_str_fields
        )


# ---------------------------------------------------------------------------
# Cursor + scan helpers
# ---------------------------------------------------------------------------


def _normalize_null_dtypes(schema: dict[str, pl.DataType]) -> dict[str, pl.DataType]:
    """Promote Null → String and List(Null) → List(String) in schemaless inference."""
    result: dict[str, pl.DataType] = {}
    for name, dtype in schema.items():
        if dtype == pl.Null:
            result[name] = pl.String()
        elif dtype == pl.List(pl.Null):
            result[name] = pl.List(pl.String())
        else:
            result[name] = dtype
    return result


def _infer_schema_from_sample(
    collection: Any,
    filter_dict: dict[str, Any],
    projection: dict[str, Any] | None,
    spec: Any,
    schema_str_fields: frozenset[str],
) -> dict[str, pl.DataType]:
    cursor = collection.find(filter_dict, projection, batch_size=spec.batch_size)
    if spec.limit is not None:
        cursor = cursor.limit(spec.limit)

    docs: list[dict[str, Any]] = []
    for doc in cursor:
        docs.append(normalize_bson_doc(doc))
        if len(docs) >= _SCHEMA_INFERENCE_SAMPLE:
            break
    cursor.close()

    if not docs:
        return {}

    processor = MongoBatchProcessor(schema_str_fields=schema_str_fields)
    sample_frame = processor.build_frame(docs)
    raw = dict(zip(sample_frame.columns, sample_frame.dtypes, strict=True))
    return _normalize_null_dtypes(raw)


def _open_cursor(
    collection: Any,
    filter_dict: dict[str, Any],
    projection: dict[str, Any] | None,
    spec: Any,
    n_rows: int | None,
) -> Any:
    cursor = collection.find(filter_dict, projection, batch_size=spec.batch_size)
    effective_limit = spec.limit
    if n_rows is not None:
        effective_limit = min(effective_limit, n_rows) if effective_limit is not None else n_rows
    if effective_limit is not None:
        cursor = cursor.limit(effective_limit)
    return cursor


def _finalize_batch(
    frame: pl.DataFrame,
    declared_schema: dict[str, pl.DataType],
    registered_schema: dict[str, pl.DataType],
    extra_fields_mode: str,
    schema_name: str,
    with_columns: list[str] | None,
    n_rows: int | None,
) -> pl.DataFrame:
    if declared_schema:
        frame = apply_declared_schema(frame, declared_schema, extra_fields_mode, schema_name)
    else:
        frame = align_to_schema(frame, registered_schema)
    if with_columns:
        present = [c for c in with_columns if c in frame.columns]
        frame = frame.select(present) if present else frame.head(0)
    if n_rows is not None:
        frame = frame.head(n_rows)
    return frame


def _scan(
    collection: Any,
    filter_dict: dict[str, Any],
    projection: dict[str, Any] | None,
    spec: Any,
    batch_processor: MongoBatchProcessor,
    declared_schema: dict[str, pl.DataType],
    registered_schema: dict[str, pl.DataType],
    with_columns: list[str] | None,
    n_rows: int | None,
) -> Any:
    schema_name = spec.collection
    schema_overrides = declared_schema or registered_schema or None
    cursor = _open_cursor(collection, filter_dict, projection, spec, n_rows)
    remaining = n_rows
    batch: list[dict[str, Any]] = []

    for doc in cursor:
        batch.append(normalize_bson_doc(doc))
        if len(batch) >= spec.batch_size:
            frame = batch_processor.build_frame(
                batch, schema_overrides=schema_overrides, declared_schema=declared_schema or None
            )
            frame = _finalize_batch(
                frame,
                declared_schema,
                registered_schema,
                spec.extra_fields_mode,
                schema_name,
                with_columns,
                remaining,
            )
            if remaining is not None:
                remaining -= len(frame)
            yield frame
            batch = []
            if remaining is not None and remaining <= 0:
                return

    if batch:
        frame = batch_processor.build_frame(batch, schema_overrides=schema_overrides)
        frame = _finalize_batch(
            frame,
            declared_schema,
            registered_schema,
            spec.extra_fields_mode,
            schema_name,
            with_columns,
            remaining,
        )
        yield frame


__all__ = ["FromMongo", "MongoSourceReader", "MongoSourceSpec", "SourceRef"]
