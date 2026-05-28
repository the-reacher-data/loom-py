"""MongoDB source reader."""

from __future__ import annotations

import datetime
import json
import logging
import types
import typing
from typing import Any

import polars as pl

from loom.etl.declarative.source._from_mongo import FromMongo, SourceRef
from loom.etl.declarative.source._specs import MongoSourceSpec
from loom.etl.io.sources._mongo_bson import normalize_bson_doc
from loom.etl.io.sources._mongo_predicate import predicate_to_mongo

_log = logging.getLogger(__name__)

# Number of documents sampled during read() to infer schema when no schema_type is given.
_SCHEMA_INFERENCE_SAMPLE = 100

# Maximum recursion depth for _nested_has_conflict to prevent DoS via deeply nested documents.
_MAX_NESTED_DEPTH = 64


class MongoSourceReader:
    """Reads a :class:`~loom.etl.declarative.source.MongoSourceSpec` into a
    Polars ``LazyFrame`` via pymongo.

    MongoDB I/O is deferred until ``.collect()`` is called on the returned frame.
    The LazyFrame is backed by a custom scan source (``polars.io.plugins.register_io_source``);
    any ``.filter()`` chained on it executes in memory — server-side filtering is applied
    via ``spec.filter`` before cursor iteration.

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

    def read(self, spec: Any, params_instance: Any, /) -> pl.LazyFrame:
        """Read the collection described by *spec* and return a Polars LazyFrame.

        When ``spec.schema_type`` is set the registered schema is known upfront and
        no sampling query is issued during this call — the cursor is opened lazily
        inside the generator at ``.collect()`` time.

        When ``spec.schema_type`` is ``None`` a small sampling query
        (up to :data:`_SCHEMA_INFERENCE_SAMPLE` documents) is issued eagerly to
        infer the column schema before returning.  The full cursor scan is still
        deferred to ``.collect()``.

        Args:
            spec:            A :class:`~loom.etl.declarative.source.MongoSourceSpec`.
            params_instance: ETL params instance for resolving param expressions.
                             All ``SourceRef`` nodes must be resolved by
                             ``materialize_filter()`` before this call.
        """
        # polars.io.plugins is marked unstable in Polars 1.x.
        from polars.io.plugins import register_io_source

        if self._client is None:
            raise RuntimeError(
                "MongoSourceReader: no client configured. "
                "Pass a pymongo MongoClient and database name at construction time."
            )
        collection = self._client[self._database][spec.collection]
        filter_dict = predicate_to_mongo(spec.filter, params_instance) if spec.filter else {}
        projection = dict.fromkeys(spec.projection, 1) if spec.projection else None
        explicit_json_fields: frozenset[str] = frozenset(spec.json_fields)
        schema_str_fields: frozenset[str] = (
            _schema_str_field_names(spec.schema_type)
            if spec.schema_type is not None
            else frozenset()
        )

        # ---- Determine the registered schema ----
        if spec.schema_type is not None:
            registered_schema: dict[str, pl.DataType] = dict(_schema_field_dtypes(spec.schema_type))
            if spec.extra_fields_mode == "capture":
                registered_schema["_extra"] = pl.String()
        else:
            # Infer schema from the first sample batch (one lightweight query).
            sample_cursor = collection.find(filter_dict, projection, batch_size=spec.batch_size)
            if spec.limit is not None:
                sample_cursor = sample_cursor.limit(spec.limit)
            sample_docs: list[dict[str, Any]] = []
            for doc in sample_cursor:
                sample_docs.append(normalize_bson_doc(doc))
                if len(sample_docs) >= _SCHEMA_INFERENCE_SAMPLE:
                    break
            if not sample_docs:
                registered_schema = {}
            else:
                sample_frame = _build_batch_frame(sample_docs, explicit_json_fields, frozenset())
                registered_schema = dict(
                    zip(sample_frame.columns, sample_frame.dtypes, strict=True)
                )

        # ---- Lazy scan generator (executes on .collect()) ----
        def _io_source(
            with_columns: list[str] | None,
            predicate: pl.Expr | None,
            n_rows: int | None,
            batch_size_hint: int | None,
        ) -> Any:
            cursor = collection.find(filter_dict, projection, batch_size=spec.batch_size)
            effective_limit = spec.limit
            if n_rows is not None:
                effective_limit = (
                    min(effective_limit, n_rows) if effective_limit is not None else n_rows
                )
            if effective_limit is not None:
                cursor = cursor.limit(effective_limit)

            remaining = n_rows
            batch: list[dict[str, Any]] = []

            for doc in cursor:
                batch.append(normalize_bson_doc(doc))
                if len(batch) >= spec.batch_size:
                    frame = _build_batch_frame(batch, explicit_json_fields, schema_str_fields)
                    frame = _finalize_batch(frame, spec, registered_schema, with_columns, remaining)
                    if remaining is not None:
                        remaining -= len(frame)
                    yield frame
                    batch = []
                    if remaining is not None and remaining <= 0:
                        return

            if batch:
                frame = _build_batch_frame(batch, explicit_json_fields, schema_str_fields)
                frame = _finalize_batch(frame, spec, registered_schema, with_columns, remaining)
                yield frame

        return register_io_source(
            _io_source,
            schema=registered_schema,
            # validate_schema=False: _cast_to_registered_schema/_finalize_batch already align
            # each batch to the registered schema (adds null cols, serialises Struct→String).
            # Enabling strict validation would reject batches with legitimate type differences
            # that our coercion layer handles correctly.
            validate_schema=False,
            is_pure=False,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _json_default(obj: object) -> str:
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.hex()
    # Unexpected type slipped past normalize_bson_doc — log and fall back to type name only
    # to avoid leaking sensitive object representations into pipeline output.
    _log.warning(
        "MongoSourceReader: unexpected type %s in document field — using type name as placeholder",
        type(obj).__name__,
    )
    return f"<{type(obj).__name__}>"


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, default=_json_default, ensure_ascii=False)


def _safe_dumps(v: Any) -> str | None:
    if v is None:
        return None
    try:
        return _json_dumps(v)
    except (RecursionError, ValueError):
        _log.warning(
            "MongoSourceReader: could not serialise value of type %s — replacing with null",
            type(v).__name__,
        )
        return None


_SCHEMA_PRIMITIVE_DTYPES: dict[Any, Any] = {
    str: pl.String,
    int: pl.Int64,
    float: pl.Float64,
    bool: pl.Boolean,
    bytes: pl.Binary,
    datetime.datetime: pl.Datetime("us"),
    datetime.date: pl.Date,
}


def _strip_optional_annotation(annotation: Any) -> tuple[bool, Any]:
    origin = typing.get_origin(annotation)
    if origin not in {typing.Union, types.UnionType}:
        return False, annotation
    args = typing.get_args(annotation)
    non_none = [arg for arg in args if arg is not type(None)]
    if len(non_none) == 1 and len(non_none) < len(args):
        return True, non_none[0]
    return False, annotation


def _annotation_to_polars_dtype(annotation: Any) -> pl.DataType:
    is_optional, inner = _strip_optional_annotation(annotation)
    if is_optional:
        return _annotation_to_polars_dtype(inner)
    if annotation in _SCHEMA_PRIMITIVE_DTYPES:
        return _SCHEMA_PRIMITIVE_DTYPES[annotation]  # type: ignore[no-any-return]
    if isinstance(annotation, pl.DataType):
        return annotation
    origin = typing.get_origin(annotation)
    if origin is list:
        args = typing.get_args(annotation)
        inner_annotation = args[0] if args else str
        return pl.List(_annotation_to_polars_dtype(inner_annotation))
    if isinstance(annotation, type) and hasattr(annotation, "__annotations__"):
        hints = typing.get_type_hints(annotation)
        return pl.Struct(
            [pl.Field(name, _annotation_to_polars_dtype(ann)) for name, ann in hints.items()]
        )
    raise TypeError(
        f"MongoSourceReader: cannot convert annotation {annotation!r} to a Polars dtype"
    )


def _schema_field_dtypes(schema_type: type) -> dict[str, pl.DataType]:
    try:
        hints = typing.get_type_hints(schema_type)
    except (NameError, AttributeError) as exc:
        raise TypeError(
            f"MongoSourceReader: could not resolve type hints for {schema_type!r}. "
            "Ensure all forward references are resolvable at call time."
        ) from exc
    return {name: _annotation_to_polars_dtype(ann) for name, ann in hints.items()}


def _schema_str_field_names(schema_type: type) -> frozenset[str]:
    """Return names of fields declared as str (or Optional[str]) in schema_type."""
    try:
        hints = typing.get_type_hints(schema_type)
    except (NameError, AttributeError) as exc:
        raise TypeError(
            f"MongoSourceReader: could not resolve type hints for {schema_type!r}. "
            "Ensure all forward references are resolvable at call time."
        ) from exc
    result: set[str] = set()
    for name, ann in hints.items():
        _, inner = _strip_optional_annotation(ann)
        if inner is str:
            result.add(name)
    return frozenset(result)


def _pre_serialize_value(v: Any) -> Any:
    """Serialize dict/list/non-string values to JSON; pass None and str through unchanged."""
    if v is None:
        return None
    if isinstance(v, str):
        return v
    return _json_dumps(v)


def _pre_serialize_fields(
    batch: list[dict[str, Any]], json_fields: frozenset[str]
) -> list[dict[str, Any]]:
    """Return a new batch with all json_fields pre-serialized to JSON strings."""
    result = []
    for doc in batch:
        new_doc = dict(doc)
        for k in json_fields:
            if k in new_doc:
                new_doc[k] = _pre_serialize_value(new_doc[k])
        result.append(new_doc)
    return result


_LIST_CLASSIFY_SAMPLE = 20


def _classify_value(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        return "str"
    if isinstance(v, bytes):
        return "bytes"
    if isinstance(v, (datetime.datetime, datetime.date)):
        return "datetime"
    if isinstance(v, dict):
        return "dict"
    if isinstance(v, list):
        return _classify_list(v)
    return f"other:{type(v).__name__}"


def _classify_list(v: list[Any]) -> str:
    seen: set[str] = set()
    count = 0
    for item in v:
        if item is None:
            continue
        seen.add(_classify_value(item))
        count += 1
        if len(seen) > 1 or count >= _LIST_CLASSIFY_SAMPLE:
            break
    if not seen:
        return "list"
    return f"list[{next(iter(seen))}]" if len(seen) == 1 else "list[mixed]"


def _detect_conflicted_keys(batch: list[dict[str, Any]]) -> set[str]:
    key_types: dict[str, set[str]] = {}
    for doc in batch:
        for k, v in doc.items():
            t = _classify_value(v)
            if t == "null":
                continue
            key_types.setdefault(k, set()).add(t)
    return {k for k, types in key_types.items() if len(types) > 1}


def _serialize_conflicted(
    batch: list[dict[str, Any]], conflicted_keys: set[str]
) -> list[dict[str, Any]]:
    if not conflicted_keys:
        return batch
    result = []
    for doc in batch:
        new_doc = dict(doc)
        for k in conflicted_keys:
            if k in new_doc and new_doc[k] is not None:
                new_doc[k] = _json_dumps(new_doc[k])
        result.append(new_doc)
    return result


def _list_has_dicts(values: list[Any]) -> bool:
    return any(isinstance(item, dict) for item in values)


def _collect_nested_values(values: list[Any]) -> list[Any]:
    """Flatten one level of list nesting to collect dict/list items."""
    result: list[Any] = []
    for v in values:
        if isinstance(v, list):
            result.extend(v)
        else:
            result.append(v)
    return result


def _nested_has_conflict(values: list[Any], _depth: int = 0) -> bool:
    """Return True if the given values (all from the same root key across docs)
    contain a heterogeneous type conflict at any nesting depth.

    Nulls are excluded from conflict detection (consistent with _detect_conflicted_keys).
    """
    if _depth >= _MAX_NESTED_DEPTH:
        return True  # conservatively serialise when structure is too deep
    non_null = [v for v in values if v is not None]
    if not non_null:
        return False

    has_dict = any(isinstance(v, dict) for v in non_null)
    has_list = any(isinstance(v, list) for v in non_null)
    has_scalar = any(not isinstance(v, (dict, list)) for v in non_null)

    if has_dict and has_scalar:
        return True
    if has_dict and has_list:
        return True

    if has_scalar and not has_dict and not has_list:
        scalar_types: set[str] = set()
        for v in non_null:
            if not isinstance(v, (dict, list)):
                scalar_types.add(_classify_value(v))
        if len(scalar_types) > 1:
            return True

    if has_dict:
        sub_keys: dict[str, list[Any]] = {}
        for v in non_null:
            if isinstance(v, dict):
                for sk, sv in v.items():
                    sub_keys.setdefault(sk, []).append(sv)
        for sub_vals in sub_keys.values():
            if _nested_has_conflict(sub_vals, _depth + 1):
                return True

    if has_list:
        flat = _collect_nested_values([v for v in non_null if isinstance(v, list)])
        if _nested_has_conflict(flat, _depth + 1):
            return True

    return False


def _complex_root_keys(batch: list[dict[str, Any]]) -> set[str]:
    """Return root keys that hold dicts or list-of-dicts in any document
    AND whose nested structure contains a type conflict."""
    key_values: dict[str, list[Any]] = {}
    for doc in batch:
        for k, v in doc.items():
            if isinstance(v, dict) or (isinstance(v, list) and _list_has_dicts(v)):
                key_values.setdefault(k, []).append(v)

    return {k for k, vals in key_values.items() if _nested_has_conflict(vals)}


def _build_batch_frame_column_fallback(batch: list[dict[str, Any]]) -> pl.DataFrame:
    if not batch:
        return pl.DataFrame()
    all_keys = list(dict.fromkeys(k for doc in batch for k in doc))
    cols: dict[str, list[Any]] = {k: [] for k in all_keys}
    for doc in batch:
        for k in all_keys:
            cols[k].append(doc.get(k))
    series_list = []
    for k in all_keys:
        try:
            series_list.append(pl.Series(name=k, values=cols[k]))
        except (pl.exceptions.ComputeError, pl.exceptions.SchemaError):
            serialized = [_safe_dumps(v) for v in cols[k]]
            series_list.append(pl.Series(name=k, values=serialized, dtype=pl.String))
    return pl.DataFrame(series_list)


def _build_batch_frame(
    batch: list[dict[str, Any]],
    json_fields: frozenset[str] = frozenset(),
    schema_str_fields: frozenset[str] = frozenset(),
) -> pl.DataFrame:
    if not batch:
        return pl.DataFrame()

    all_pre_serialize = json_fields | schema_str_fields

    # Pre-serialize declared fields before any type inference.
    # Emit a warning for schema-declared str fields that contain complex values.
    if all_pre_serialize:
        if schema_str_fields:
            complex_schema_fields = {
                k
                for k in schema_str_fields
                if any(isinstance(doc.get(k), (dict, list)) for doc in batch)
            }
            if complex_schema_fields:
                _log.warning(
                    "MongoSourceReader: field(s) %s declared as str in schema but data contains "
                    "complex types — pre-serialising to JSON string",
                    sorted(complex_schema_fields),
                )
        batch = _pre_serialize_fields(batch, all_pre_serialize)

    # Pass 1 — flat conflict detection
    conflicted = _detect_conflicted_keys(batch)
    if conflicted:
        _log.warning(
            "MongoSourceReader: heterogeneous types detected in fields %s"
            " — serialising to JSON string",
            sorted(conflicted),
        )
    clean_batch = _serialize_conflicted(batch, conflicted)

    # Pass 2 — nested conflict: proactively serialise root fields whose nested
    # structure has type heterogeneity invisible at the root level.
    nested_complex = _complex_root_keys(clean_batch)
    if nested_complex:
        _log.warning(
            "MongoSourceReader: nested type conflict — serialising parent field(s) %s"
            " to JSON string",
            sorted(nested_complex),
        )
    deep_clean = _serialize_conflicted(clean_batch, nested_complex)

    try:
        return pl.from_dicts(deep_clean, infer_schema_length=len(deep_clean))
    except (pl.exceptions.ComputeError, pl.exceptions.SchemaError):
        return _build_batch_frame_column_fallback(deep_clean)


def _series_to_json_string(s: pl.Series) -> pl.Series:
    if s.dtype == pl.String:
        return s
    if isinstance(s.dtype, (pl.Struct, pl.List, pl.Array)):
        return s.map_elements(_safe_dumps, return_dtype=pl.String)
    return s.cast(pl.String)


def _cast_to_registered_schema(
    df: pl.DataFrame, registered: dict[str, pl.DataType]
) -> pl.DataFrame:
    """Align a batch DataFrame to the schema registered with the scan plugin.

    Used on the schemaless path (no schema_type) to ensure all yielded batches
    share the same columns and dtypes as inferred from the sample.
    """
    if not registered:
        return df

    exprs: list[pl.Expr | pl.Series] = []
    for col_name, target_dtype in registered.items():
        if col_name not in df.columns:
            exprs.append(pl.lit(None, dtype=target_dtype).alias(col_name))
            continue
        col_dtype = df[col_name].dtype
        if col_dtype == target_dtype:
            continue
        if target_dtype == pl.String and isinstance(col_dtype, (pl.Struct, pl.List, pl.Array)):
            exprs.append(_series_to_json_string(df[col_name]).alias(col_name))
        elif col_dtype == pl.String and target_dtype != pl.String:
            # Preserve conflict-serialized strings; do not attempt to re-cast.
            pass
        else:
            exprs.append(pl.col(col_name).cast(target_dtype, strict=False).alias(col_name))

    if exprs:
        df = df.with_columns(exprs)

    # Columns in this batch absent from the registered schema (new field not seen in sample)
    new_cols = [c for c in df.columns if c not in registered]
    if new_cols:
        _log.warning(
            "MongoSourceReader: batch contains columns %s absent from schema inference sample "
            "— dropping",
            sorted(new_cols),
        )
        df = df.drop(new_cols)

    return df.select([c for c in registered if c in df.columns])


def _finalize_batch(
    frame: pl.DataFrame,
    spec: Any,
    registered_schema: dict[str, pl.DataType],
    with_columns: list[str] | None,
    n_rows: int | None,
) -> pl.DataFrame:
    """Apply schema coercion, column projection, and row limit to a generator batch."""
    if spec.schema_type is not None:
        frame = _apply_schema(frame, spec.schema_type, spec.extra_fields_mode)
    else:
        frame = _cast_to_registered_schema(frame, registered_schema)
    if with_columns:
        present = [c for c in with_columns if c in frame.columns]
        frame = frame.select(present) if present else frame.head(0)
    if n_rows is not None:
        frame = frame.head(n_rows)
    return frame


def _apply_schema(df: pl.DataFrame, schema_type: type, mode: str) -> pl.DataFrame:
    """Drop or capture document fields not declared in *schema_type*."""
    declared_fields = _schema_field_dtypes(schema_type)
    extra_cols = [c for c in df.columns if c not in declared_fields]

    expressions: list[pl.Expr | pl.Series] = []
    for col_name, expected_dtype in declared_fields.items():
        if col_name not in df.columns:
            expressions.append(pl.lit(None, dtype=expected_dtype).alias(col_name))
            continue

        col_dtype = df[col_name].dtype
        if expected_dtype == pl.String and isinstance(col_dtype, (pl.Struct, pl.List, pl.Array)):
            # Fallback: pre-serialization should have handled this, but if not:
            expressions.append(_series_to_json_string(df[col_name]).alias(col_name))
            continue

        if col_dtype == pl.String and expected_dtype != pl.String:
            # Preserve string payloads emitted by conflict handling.
            continue

        if col_dtype != expected_dtype:
            expressions.append(pl.col(col_name).cast(expected_dtype).alias(col_name))

    if expressions:
        df = df.with_columns(expressions)

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
        extra_struct = pl.struct([df[c] for c in extra_cols])
        extra_series = extra_struct.map_elements(_safe_dumps, return_dtype=pl.String).alias(
            "_extra"
        )
        return df.drop(extra_cols).with_columns(extra_series)
    # "ignore"
    return df.drop(extra_cols)


__all__ = ["FromMongo", "MongoSourceReader", "MongoSourceSpec", "SourceRef"]
