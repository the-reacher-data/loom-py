"""MongoDB batch processor — list[dict] → pl.DataFrame."""

from __future__ import annotations

import datetime
import json
import logging
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

import polars as pl

from loom.etl.io.sources._mongo_bson import deep_normalize_for_json

_log = logging.getLogger(__name__)

_LIST_CLASSIFY_SAMPLE = 20
_MAX_NESTED_DEPTH = 64
_MAX_RISKY_ROWS_REPORTED = 5
_MAX_RISKY_NOTES_PER_ROW = 8
_SHAPE_SUMMARY_DEPTH = 8
_SHAPE_SUMMARY_LIMIT = 24


# ---------------------------------------------------------------------------
# JSON serialization
# ---------------------------------------------------------------------------


def _json_default(obj: object) -> str:
    # datetime/bytes pass through _normalize unchanged; handled at JSON-encode time.
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.hex()
    _log.warning(
        "MongoSourceReader: unexpected type %s in document field — using str()",
        type(obj).__name__,
    )
    return str(obj)


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


# ---------------------------------------------------------------------------
# Type classification
# ---------------------------------------------------------------------------


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
        return "null" if not v else _classify_list(v)  # P5: empty list = no type info
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


# ---------------------------------------------------------------------------
# Conflict detection
# ---------------------------------------------------------------------------


def _detect_conflicted_keys(batch: list[dict[str, Any]]) -> set[str]:
    conflicted, _ = _classify_batch_keys(batch)
    return conflicted


def _classify_batch_keys(
    batch: list[dict[str, Any]],
) -> tuple[set[str], set[str]]:
    """Return (conflicted_keys, complex_root_keys) in a single pass over the batch."""
    key_types: dict[str, set[str]] = {}
    key_values: dict[str, list[Any]] = {}
    for doc in batch:
        for k, v in doc.items():
            t = _classify_value(v)
            if t != "null":
                key_types.setdefault(k, set()).add(t)
            if isinstance(v, dict) or (isinstance(v, list) and any(isinstance(i, dict) for i in v)):
                key_values.setdefault(k, []).append(v)
    conflicted: set[str] = set()
    for k, ts in key_types.items():
        effective = ts - {"int", "float"} | ({"numeric"} if ts & {"int", "float"} else set())
        if len(effective) > 1:
            conflicted.add(k)
    complex_root = {k for k, vals in key_values.items() if _nested_has_conflict(vals)}
    return conflicted, complex_root


def _has_type_conflict(non_null: list[Any]) -> bool:
    """True if values mix container and scalar types, or have incompatible scalar types."""
    has_dict = any(isinstance(v, dict) for v in non_null)
    has_list = any(isinstance(v, list) for v in non_null)
    has_scalar = any(not isinstance(v, (dict, list)) for v in non_null)
    if (has_dict and has_scalar) or (has_dict and has_list):
        return True
    return (
        has_scalar
        and not has_dict
        and not has_list
        and len({_classify_value(v) for v in non_null}) > 1
    )


def _dict_children_conflict(non_null: list[Any], depth: int) -> bool:
    """True if any sub-key has a type conflict across the dicts in the batch."""
    sub: dict[str, list[Any]] = {}
    for v in non_null:
        if isinstance(v, dict):
            for sk, sv in v.items():
                sub.setdefault(sk, []).append(sv)
    return any(_nested_has_conflict(sv, depth + 1) for sv in sub.values())


def _list_children_conflict(non_null: list[Any], depth: int) -> bool:
    """True if the flattened items of all lists have a type conflict."""
    flat: list[Any] = [item for v in non_null if isinstance(v, list) for item in v]
    return bool(flat) and _nested_has_conflict(flat, depth + 1)


def _nested_has_conflict(values: list[Any], _depth: int = 0) -> bool:
    if _depth >= _MAX_NESTED_DEPTH:
        return True
    non_null = [v for v in values if v is not None]
    if not non_null:
        return False
    if _has_type_conflict(non_null):
        return True
    if any(isinstance(v, dict) for v in non_null) and _dict_children_conflict(non_null, _depth):
        return True
    return any(isinstance(v, list) for v in non_null) and _list_children_conflict(non_null, _depth)


def _complex_root_keys(batch: list[dict[str, Any]]) -> set[str]:
    _, complex_root = _classify_batch_keys(batch)
    return complex_root


# ---------------------------------------------------------------------------
# Batch transformation helpers
# ---------------------------------------------------------------------------


def _pre_serialize_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        return v
    # Contract: normalize_bson_doc() already ran in _mongo.py's cursor loop, converting
    # all BSON types to Python builtins. deep_normalize_for_json() is a defensive guard
    # for callers that invoke build_frame() directly without going through the cursor.
    return _json_dumps(deep_normalize_for_json(v))


def _pre_serialize_fields(
    batch: list[dict[str, Any]], fields: frozenset[str]
) -> list[dict[str, Any]]:
    result = []
    for doc in batch:
        new_doc = dict(doc)
        for k in fields:
            if k in new_doc:
                new_doc[k] = _pre_serialize_value(new_doc[k])
        result.append(new_doc)
    return result


def _serialize_extra_complex_fields(
    batch: list[dict[str, Any]], schema_overrides: dict[str, pl.DataType] | None
) -> list[dict[str, Any]]:
    if not batch or not schema_overrides:
        return batch
    declared = set(schema_overrides)
    extra_fields: set[str] = set()
    for doc in batch:
        for key, value in doc.items():
            if key in declared:
                continue
            if isinstance(value, (dict, list)):
                extra_fields.add(key)
    if not extra_fields:
        return batch
    _log.warning(
        "MongoSourceReader: serialising undeclared complex field(s) %s to JSON string",
        sorted(extra_fields),
    )
    return _serialize_conflicted(batch, extra_fields)


def _serialize_conflicted(
    batch: list[dict[str, Any]], conflicted: set[str]
) -> list[dict[str, Any]]:
    if not conflicted:
        return batch
    result = []
    for doc in batch:
        new_doc = dict(doc)
        for k in conflicted:
            if k in new_doc and new_doc[k] is not None:
                new_doc[k] = _json_dumps(new_doc[k])
        result.append(new_doc)
    return result


def _series_to_json_string(s: pl.Series) -> pl.Series:
    if s.dtype == pl.String:
        return s
    if isinstance(s.dtype, (pl.Struct, pl.List, pl.Array)):
        return s.map_elements(_safe_dumps, return_dtype=pl.String)
    return s.cast(pl.String)


def _summarize_nested_shapes(
    value: Any, path: str = "$", depth: int = 0, limit: int = _SHAPE_SUMMARY_LIMIT
) -> list[str]:
    if depth >= _SHAPE_SUMMARY_DEPTH or limit <= 0:
        return []
    lines: list[str] = []
    if isinstance(value, dict):
        lines.append(f"{path}: dict keys={list(value.keys())}")
        remaining = limit - 1
        for key, item in value.items():
            if remaining <= 0:
                break
            nested = _summarize_nested_shapes(item, f"{path}.{key}", depth + 1, remaining)
            lines.extend(nested)
            remaining = limit - len(lines)
        return lines
    if isinstance(value, list):
        lines.append(f"{path}: list len={len(value)}")
        remaining = limit - 1
        for idx, item in enumerate(value[: min(len(value), remaining)]):
            nested = _summarize_nested_shapes(item, f"{path}[{idx}]", depth + 1, remaining)
            lines.extend(nested)
            remaining = limit - len(lines)
            if remaining <= 0:
                break
        return lines
    lines.append(f"{path}: {type(value).__name__}")
    return lines


def _row_id(doc: dict[str, Any]) -> Any:
    for key in ("_id", "id", "event_id", "root_id"):
        if key in doc:
            return doc[key]
    return None


def _value_risk_notes(value: Any, plan: _CanonicalValuePlan, path: str) -> list[str]:
    notes: list[str] = []
    if plan.kind == "scalar":
        if value is None:
            return notes
        if isinstance(value, (dict, list)):
            notes.append(f"{path}: expected scalar got {type(value).__name__}")
            return notes
        if isinstance(value, str):
            coerced = _coerce_string_to_dtype(value, plan.dtype)
            if isinstance(coerced, str) and plan.dtype not in (None, pl.String):
                notes.append(f"{path}: unconvertible str for {plan.dtype}")
        return notes

    if value is None:
        return notes

    if plan.kind == "list":
        if not isinstance(value, list):
            notes.append(f"{path}: expected list got {type(value).__name__}")
            return notes
        if plan.inner is None:
            return notes
        for idx, item in enumerate(value[:5]):
            notes.extend(_value_risk_notes(item, plan.inner, f"{path}[{idx}]"))
        return notes

    if plan.kind == "struct":
        if not isinstance(value, dict):
            notes.append(f"{path}: expected struct got {type(value).__name__}")
            return notes
        expected = {field.name for field in plan.fields}
        extra = [key for key in value if key not in expected]
        if extra:
            notes.append(f"{path}: extra key(s) {sorted(extra)!r}")
        for field in plan.fields:
            if field.name in value:
                notes.extend(
                    _value_risk_notes(value[field.name], field.plan, f"{path}.{field.name}")
                )
        return notes

    return notes


def _row_risk_notes(
    doc: dict[str, Any], compiled_plan: dict[str, _CanonicalValuePlan]
) -> list[str]:
    notes: list[str] = []
    declared = set(compiled_plan)
    for key, value in doc.items():
        if key not in declared:
            if isinstance(value, (dict, list)):
                notes.append(f"$.{key}: undeclared complex {type(value).__name__}")
            continue
        notes.extend(_value_risk_notes(value, compiled_plan[key], f"$.{key}"))
    return notes


def _log_risky_rows(
    batch: list[dict[str, Any]], schema_overrides: dict[str, pl.DataType] | None
) -> None:
    if not batch or not schema_overrides:
        return
    compiled_plan = _build_schema_plan(schema_overrides)
    reported = 0
    for row_index, doc in enumerate(batch):
        notes = _row_risk_notes(doc, compiled_plan)
        if not notes:
            continue
        reported += 1
        _log.warning(
            "MongoSourceReader: risky row sample row=%d id=%r issues=%s",
            row_index,
            _row_id(doc),
            notes[:_MAX_RISKY_NOTES_PER_ROW],
        )
        if reported >= _MAX_RISKY_ROWS_REPORTED:
            break


# ---------------------------------------------------------------------------
# Schema canonicalization
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _CanonicalFieldPlan:
    name: str
    plan: _CanonicalValuePlan


@dataclass(frozen=True, slots=True)
class _CanonicalValuePlan:
    kind: str
    dtype: pl.DataType | None = None
    fields: tuple[_CanonicalFieldPlan, ...] = ()
    inner: _CanonicalValuePlan | None = None


def _plan_from_dtype(dtype: pl.DataType) -> _CanonicalValuePlan:
    if isinstance(dtype, pl.Struct):
        return _CanonicalValuePlan(
            kind="struct",
            fields=tuple(
                _CanonicalFieldPlan(name=field.name, plan=_plan_from_dtype(field.dtype))  # type: ignore[arg-type]
                for field in dtype.fields
            ),
        )
    if isinstance(dtype, (pl.List, pl.Array)):
        return _CanonicalValuePlan(kind="list", inner=_plan_from_dtype(dtype.inner))  # type: ignore[arg-type]
    return _CanonicalValuePlan(kind="scalar", dtype=dtype)


def _coerce_string_to_dtype(value: Any, dtype: pl.DataType | None) -> Any:
    """Coerce a str value to the target Polars dtype. Non-str values are returned unchanged."""
    if value is None or dtype is None:
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if dtype == pl.String:
            return value
        if dtype in (
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
        ):
            try:
                return int(text)
            except ValueError:
                return value
        if dtype in (pl.Float32, pl.Float64):
            try:
                return float(text)
            except ValueError:
                return value
        if dtype == pl.Boolean:
            lowered = text.lower()
            if lowered in {"true", "t", "1", "yes", "y"}:
                return True
            if lowered in {"false", "f", "0", "no", "n"}:
                return False
            return value
        if isinstance(dtype, pl.Datetime):
            try:
                return datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                return value
        if isinstance(dtype, pl.Date):
            try:
                return datetime.date.fromisoformat(text)
            except ValueError:
                return value
        if isinstance(dtype, pl.Time):
            try:
                return datetime.time.fromisoformat(text)
            except ValueError:
                return value
        return value
    if dtype == pl.String:
        return value
    return value


def _build_schema_plan(schema_overrides: dict[str, pl.DataType]) -> dict[str, _CanonicalValuePlan]:
    return {name: _plan_from_dtype(dtype) for name, dtype in schema_overrides.items()}


def _declared_complex_fields(schema_overrides: dict[str, pl.DataType] | None) -> frozenset[str]:
    """Return the names of fields explicitly declared as Struct, List, or Array.

    These fields are excluded from conflict-serialisation so that the schema
    canonicalisation step can coerce their sub-values instead of flattening the
    whole field to a JSON string.
    """
    if not schema_overrides:
        return frozenset()
    return frozenset(
        name
        for name, dtype in schema_overrides.items()
        if isinstance(dtype, (pl.Struct, pl.List, pl.Array))
    )


def _coerce_list_items(value: list[Any], inner: _CanonicalValuePlan) -> list[Any]:
    return [_canonicalize_value(item, inner) for item in value]


def _coerce_struct_fields(
    value: dict[str, Any], fields: tuple[_CanonicalFieldPlan, ...]
) -> dict[str, Any]:
    declared = {f.name for f in fields}
    extra = sorted(k for k in value if k not in declared)
    if extra:
        _log.warning(
            "MongoSourceReader: dropping undeclared sub-field(s) %s from declared struct",
            extra,
        )
    return {f.name: _canonicalize_value(value.get(f.name), f.plan) for f in fields}


def _canonicalize_value(value: Any, plan: _CanonicalValuePlan) -> Any:
    if plan.kind == "scalar":
        return _coerce_string_to_dtype(value, plan.dtype)
    if value is None:
        return None
    if plan.kind == "list":
        if not isinstance(value, list):
            return None
        return value if plan.inner is None else _coerce_list_items(value, plan.inner)
    if plan.kind == "struct":
        return _coerce_struct_fields(value, plan.fields) if isinstance(value, dict) else None
    return value


def _canonicalize_batch(
    batch: list[dict[str, Any]],
    plan: dict[str, _CanonicalValuePlan],
) -> list[dict[str, Any]]:
    if not batch or not plan:
        return batch
    result: list[dict[str, Any]] = []
    for doc in batch:
        new_doc = dict(doc)
        for field_name, field_plan in plan.items():
            if field_name in new_doc:
                new_doc[field_name] = _canonicalize_value(new_doc[field_name], field_plan)
        result.append(new_doc)
    return result


# ---------------------------------------------------------------------------
# DataFrame construction
# ---------------------------------------------------------------------------


def _build_frame_fallback(
    batch: list[dict[str, Any]],
    schema_overrides: dict[str, pl.DataType] | None = None,
) -> pl.DataFrame:
    if not batch:
        return pl.DataFrame()
    all_keys = list(dict.fromkeys(k for doc in batch for k in doc))
    cols = {k: [doc.get(k) for doc in batch] for k in all_keys}
    series_list = []
    for k in all_keys:
        try:
            s = pl.Series(name=k, values=cols[k])
        except (pl.exceptions.ComputeError, pl.exceptions.SchemaError, TypeError):
            s = pl.Series(name=k, values=[_safe_dumps(v) for v in cols[k]], dtype=pl.String)
        override = (schema_overrides or {}).get(k)
        if override is not None and s.dtype != override:
            try:
                if override == pl.String and isinstance(s.dtype, (pl.Struct, pl.List, pl.Array)):
                    # polars .cast(String) on Struct emits "{10}" not JSON — use safe_dumps
                    s = pl.Series(name=k, values=[_safe_dumps(v) for v in cols[k]], dtype=pl.String)
                else:
                    s = s.cast(override, strict=False)
            except Exception as exc:
                _log.debug(
                    "MongoSourceReader: could not cast %r to %s — leaving inferred type (%s)",
                    k,
                    override,
                    type(exc).__name__,
                )
        series_list.append(s)
    return pl.DataFrame(series_list)


# ---------------------------------------------------------------------------
# Schema alignment
# ---------------------------------------------------------------------------


def align_to_schema(df: pl.DataFrame, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """Align a schemaless-path batch to the sample-inferred registered schema."""
    if not schema:
        return df
    exprs: list[pl.Expr | pl.Series] = []
    for col_name, target_dtype in schema.items():
        if col_name not in df.columns:
            exprs.append(pl.lit(None, dtype=target_dtype).alias(col_name))
            continue
        col_dtype = df[col_name].dtype
        if col_dtype == target_dtype:
            continue
        if target_dtype == pl.String and isinstance(col_dtype, (pl.Struct, pl.List, pl.Array)):
            exprs.append(_series_to_json_string(df[col_name]).alias(col_name))
        elif col_dtype == pl.String and target_dtype != pl.String:
            pass  # preserve conflict-serialized strings
        else:
            exprs.append(pl.col(col_name).cast(target_dtype, strict=False).alias(col_name))
    if exprs:
        df = df.with_columns(exprs)
    new_cols = [c for c in df.columns if c not in schema]
    if new_cols:
        _log.warning(
            "MongoSourceReader: schema drift — field(s) %s not in registered schema, dropping",
            sorted(new_cols),
        )
        df = df.drop(new_cols)
    return df.select(list(schema.keys()))


def apply_declared_schema(
    df: pl.DataFrame,
    declared: dict[str, pl.DataType],
    mode: str,
    schema_name: str,
) -> pl.DataFrame:
    """Add missing declared columns and dispatch extra columns per extra_fields_mode."""
    missing = [
        pl.lit(None, dtype=dtype).alias(name)
        for name, dtype in declared.items()
        if name not in df.columns
    ]
    if missing:
        df = df.with_columns(missing)

    extra_cols = [c for c in df.columns if c not in declared]
    if extra_cols:
        if mode == "error":
            raise ValueError(
                f"MongoSourceReader: documents contain fields not declared in "
                f"{schema_name}: {extra_cols}. "
                "Use on_extra_fields('ignore'), ('warn'), or ('capture') to suppress."
            )
        if mode == "warn":
            _log.warning(
                "MongoSourceReader: dropping %d undeclared field(s) from %s: %s",
                len(extra_cols),
                schema_name,
                extra_cols,
            )
        if mode == "capture":
            extra_struct = pl.struct([df[c] for c in extra_cols])
            extra_series = extra_struct.map_elements(_safe_dumps, return_dtype=pl.String).alias(
                "_extra"
            )
            df = df.drop(extra_cols).with_columns(extra_series)
        else:
            df = df.drop(extra_cols)

    if mode == "capture" and "_extra" not in df.columns:
        df = df.with_columns(pl.lit(None, pl.String()).alias("_extra"))

    output_cols = list(declared.keys()) + (["_extra"] if mode == "capture" else [])
    return df if df.columns == output_cols else df.select(output_cols)


# ---------------------------------------------------------------------------
# Main processor
# ---------------------------------------------------------------------------


class MongoBatchProcessor:
    """Transform a list[dict] batch into a pl.DataFrame.

    Handles pre-serialization of declared/explicit JSON fields, type-conflict
    detection and serialization, and DataFrame construction with schema hints.
    Schema alignment and extra-field handling are delegated to the caller.

    Not thread-safe: designed for single-threaded use inside a Polars scan plugin.

    Args:
        schema_str_fields: Field names explicitly declared as String in the user schema.
            These are pre-serialised to JSON before conflict detection.
        declared_schema: The schema explicitly declared by the user (not inferred).
            Used only for risky-row logging to avoid false positives on inferred types.
    """

    def __init__(
        self,
        *,
        schema_str_fields: frozenset[str],
        declared_schema: dict[str, pl.DataType] | None = None,
    ) -> None:
        self._schema_str_fields = schema_str_fields
        self._declared_schema = declared_schema
        self._str_coercion_warned = False
        # Lazily built on first batch; safe as a singleton because schema_overrides
        # is the same dict object across all build_frame() calls within a single scan.
        self._canonical_plan: dict[str, _CanonicalValuePlan] | None = None
        # None until the first batch is seen; updated as new fields appear.
        self._observed_fields: frozenset[str] | None = None

    def _check_schema_drift(self, batch: list[dict[str, Any]]) -> None:
        """Warn when fields appear or disappear relative to the first batch seen."""
        current_fields = frozenset(k for doc in batch for k in doc)
        if self._observed_fields is None:
            self._observed_fields = current_fields
            return
        new_fields = current_fields - self._observed_fields
        dropped_fields = self._observed_fields - current_fields
        if new_fields:
            _log.warning(
                "MongoSourceReader: schema drift — new field(s) %s appeared after first batch",
                sorted(new_fields),
            )
            self._observed_fields = self._observed_fields | new_fields
        if dropped_fields:
            _log.warning(
                "MongoSourceReader: schema drift — field(s) %s absent in this batch",
                sorted(dropped_fields),
            )
            # Do not update _observed_fields for dropped fields so the warning persists.

    def build_frame(
        self,
        batch: list[dict[str, Any]],
        schema_overrides: dict[str, pl.DataType] | None = None,
    ) -> pl.DataFrame:
        if not batch:
            return pl.DataFrame()
        self._check_schema_drift(batch)
        batch = self._pre_serialize(batch)
        batch = self._resolve_conflicts(batch, schema_overrides)
        batch = _serialize_extra_complex_fields(batch, schema_overrides)
        _log_risky_rows(batch, self._declared_schema)
        batch = self._canonicalize_declared_structs(batch, schema_overrides)
        return self._to_dataframe(batch, schema_overrides)

    def _pre_serialize(self, batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not self._schema_str_fields:
            return batch
        self._warn_str_coercion(batch)
        return _pre_serialize_fields(batch, self._schema_str_fields)

    def _warn_str_coercion(self, batch: list[dict[str, Any]]) -> None:
        if self._str_coercion_warned or not self._schema_str_fields:
            return
        complex_fields = {
            k
            for k in self._schema_str_fields
            if any(isinstance(doc.get(k), (dict, list)) for doc in batch)
        }
        if complex_fields:
            _log.warning(
                "MongoSourceReader: field(s) %s declared as str in schema but data contains "
                "complex types — pre-serialising to JSON string",
                sorted(complex_fields),
            )
            self._str_coercion_warned = True

    def _resolve_conflicts(
        self,
        batch: list[dict[str, Any]],
        schema_overrides: dict[str, pl.DataType] | None = None,
    ) -> list[dict[str, Any]]:
        protected = _declared_complex_fields(schema_overrides)
        all_conflicted, complex_root = _classify_batch_keys(batch)
        all_nested = complex_root - all_conflicted

        to_serialize = (all_conflicted | all_nested) - protected
        skipped = (all_conflicted | all_nested) & protected

        if to_serialize & all_conflicted:
            _log.warning(
                "MongoSourceReader: heterogeneous types detected in fields %s"
                " — serialising to JSON string",
                sorted(to_serialize & all_conflicted),
            )
        if to_serialize & all_nested:
            _log.warning(
                "MongoSourceReader: nested type conflict — serialising parent field(s) %s"
                " to JSON string",
                sorted(to_serialize & all_nested),
            )
        if skipped:
            _log.warning(
                "MongoSourceReader: type conflict in declared complex field(s) %s"
                " — deferring to schema canonicalization",
                sorted(skipped),
            )
        return _serialize_conflicted(batch, to_serialize)

    def _canonicalize_declared_structs(
        self,
        batch: list[dict[str, Any]],
        schema_overrides: dict[str, pl.DataType] | None,
    ) -> list[dict[str, Any]]:
        if not schema_overrides:
            return batch
        if self._canonical_plan is None:
            self._canonical_plan = _build_schema_plan(schema_overrides)
        return _canonicalize_batch(batch, self._canonical_plan)

    def _to_dataframe(
        self, batch: list[dict[str, Any]], schema_overrides: dict[str, pl.DataType] | None
    ) -> pl.DataFrame:
        try:
            return pl.from_dicts(
                batch,
                schema_overrides=schema_overrides or None,
                strict=False,
            )
        except (pl.exceptions.ComputeError, pl.exceptions.SchemaError, TypeError):
            if batch:
                _log.warning(
                    "MongoSourceReader: pl.from_dicts() failed — "
                    "falling back to series construction. Batch shape sample:\n%s",
                    "\n".join(_summarize_nested_shapes(batch[0])),
                )
            return _build_frame_fallback(batch, schema_overrides)


@runtime_checkable
class BatchProcessorProtocol(Protocol):
    """Structural interface for batch processors used by MongoSourceReader.

    Any object implementing ``build_frame`` is compatible, enabling injection of
    alternative processors (e.g., mocks, instrumented versions) without subclassing.

    Args:
        batch: Raw normalised documents (output of ``normalize_bson_doc``).
        schema_overrides: Polars dtype hints, either declared or inferred.

    Returns:
        A ``pl.DataFrame`` with one row per document.
    """

    def build_frame(
        self,
        batch: list[dict[str, Any]],
        schema_overrides: dict[str, pl.DataType] | None = None,
    ) -> pl.DataFrame: ...


__all__ = [
    "BatchProcessorProtocol",
    "MongoBatchProcessor",
    "align_to_schema",
    "apply_declared_schema",
]
