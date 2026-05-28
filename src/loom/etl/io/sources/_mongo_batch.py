"""MongoDB batch processor — list[dict] → pl.DataFrame."""

from __future__ import annotations

import datetime
import json
import logging
from dataclasses import dataclass
from typing import Any

import polars as pl

from loom.etl.io.sources._mongo_bson import deep_normalize_for_json

_log = logging.getLogger(__name__)

_LIST_CLASSIFY_SAMPLE = 20
_MAX_NESTED_DEPTH = 64


# ---------------------------------------------------------------------------
# JSON serialization
# ---------------------------------------------------------------------------


def _json_default(obj: object) -> str:
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.hex()
    _log.warning(
        "MongoSourceReader: unexpected type %s in document field — using type name",
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
    key_types: dict[str, set[str]] = {}
    for doc in batch:
        for k, v in doc.items():
            t = _classify_value(v)
            if t == "null":
                continue
            key_types.setdefault(k, set()).add(t)
    return {k for k, ts in key_types.items() if len(ts) > 1}


def _nested_has_conflict(values: list[Any], _depth: int = 0) -> bool:
    if _depth >= _MAX_NESTED_DEPTH:
        return True
    non_null = [v for v in values if v is not None]
    if not non_null:
        return False

    has_dict = any(isinstance(v, dict) for v in non_null)
    has_list = any(isinstance(v, list) for v in non_null)
    has_scalar = any(not isinstance(v, (dict, list)) for v in non_null)

    if (has_dict and has_scalar) or (has_dict and has_list):
        return True
    scalars_conflict = has_scalar and not has_dict and not has_list
    if scalars_conflict and len({_classify_value(v) for v in non_null}) > 1:
        return True
    if has_dict:
        sub_keys: dict[str, list[Any]] = {}
        for v in non_null:
            if isinstance(v, dict):
                for sk, sv in v.items():
                    sub_keys.setdefault(sk, []).append(sv)
        if any(_nested_has_conflict(sv, _depth + 1) for sv in sub_keys.values()):
            return True
    if has_list:
        flat: list[Any] = [item for v in non_null if isinstance(v, list) for item in v]
        if _nested_has_conflict(flat, _depth + 1):
            return True
    return False


def _complex_root_keys(batch: list[dict[str, Any]]) -> set[str]:
    key_values: dict[str, list[Any]] = {}
    for doc in batch:
        for k, v in doc.items():
            if isinstance(v, dict) or (isinstance(v, list) and any(isinstance(i, dict) for i in v)):
                key_values.setdefault(k, []).append(v)
    return {k for k, vals in key_values.items() if _nested_has_conflict(vals)}


# ---------------------------------------------------------------------------
# Batch transformation helpers
# ---------------------------------------------------------------------------


def _pre_serialize_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        return v
    return _json_dumps(deep_normalize_for_json(v))  # P4: recursive BSON normalization


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
    value: Any, path: str = "$", depth: int = 0, limit: int = 24
) -> list[str]:
    if depth >= 8 or limit <= 0:
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


def _coerce_scalar_value(value: Any, dtype: pl.DataType | None) -> Any:
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


def _canonicalize_value(value: Any, plan: _CanonicalValuePlan) -> Any:
    if plan.kind == "scalar":
        return _coerce_scalar_value(value, plan.dtype)
    if value is None:
        return None

    if plan.kind == "list":
        if not isinstance(value, list) or plan.inner is None:
            return value
        return [_canonicalize_value(item, plan.inner) for item in value]

    if plan.kind == "struct":
        if not isinstance(value, dict):
            return value
        canonical: dict[str, Any] = {}
        seen: set[str] = set()
        for field in plan.fields:
            if field.name in value:
                canonical[field.name] = _canonicalize_value(value[field.name], field.plan)
                seen.add(field.name)
            else:
                canonical[field.name] = None
        for key, item in value.items():
            if key in seen:
                continue
            canonical[key] = item
        return canonical

    return value


def _canonicalize_batch(
    batch: list[dict[str, Any]],
    schema_overrides: dict[str, pl.DataType] | None,
    *,
    plan: dict[str, _CanonicalValuePlan] | None = None,
) -> list[dict[str, Any]]:
    if not batch or not schema_overrides:
        return batch
    compiled_plan = plan if plan is not None else _build_schema_plan(schema_overrides)
    if not compiled_plan:
        return batch
    result: list[dict[str, Any]] = []
    for doc in batch:
        new_doc = dict(doc)
        for field_name, field_plan in compiled_plan.items():
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
        except (pl.exceptions.ComputeError, pl.exceptions.SchemaError):
            s = pl.Series(name=k, values=[_safe_dumps(v) for v in cols[k]], dtype=pl.String)
        override = (schema_overrides or {}).get(k)
        if override is not None and s.dtype != override:
            try:
                if override == pl.String and isinstance(s.dtype, (pl.Struct, pl.List, pl.Array)):
                    # polars .cast(String) on Struct emits "{10}" not JSON — use safe_dumps
                    s = pl.Series(name=k, values=[_safe_dumps(v) for v in cols[k]], dtype=pl.String)
                else:
                    s = s.cast(override, strict=False)
            except Exception:
                pass  # leave inferred type; _finalize_batch aligns types later
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
            "MongoSourceReader: batch columns %s absent from schema — dropping",
            sorted(new_cols),
        )
        df = df.drop(new_cols)
    return df.select([c for c in schema if c in df.columns])


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
    if not extra_cols:
        return df

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
        return df.drop(extra_cols)
    if mode == "capture":
        extra_struct = pl.struct([df[c] for c in extra_cols])
        extra_series = extra_struct.map_elements(_safe_dumps, return_dtype=pl.String).alias(
            "_extra"
        )
        return df.drop(extra_cols).with_columns(extra_series)
    return df.drop(extra_cols)


# ---------------------------------------------------------------------------
# Main processor
# ---------------------------------------------------------------------------


class MongoBatchProcessor:
    """Transform a list[dict] batch into a pl.DataFrame.

    Handles pre-serialization of declared/explicit JSON fields, type-conflict
    detection and serialization, and DataFrame construction with schema hints.
    Schema alignment and extra-field handling are delegated to the caller.
    """

    def __init__(
        self,
        *,
        schema_str_fields: frozenset[str],
    ) -> None:
        self._schema_str_fields = schema_str_fields
        self._str_coercion_warned = False
        # Lazily built on first batch; safe as a singleton because schema_overrides
        # is the same dict object across all build_frame() calls within a single scan.
        self._canonical_plan: dict[str, _CanonicalValuePlan] | None = None

    def build_frame(
        self,
        batch: list[dict[str, Any]],
        schema_overrides: dict[str, pl.DataType] | None = None,
    ) -> pl.DataFrame:
        if not batch:
            return pl.DataFrame()
        batch = self._pre_serialize(batch)
        batch = self._resolve_conflicts(batch)
        batch = self._serialize_extra_complex_fields(batch, schema_overrides)
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

    def _resolve_conflicts(self, batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
        conflicted = _detect_conflicted_keys(batch)
        nested = _complex_root_keys(batch) - conflicted
        if conflicted:
            _log.warning(
                "MongoSourceReader: heterogeneous types detected in fields %s"
                " — serialising to JSON string",
                sorted(conflicted),
            )
        if nested:
            _log.warning(
                "MongoSourceReader: nested type conflict — serialising parent field(s) %s"
                " to JSON string",
                sorted(nested),
            )
        return _serialize_conflicted(batch, conflicted | nested)

    def _serialize_extra_complex_fields(
        self,
        batch: list[dict[str, Any]],
        schema_overrides: dict[str, pl.DataType] | None,
    ) -> list[dict[str, Any]]:
        return _serialize_extra_complex_fields(batch, schema_overrides)

    def _canonicalize_declared_structs(
        self,
        batch: list[dict[str, Any]],
        schema_overrides: dict[str, pl.DataType] | None,
    ) -> list[dict[str, Any]]:
        if not schema_overrides:
            return batch
        if self._canonical_plan is None:
            self._canonical_plan = _build_schema_plan(schema_overrides)
        return _canonicalize_batch(batch, schema_overrides, plan=self._canonical_plan)

    def _to_dataframe(
        self, batch: list[dict[str, Any]], schema_overrides: dict[str, pl.DataType] | None
    ) -> pl.DataFrame:
        try:
            return pl.from_dicts(
                batch,
                schema_overrides=schema_overrides or None,
                strict=False,
            )
        except (pl.exceptions.ComputeError, pl.exceptions.SchemaError):
            if batch:
                _log.warning(
                    "MongoSourceReader: pl.from_dicts() failed — "
                    "falling back to series construction. Batch shape sample:\n%s",
                    "\n".join(_summarize_nested_shapes(batch[0])),
                )
            return _build_frame_fallback(batch, schema_overrides)


__all__ = [
    "MongoBatchProcessor",
    "align_to_schema",
    "apply_declared_schema",
]
