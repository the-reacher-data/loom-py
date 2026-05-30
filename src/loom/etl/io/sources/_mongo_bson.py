"""ETL-context BSON normalizer.

Converts raw pymongo document values to Python builtins that
``pl.from_dicts()`` can ingest without producing ``Object`` dtype columns.

Differs intentionally from ``loom.streaming.mongo._normalize``:

- ``datetime``   → kept as ``datetime`` (Polars infers ``Datetime(us)`` natively)
- ``ObjectId``   → ``str`` (hex string)
- ``Decimal128`` → ``float`` (via ``to_decimal()``; precision ≤ 15 sig. digits)
- ``Binary``     → ``bytes`` (Polars ``Binary`` column — no base64 overhead)
- ``Timestamp``  → ``int`` (``time`` field in seconds; BSON Timestamp is oplog-internal)
- ``DBRef``      → ``str`` (``"collection/id"`` repr)
- ``Mapping``    → ``dict[str, …]`` (keys coerced to ``str``, values recursed)
- ``Sequence``   → ``list`` (items recursed)
- bson-origin unknown dict-like → ``dict[str, …]`` (duck-typed via ``.items()``; warns)
- bson-origin unknown scalar    → ``None`` (null; avoids non-JSON ``Object(…)`` strings)
- Non-bson unknown types        → ``str(value)`` (original fallback preserved)

Note on ``Decimal128`` precision: Decimal128 is IEEE 754-2008 128-bit decimal
(up to 34 significant digits).  Conversion to float64 retains ~15-17 digits.
For high-precision monetary columns declare an explicit schema and apply a
column-level cast after reading.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

_log = logging.getLogger(__name__)

_MAX_DEPTH = 64

# Tracks bson type names already warned about — prevents WARNING flood when an unknown
# type appears in every document of a large batch (log each type name once per process).
_warned_bson_types: set[str] = set()


def _is_bson_type(value: object) -> bool:
    """Return True if *value* originates from the bson/pymongo package tree."""
    module = type(value).__module__
    return module == "bson" or module.startswith("bson.")


def normalize_bson_doc(doc: dict[str, Any], *, _depth: int = 0) -> dict[str, Any]:
    """Return a copy of *doc* with all BSON-specific values replaced by builtins.

    Args:
        doc: Raw pymongo document dict.

    Returns:
        New dict safe to pass to ``pl.from_dicts()``.

    Raises:
        ValueError: When document nesting exceeds :data:`_MAX_DEPTH`.
    """
    if _depth > _MAX_DEPTH:
        raise ValueError(
            f"normalize_bson_doc: document exceeds maximum nesting depth of {_MAX_DEPTH}."
        )
    return {str(k): _normalize(v, _depth + 1) for k, v in doc.items()}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalize(value: object, depth: int) -> object:
    if depth > _MAX_DEPTH:
        raise ValueError(f"normalize_bson_doc: nested value exceeds maximum depth of {_MAX_DEPTH}.")
    # Native Python types Polars handles directly
    if value is None or isinstance(value, (bool, int, float, str, bytes)):
        return value
    if isinstance(value, datetime):
        # Normalize all ETL datetimes to UTC so downstream filters and Delta
        # pushdown compare consistent tz-aware values.
        return value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    # Recursive containers
    if isinstance(value, Mapping):
        return {str(k): _normalize(v, depth + 1) for k, v in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (bytearray, memoryview)):
        return [_normalize(item, depth + 1) for item in value]
    # BSON types dispatched by class name — avoids a hard bson import at module level
    normalizer = _NORMALIZERS.get(type(value).__name__)
    if normalizer is not None:
        return normalizer(value)

    # bson-origin types: handle separately to avoid str() producing Object({...}) strings
    # that are not valid JSON. Guard limits scope to the bson package only.
    if _is_bson_type(value):
        type_name = type(value).__name__
        if callable(getattr(value, "items", None)):
            if type_name not in _warned_bson_types:
                _warned_bson_types.add(type_name)
                _log.warning(
                    "normalize_bson_doc: unknown bson dict-like %s"
                    " — normalizing via .items(); add handler to _NORMALIZERS if frequent",
                    type_name,
                )
            try:
                return {str(k): _normalize(v, depth + 1) for k, v in value.items()}  # type: ignore[attr-defined]
            except (TypeError, AttributeError, ValueError) as exc:
                _log.warning(
                    "normalize_bson_doc: .items() failed for %s: %s — replacing with null",
                    type_name,
                    exc,
                )
        elif type_name not in _warned_bson_types:
            _warned_bson_types.add(type_name)
            _log.warning(
                "normalize_bson_doc: unknown bson type %s — cannot normalize, replacing with null",
                type_name,
            )
        return None

    # Non-bson unknown type: str() fallback preserved (avoids Polars Object dtype columns).
    _log.warning(
        "normalize_bson_doc: unknown non-bson type %s — using str() fallback",
        type(value).__name__,
    )
    return str(value)


def _normalize_objectid(value: object) -> str:
    return str(value)


def _normalize_decimal128(value: object) -> float:
    # Decimal128.to_decimal() returns a Python decimal.Decimal (full precision).
    # We convert to float for Polars Float64 compatibility.
    to_decimal = getattr(value, "to_decimal", None)
    if callable(to_decimal):
        return float(str(to_decimal()))
    return float(str(value))


def _normalize_binary(value: object) -> bytes:
    # pymongo Binary is a bytes subclass — materialise to plain bytes for Polars Binary.
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    raise TypeError(f"Expected bytes-like BSON Binary, got {type(value)!r}")


def _normalize_timestamp(value: object) -> int:
    # BSON Timestamp is used internally by MongoDB for the oplog.
    # Expose only the seconds component as an int.
    t = getattr(value, "time", None)
    return int(t) if isinstance(t, (int, float)) else 0


def _normalize_dbref(value: object) -> str:
    coll = getattr(value, "collection", "?")
    oid = getattr(value, "id", "?")
    return f"{coll}/{oid}"


_NORMALIZERS: dict[str, Callable[[object], object]] = {
    "ObjectId": _normalize_objectid,
    "Decimal128": _normalize_decimal128,
    "Binary": _normalize_binary,
    "Timestamp": _normalize_timestamp,
    "DBRef": _normalize_dbref,
}


def deep_normalize_for_json(value: Any, *, _depth: int = 0) -> Any:
    """Normalize BSON types recursively in an arbitrary value for JSON serialization.

    For full documents, prefer :func:`normalize_bson_doc`. This function is intended
    for single-value normalization before JSON serialization (e.g., str-declared fields).
    """
    return _normalize(value, _depth)


__all__ = ["normalize_bson_doc", "deep_normalize_for_json"]
