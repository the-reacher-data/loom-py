"""PolarsDeltaReader — SourceReader backed by Polars + delta-rs.

Handles both Delta table sources and file-based sources (CSV, JSON, XLSX,
Parquet).

Delta tables
------------
Uses ``DeltaTable.to_pyarrow_dataset()`` for lazy scanning to avoid
Polars ↔ deltalake Schema API version skew.

Predicate pushdown
~~~~~~~~~~~~~~~~~~
Predicates declared via ``.where()`` are converted to ``polars.Expr`` via
:func:`~loom.etl.backends.polars._predicate.predicate_to_polars` and applied
as ``LazyFrame.filter()`` calls.  Polars' lazy optimizer pushes these filters
into the PyArrow dataset scanner, which performs:

* **Partition pruning** — skips partition directories whose column values
  exclude the predicate (no I/O for those files).
* **Row-group pruning** — skips Parquet row groups via column statistics.
* **Row-level filtering** — applied in-memory on remaining rows.

A ``WHERE year = 2024`` predicate on a ``year``-partitioned table reads only
the matching partition — no full table scan.

File sources
------------
CSV, JSON (NDJSON), XLSX, and Parquet files are scanned lazily where possible.
Format-specific options are declared on :class:`~loom.etl.FromFile` via
``.with_options(CsvReadOptions(...))`` and forwarded to the corresponding
Polars scan function.

Schema application
------------------
When ``.with_schema(schema)`` is declared on a source, each column is cast
to its :class:`~loom.etl._schema.LoomDtype` via ``with_columns(cast(...))``.
Cast is applied **lazily** — no materialization occurs.  Extra columns in
the source not declared in the schema pass through unchanged.

Storage options are forwarded verbatim to delta-rs — the locator resolves
credentials per table.  See https://delta-io.github.io/delta-rs/api/delta_writer/
"""

from __future__ import annotations

import logging
import os
from typing import Any

import polars as pl
from deltalake import DeltaTable

from loom.etl._format import Format
from loom.etl._locator import TableLocator, _as_locator
from loom.etl._read_options import (
    CsvReadOptions,
    ExcelReadOptions,
    JsonReadOptions,
)
from loom.etl._schema import ColumnSchema
from loom.etl._source import SourceKind, SourceSpec
from loom.etl.backends.polars._dtype import loom_type_to_polars
from loom.etl.backends.polars._predicate import predicate_to_polars

_log = logging.getLogger(__name__)


class PolarsDeltaReader:
    """Read ETL sources as Polars lazy frames.

    Supports Delta table sources (``SourceKind.TABLE``) and file sources
    (``SourceKind.FILE`` — CSV, JSON, XLSX, Parquet).

    Implements :class:`~loom.etl._io.SourceReader`.

    Args:
        locator: Root URI string, :class:`pathlib.Path`, or any
                 :class:`~loom.etl._locator.TableLocator`.  A plain string or
                 path is wrapped in :class:`~loom.etl._locator.PrefixLocator`
                 automatically.

    Example::

        from loom.etl.backends.polars import PolarsDeltaReader

        reader = PolarsDeltaReader("s3://my-lake/")
    """

    def __init__(self, locator: str | os.PathLike[str] | TableLocator) -> None:
        self._locator = _as_locator(locator)

    def read(self, spec: SourceSpec, params_instance: Any) -> pl.LazyFrame:
        """Return a lazy frame for the source described by *spec*.

        Dispatches to Delta or file reading based on ``spec.kind``.
        Applies source schema casts and predicates lazily.

        Args:
            spec:             Compiled source spec.
            params_instance:  Concrete params for resolving predicate expressions.

        Returns:
            Lazy Polars frame.

        Raises:
            TypeError: If ``spec.kind`` is ``TEMP`` (handled by
                       :class:`~loom.etl._temp_store.IntermediateStore`).
            ValueError: If ``spec.kind`` is ``FILE`` but ``spec.path`` is
                        ``None``.
        """
        match spec.kind:
            case SourceKind.TABLE:
                return self._read_delta(spec, params_instance)
            case SourceKind.FILE:
                return self._read_file(spec)
            case _:
                raise TypeError(
                    f"PolarsDeltaReader cannot read source kind {spec.kind!r}. "
                    "TEMP sources are handled by IntermediateStore."
                )

    # ------------------------------------------------------------------
    # Delta
    # ------------------------------------------------------------------

    def _read_delta(self, spec: SourceSpec, params_instance: Any) -> pl.LazyFrame:
        ref = spec.table_ref
        if ref is None:
            raise TypeError("table_ref must be set for Delta read operations")
        _log.debug(
            "read delta table=%s predicates=%d columns=%d schema_cols=%d",
            ref.ref,
            len(spec.predicates),
            len(spec.columns),
            len(spec.schema),
        )
        loc = self._locator.locate(ref)
        dataset = DeltaTable(
            loc.uri, storage_options=loc.storage_options or None
        ).to_pyarrow_dataset()
        frame: pl.LazyFrame = pl.scan_pyarrow_dataset(dataset)
        for pred in spec.predicates:
            frame = frame.filter(predicate_to_polars(pred, params_instance))
        if spec.columns:
            frame = frame.select(list(spec.columns))
        return _apply_source_schema(frame, spec.schema)

    # ------------------------------------------------------------------
    # File
    # ------------------------------------------------------------------

    def _read_file(self, spec: SourceSpec) -> pl.LazyFrame:
        if spec.path is None:
            raise ValueError(f"FromFile spec has no path: {spec}")
        _log.debug(
            "read file path=%s format=%s columns=%d schema_cols=%d",
            spec.path,
            spec.format,
            len(spec.columns),
            len(spec.schema),
        )
        frame = _FILE_READERS[spec.format](spec.path, spec.read_options)
        if spec.columns:
            frame = frame.select(list(spec.columns))
        return _apply_source_schema(frame, spec.schema)


# ---------------------------------------------------------------------------
# Schema application
# ---------------------------------------------------------------------------


def _apply_source_schema(frame: pl.LazyFrame, schema: tuple[ColumnSchema, ...]) -> pl.LazyFrame:
    """Cast declared columns to their LoomDtype equivalents.

    Only columns declared in *schema* are cast; all other columns pass
    through unchanged.  No materialization — cast is applied lazily.

    Args:
        frame:  Lazy frame to apply the schema to.
        schema: Tuple of :class:`~loom.etl._schema.ColumnSchema` entries.

    Returns:
        Original frame when *schema* is empty; otherwise a new lazy frame
        with cast expressions for the declared columns.
    """
    if not schema:
        return frame
    cast_exprs = [pl.col(col.name).cast(loom_type_to_polars(col.dtype)) for col in schema]
    return frame.with_columns(cast_exprs)


# ---------------------------------------------------------------------------
# File format readers
# ---------------------------------------------------------------------------


def _read_csv(path: str, options: Any) -> pl.LazyFrame:
    opts = options if isinstance(options, CsvReadOptions) else CsvReadOptions()
    kwargs: dict[str, Any] = {
        "separator": opts.separator,
        "has_header": opts.has_header,
        "encoding": opts.encoding,
        "infer_schema_length": opts.infer_schema_length,
        "skip_rows": opts.skip_rows,
    }
    if opts.null_values:
        kwargs["null_values"] = list(opts.null_values)
    return pl.scan_csv(path, **kwargs)


def _read_json(path: str, options: Any) -> pl.LazyFrame:
    opts = options if isinstance(options, JsonReadOptions) else JsonReadOptions()
    return pl.scan_ndjson(path, infer_schema_length=opts.infer_schema_length)


def _read_excel(path: str, options: Any) -> pl.LazyFrame:
    opts = options if isinstance(options, ExcelReadOptions) else ExcelReadOptions()
    if opts.sheet_name is not None:
        df: pl.DataFrame = pl.read_excel(
            path, sheet_name=opts.sheet_name, has_header=opts.has_header
        )
    else:
        df = pl.read_excel(path, has_header=opts.has_header)
    return df.lazy()


def _read_parquet(path: str, options: Any) -> pl.LazyFrame:
    # ParquetReadOptions has no fields currently — schema carries type overrides
    return pl.scan_parquet(path)


_FILE_READERS: dict[Format, Any] = {
    Format.CSV: _read_csv,
    Format.JSON: _read_json,
    Format.XLSX: _read_excel,
    Format.PARQUET: _read_parquet,
}
