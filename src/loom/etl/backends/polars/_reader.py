"""Polars read helpers used by the backend read operations."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import polars as pl

from loom.etl.backends.polars._dtype import loom_type_to_polars
from loom.etl.io._format import Format
from loom.etl.io._read_options import CsvReadOptions, ExcelReadOptions, JsonReadOptions
from loom.etl.io.source import JsonColumnSpec
from loom.etl.schema._schema import ColumnSchema


def read_file_scan(path: str, format: Format | str, options: Any = None) -> pl.LazyFrame:
    """Return a lazy file scan for one supported file format."""
    fmt = format if isinstance(format, Format) else Format(format)
    reader = _FILE_READERS.get(fmt)
    if reader is None:
        raise ValueError(f"Unsupported file format for Polars reader: {fmt!r}")
    return reader(path, options)


def _apply_source_schema(frame: pl.LazyFrame, schema: tuple[ColumnSchema, ...]) -> pl.LazyFrame:
    if not schema:
        return frame
    cast_exprs = [pl.col(col.name).cast(loom_type_to_polars(col.dtype)) for col in schema]
    return frame.with_columns(cast_exprs)


def _apply_json_decode(
    frame: pl.LazyFrame, json_columns: tuple[JsonColumnSpec, ...]
) -> pl.LazyFrame:
    if not json_columns:
        return frame
    exprs = [
        pl.col(jc.column).str.json_decode(loom_type_to_polars(jc.loom_type)) for jc in json_columns
    ]
    return frame.with_columns(exprs)


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
            path,
            sheet_name=opts.sheet_name,
            has_header=opts.has_header,
        )
    else:
        df = pl.read_excel(path, has_header=opts.has_header)
    return df.lazy()


def _read_parquet(path: str, _options: Any) -> pl.LazyFrame:
    return pl.scan_parquet(path)


_LazyFileReader = Callable[[str, Any], pl.LazyFrame]

_FILE_READERS: dict[Format, _LazyFileReader] = {
    Format.CSV: _read_csv,
    Format.JSON: _read_json,
    Format.XLSX: _read_excel,
    Format.PARQUET: _read_parquet,
}
