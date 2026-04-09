"""Polars source reader - direct implementation without layers."""

from __future__ import annotations

import logging
from typing import Any

import polars as pl

from loom.etl.io._format import Format
from loom.etl.io._read_options import CsvReadOptions, ExcelReadOptions, JsonReadOptions
from loom.etl.io.source import FileSourceSpec, SourceSpec, TableSourceSpec
from loom.etl.schema._schema import ColumnSchema
from loom.etl.storage._locator import _as_locator
from loom.etl.storage.protocols import SourceReader
from loom.etl.storage.routing import (
    CatalogTarget,
    PathRouteResolver,
    TableRouteResolver,
)

from ._dtype import loom_type_to_polars
from ._predicate import predicate_to_polars

_log = logging.getLogger(__name__)


class PolarsSourceReader(SourceReader):
    """Polars source reader - reads Delta tables and files directly."""

    def __init__(
        self,
        locator: str,
        *,
        route_resolver: TableRouteResolver | None = None,
    ) -> None:
        self._locator = _as_locator(locator)
        self._resolver = route_resolver or PathRouteResolver(self._locator)

    def read(self, spec: SourceSpec, params_instance: Any) -> pl.LazyFrame:
        """Read source spec and return lazy frame."""
        if isinstance(spec, TableSourceSpec):
            return self._read_table(spec, params_instance)

        if isinstance(spec, FileSourceSpec):
            return self._read_file(spec)

        raise TypeError(
            f"PolarsSourceReader does not support source kind {spec.kind!r}. "
            "TEMP sources are handled by IntermediateStore."
        )

    def _read_table(self, spec: TableSourceSpec, params: Any) -> pl.LazyFrame:
        """Read Delta table."""
        target = self._resolver.resolve(spec.table_ref)

        if isinstance(target, CatalogTarget):
            raise ValueError(
                "Polars backend only supports path targets for table reads; "
                f"got catalog target for {target.logical_ref.ref!r}."
            )

        location = target.location
        frame = pl.scan_delta(location.uri, storage_options=location.storage_options or None)

        # Apply predicates
        for predicate in spec.predicates:
            frame = frame.filter(predicate_to_polars(predicate, params))

        # Apply column projection
        if spec.columns:
            frame = frame.select(list(spec.columns))

        # Apply schema casting
        frame = self._apply_source_schema(frame, spec.schema)

        # Apply JSON decoding
        frame = self._apply_json_decode(frame, spec.json_columns)

        return frame

    def _read_file(self, spec: FileSourceSpec) -> pl.LazyFrame:
        """Read file (CSV, JSON, Parquet)."""
        frame = self._read_file_by_format(spec.path, spec.format, spec.read_options)

        if spec.columns:
            frame = frame.select(list(spec.columns))

        frame = self._apply_source_schema(frame, spec.schema)
        frame = self._apply_json_decode(frame, spec.json_columns)

        return frame

    @staticmethod
    def _read_file_by_format(path: str, format: Any, options: Any) -> pl.LazyFrame:
        """Dispatch to format-specific reader."""
        fmt = format if isinstance(format, Format) else Format(format)

        if fmt == Format.CSV:
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

        if fmt == Format.JSON:
            opts = options if isinstance(options, JsonReadOptions) else JsonReadOptions()
            return pl.scan_ndjson(path, infer_schema_length=opts.infer_schema_length)

        if fmt == Format.XLSX:
            opts = options if isinstance(options, ExcelReadOptions) else ExcelReadOptions()
            if opts.sheet_name is not None:
                df: pl.DataFrame = pl.read_excel(
                    path, sheet_name=opts.sheet_name, has_header=opts.has_header
                )
            else:
                df = pl.read_excel(path, has_header=opts.has_header)
            return df.lazy()

        if fmt == Format.PARQUET:
            return pl.scan_parquet(path)

        raise ValueError(f"Unsupported format: {fmt}")


def _apply_source_schema(frame: pl.LazyFrame, schema: tuple[ColumnSchema, ...]) -> pl.LazyFrame:
    """Cast declared columns to their LoomDtype equivalents."""
    if not schema:
        return frame

    cast_exprs = [pl.col(col.name).cast(loom_type_to_polars(col.dtype)) for col in schema]
    return frame.with_columns(cast_exprs)


def _apply_json_decode(frame: pl.LazyFrame, json_columns: tuple[Any, ...]) -> pl.LazyFrame:
    """Decode JSON string columns."""
    if not json_columns:
        return frame

    exprs = [
        pl.col(jc.column).str.json_decode(loom_type_to_polars(jc.loom_type)) for jc in json_columns
    ]
    return frame.with_columns(exprs)


def execute_sql(frames: dict[str, pl.LazyFrame], query: str) -> pl.LazyFrame:
    """Execute SQL query against lazy frames registered in a SQLContext.

    Registers each frame by its key in the context and executes the query.
    """
    ctx = pl.SQLContext()
    for name, frame in frames.items():
        ctx.register(name, frame)
    return ctx.execute(query, eager=False)


# Bind methods to class for internal use
PolarsSourceReader._apply_source_schema = staticmethod(_apply_source_schema)
PolarsSourceReader._apply_json_decode = staticmethod(_apply_json_decode)


__all__ = ["PolarsSourceReader", "execute_sql", "_apply_source_schema", "_apply_json_decode"]
