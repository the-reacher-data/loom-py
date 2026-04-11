"""Polars source reader."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import polars as pl

from loom.etl.backends._format_registry import resolve_format_handler
from loom.etl.declarative._format import Format
from loom.etl.declarative._read_options import CsvReadOptions, ExcelReadOptions, JsonReadOptions
from loom.etl.declarative.source import FileSourceSpec, SourceSpec, TableSourceSpec
from loom.etl.runtime.contracts import SourceReader
from loom.etl.schema._schema import ColumnSchema
from loom.etl.storage._locator import TableLocator, _as_locator
from loom.etl.storage.routing import PathRouteResolver, PathTarget, TableRouteResolver

from ._dtype import loom_type_to_polars
from ._predicate import predicate_to_polars


class PolarsSourceReader(SourceReader):
    """Read table and file sources using Polars."""

    def __init__(
        self,
        locator: str | TableLocator,
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
            "TEMP sources are handled by CheckpointStore."
        )

    def execute_sql(self, frames: dict[str, Any], query: str) -> pl.LazyFrame:
        """Execute SQL query against backend frames."""
        return execute_sql(frames, query)

    def _read_table(self, spec: TableSourceSpec, params: Any) -> pl.LazyFrame:
        """Read Delta table."""
        target = self._resolver.resolve(spec.table_ref)
        if not isinstance(target, PathTarget):
            raise TypeError(
                "PolarsSourceReader requires path routing; "
                f"got {type(target).__name__} for {spec.table_ref.ref!r}."
            )

        frame = pl.scan_delta(
            target.location.uri,
            storage_options=target.location.storage_options or None,
        )

        for predicate in spec.predicates:
            frame = frame.filter(predicate_to_polars(predicate, params))

        return self._finalize_source_frame(frame, spec)

    def _read_file(self, spec: FileSourceSpec) -> pl.LazyFrame:
        """Read file (CSV, JSON, XLSX, Parquet)."""
        frame = self._read_file_by_format(spec.path, spec.format, spec.read_options)
        return self._finalize_source_frame(frame, spec)

    def _finalize_source_frame(
        self,
        frame: pl.LazyFrame,
        spec: TableSourceSpec | FileSourceSpec,
    ) -> pl.LazyFrame:
        """Apply post-read transformations: columns, schema, json decode."""
        if spec.columns:
            frame = frame.select(list(spec.columns))
        if spec.schema:
            frame = self._apply_source_schema(frame, spec.schema)
        if spec.json_columns:
            frame = self._apply_json_decode(frame, spec.json_columns)
        return frame

    @staticmethod
    def _read_file_by_format(path: str, format: Any, options: Any) -> pl.LazyFrame:
        """Dispatch to format-specific reader."""
        readers: dict[Format, Callable[[str, Any], pl.LazyFrame]] = {
            Format.CSV: PolarsSourceReader._read_csv,
            Format.JSON: PolarsSourceReader._read_json,
            Format.XLSX: PolarsSourceReader._read_xlsx,
            Format.PARQUET: PolarsSourceReader._read_parquet,
        }
        reader = resolve_format_handler(format, readers)
        return reader(path, options)

    @staticmethod
    def _read_csv(path: str, options: Any) -> pl.LazyFrame:
        csv_opts = options if isinstance(options, CsvReadOptions) else CsvReadOptions()
        kwargs: dict[str, Any] = {
            "separator": csv_opts.separator,
            "has_header": csv_opts.has_header,
            "encoding": csv_opts.encoding,
            "infer_schema_length": csv_opts.infer_schema_length,
            "skip_rows": csv_opts.skip_rows,
        }
        if csv_opts.null_values:
            kwargs["null_values"] = list(csv_opts.null_values)
        return pl.scan_csv(path, **kwargs)

    @staticmethod
    def _read_json(path: str, options: Any) -> pl.LazyFrame:
        json_opts = options if isinstance(options, JsonReadOptions) else JsonReadOptions()
        return pl.scan_ndjson(path, infer_schema_length=json_opts.infer_schema_length)

    @staticmethod
    def _read_xlsx(path: str, options: Any) -> pl.LazyFrame:
        excel_opts = options if isinstance(options, ExcelReadOptions) else ExcelReadOptions()
        if excel_opts.sheet_name is not None:
            df = pl.read_excel(
                path,
                sheet_name=excel_opts.sheet_name,
                has_header=excel_opts.has_header,
            )
        else:
            df = pl.read_excel(path, has_header=excel_opts.has_header)
        return df.lazy()

    @staticmethod
    def _read_parquet(path: str, _options: Any) -> pl.LazyFrame:
        return pl.scan_parquet(path)

    @staticmethod
    def _apply_source_schema(frame: pl.LazyFrame, schema: tuple[ColumnSchema, ...]) -> pl.LazyFrame:
        """Cast declared columns to their Loom type."""
        if not schema:
            return frame

        cast_exprs = [pl.col(col.name).cast(loom_type_to_polars(col.dtype)) for col in schema]
        return frame.with_columns(cast_exprs)

    @staticmethod
    def _apply_json_decode(frame: pl.LazyFrame, json_columns: tuple[Any, ...]) -> pl.LazyFrame:
        """Decode JSON string columns."""
        if not json_columns:
            return frame

        exprs = [
            pl.col(jc.column).str.json_decode(loom_type_to_polars(jc.loom_type))
            for jc in json_columns
        ]
        return frame.with_columns(exprs)


def _apply_source_schema(frame: pl.LazyFrame, schema: tuple[ColumnSchema, ...]) -> pl.LazyFrame:
    """Compatibility helper used by tests."""
    return PolarsSourceReader._apply_source_schema(frame, schema)


def _apply_json_decode(frame: pl.LazyFrame, json_columns: tuple[Any, ...]) -> pl.LazyFrame:
    """Compatibility helper used by tests."""
    return PolarsSourceReader._apply_json_decode(frame, json_columns)


def execute_sql(frames: dict[str, pl.LazyFrame], query: str) -> pl.LazyFrame:
    """Execute SQL query against lazy frames registered in a SQLContext."""
    ctx = pl.SQLContext()
    for name, frame in frames.items():
        ctx.register(name, frame)
    return ctx.execute(query, eager=False)


__all__ = ["PolarsSourceReader", "execute_sql", "_apply_source_schema", "_apply_json_decode"]
