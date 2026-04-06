"""Polars file target writer (CSV / JSON / PARQUET)."""

from __future__ import annotations

import logging
from collections.abc import Callable

import polars as pl

from loom.etl.io._format import Format
from loom.etl.io._write_options import (
    CsvWriteOptions,
    JsonWriteOptions,
    ParquetWriteOptions,
    WriteOptions,
)
from loom.etl.io.target._file import FileSpec

_log = logging.getLogger(__name__)


class PolarsFileWriter:
    """Write FILE targets with Polars DataFrame writers.

    Supports CSV, Parquet and JSON (NDJSON) formats.

    When ``streaming=False`` (default), the lazy frame is collected once
    before writing via ``DataFrame.write_*``.

    When ``streaming=True``, writing uses the zero-copy ``LazyFrame.sink_*``
    API — the frame is never fully materialised in memory.  The plan must be
    streaming-compatible; Polars will raise ``InvalidOperationError`` if it
    contains operations that cannot be streamed (e.g. global sort, window
    functions).

    Backend-specific options are forwarded verbatim via
    ``write_options.kwargs``.

    Example::

        writer = PolarsFileWriter()
        writer.write(lazy_frame, file_spec)
        writer.write(lazy_frame, file_spec, streaming=True)
    """

    def write(self, frame: pl.LazyFrame, spec: FileSpec, *, streaming: bool = False) -> None:
        """Write *frame* to the path declared in *spec*.

        Args:
            frame:     Lazy frame produced by the step's ``execute()``.
            spec:      File target spec carrying path, format, and write options.
            streaming: When ``True``, uses ``LazyFrame.sink_*`` for true
                       zero-copy streaming output.  When ``False`` (default),
                       collects the frame first then calls ``DataFrame.write_*``.

        Raises:
            KeyError:              When ``spec.format`` is not supported.
            InvalidOperationError: When ``streaming=True`` but the plan
                                   contains non-streaming operations.
        """
        if streaming:
            _log.debug("streaming write: sink_%s path=%s", spec.format, spec.path)
            _STREAMING_WRITERS[spec.format](frame, spec.path, spec.write_options)
        else:
            _FILE_WRITERS[spec.format](frame.collect(), spec.path, spec.write_options)


# ---------------------------------------------------------------------------
# Collect writers  (DataFrame.write_*)
# ---------------------------------------------------------------------------


_Options = WriteOptions | None


def _write_csv_file(df: pl.DataFrame, path: str, options: _Options) -> None:
    opts = options if isinstance(options, CsvWriteOptions) else CsvWriteOptions()
    df.write_csv(
        path,
        separator=opts.separator,
        include_header=opts.has_header,
        **dict(opts.kwargs),
    )


def _write_parquet_file(df: pl.DataFrame, path: str, options: _Options) -> None:
    opts = options if isinstance(options, ParquetWriteOptions) else ParquetWriteOptions()
    df.write_parquet(path, compression=opts.compression, **dict(opts.kwargs))


def _write_json_file(df: pl.DataFrame, path: str, options: _Options) -> None:
    opts = options if isinstance(options, JsonWriteOptions) else JsonWriteOptions()
    df.write_ndjson(path, **dict(opts.kwargs))


_DataFrameWriter = Callable[[pl.DataFrame, str, _Options], None]

_FILE_WRITERS: dict[Format, _DataFrameWriter] = {
    Format.CSV: _write_csv_file,
    Format.PARQUET: _write_parquet_file,
    Format.JSON: _write_json_file,
}


# ---------------------------------------------------------------------------
# Streaming writers  (LazyFrame.sink_*)
# ---------------------------------------------------------------------------


def _sink_csv_file(frame: pl.LazyFrame, path: str, options: _Options) -> None:
    opts = options if isinstance(options, CsvWriteOptions) else CsvWriteOptions()
    frame.sink_csv(
        path,
        separator=opts.separator,
        include_header=opts.has_header,
        **dict(opts.kwargs),
    )


def _sink_parquet_file(frame: pl.LazyFrame, path: str, options: _Options) -> None:
    opts = options if isinstance(options, ParquetWriteOptions) else ParquetWriteOptions()
    frame.sink_parquet(path, compression=opts.compression, **dict(opts.kwargs))


def _sink_ndjson_file(frame: pl.LazyFrame, path: str, options: _Options) -> None:
    opts = options if isinstance(options, JsonWriteOptions) else JsonWriteOptions()
    frame.sink_ndjson(path, **dict(opts.kwargs))


_LazyFrameWriter = Callable[[pl.LazyFrame, str, _Options], None]

_STREAMING_WRITERS: dict[Format, _LazyFrameWriter] = {
    Format.CSV: _sink_csv_file,
    Format.PARQUET: _sink_parquet_file,
    Format.JSON: _sink_ndjson_file,
}
