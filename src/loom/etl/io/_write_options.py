"""Per-target write options for file-based ETL targets.

Declared on :class:`~loom.etl.IntoFile` via ``.with_options()``::

    target = IntoFile("s3://exports/report.csv", format=Format.CSV)
        .with_options(CsvWriteOptions(separator=";"))

    target = IntoFile("s3://exports/data.parquet", format=Format.PARQUET)
        .with_options(ParquetWriteOptions(compression="zstd"))

Options are format-specific and forwarded to the backend writer at execution
time.  They are part of :class:`~loom.etl._target.TargetSpec` and therefore
visible to the compiler.

Each class exposes only the parameters that carry **semantic meaning at the
framework level** (dialect contract, storage codec).  Everything else is
forwarded verbatim via the ``kwargs`` escape hatch as a tuple of
``(key, value)`` pairs::

    ParquetWriteOptions(
        compression="zstd",
        kwargs=(("statistics", True), ("row_group_size", 100_000)),
    )

    CsvWriteOptions(
        separator=";",
        kwargs=(("datetime_format", "%Y-%m-%d"), ("null_value", "N/A")),
    )

For Polars targets, ``kwargs`` maps to keyword arguments of
``polars.DataFrame.write_csv / write_parquet / write_ndjson``.
For Spark targets, each pair becomes a ``DataFrameWriter.option(key, value)``
call — so values are always coerced to ``str`` by the Spark writer.

For Delta table targets, per-write options are controlled via the storage
locator (``writer:`` key in YAML) which supports ``WriterProperties``
forwarded verbatim to delta-rs.  Use the locator for Delta compression
settings, not this module.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True)
class CsvWriteOptions:
    """Write options for CSV / TSV file targets.

    Args:
        separator:  Column delimiter character.  Defaults to ``","``.
        has_header: Whether to write a header row.  Defaults to ``True``.
        kwargs:     Backend-specific keyword arguments forwarded verbatim.
                    For Polars: mapped to ``polars.DataFrame.write_csv()``.
                    For Spark: applied via ``DataFrameWriter.option(k, v)``.

    Example::

        CsvWriteOptions(
            separator=";",
            kwargs=(("datetime_format", "%Y-%m-%d"), ("null_value", "N/A")),
        )
    """

    separator: str = ","
    has_header: bool = True
    kwargs: tuple[tuple[str, Any], ...] = ()


@dataclass(frozen=True)
class ParquetWriteOptions:
    """Write options for Parquet file targets.

    Args:
        compression: Parquet compression codec.  Defaults to ``"zstd"``.
        kwargs:      Backend-specific keyword arguments forwarded verbatim.
                     For Polars: mapped to ``polars.DataFrame.write_parquet()``.
                     For Spark: applied via ``DataFrameWriter.option(k, v)``.

    Example::

        ParquetWriteOptions(
            compression="zstd",
            kwargs=(("statistics", True), ("row_group_size", 100_000)),
        )
    """

    compression: Literal["lz4", "uncompressed", "snappy", "gzip", "brotli", "zstd"] = "zstd"
    kwargs: tuple[tuple[str, Any], ...] = ()


@dataclass(frozen=True)
class JsonWriteOptions:
    """Write options for JSON (NDJSON) file targets.

    Args:
        kwargs: Backend-specific keyword arguments forwarded verbatim.
                For Polars: mapped to ``polars.DataFrame.write_ndjson()``.
                For Spark: applied via ``DataFrameWriter.option(k, v)``.

    Example::

        JsonWriteOptions(kwargs=(("compression", "gzip"),))
    """

    kwargs: tuple[tuple[str, Any], ...] = ()


WriteOptions = CsvWriteOptions | ParquetWriteOptions | JsonWriteOptions
"""Union of all supported per-target file write option types."""
