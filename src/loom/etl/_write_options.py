"""Per-target write options for file-based ETL targets.

Declared on :class:`~loom.etl.IntoFile` via ``.with_options()``::

    target = IntoFile("s3://exports/report.csv", format=Format.CSV)
        .with_options(CsvWriteOptions(separator=";"))

    target = IntoFile("s3://exports/data.parquet", format=Format.PARQUET)
        .with_options(ParquetWriteOptions(compression="zstd"))

Options are format-specific and forwarded to the backend writer at execution
time.  They are part of :class:`~loom.etl._target.TargetSpec` and therefore
visible to the compiler.

For Delta table targets, per-write options are controlled via the storage
locator (``writer:`` key in YAML) which supports ``WriterProperties``
forwarded verbatim to delta-rs.  Use the locator for Delta table compression
settings, not this module.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class CsvWriteOptions:
    """Write options for CSV / TSV file targets.

    Args:
        separator:  Column delimiter character.  Defaults to ``","``.
        has_header: Whether to write a header row.  Defaults to ``True``.
    """

    separator: str = ","
    has_header: bool = True


@dataclass(frozen=True)
class ParquetWriteOptions:
    """Write options for Parquet file targets.

    Args:
        compression: Parquet compression codec.  Accepted values:
                     ``"uncompressed"``, ``"snappy"`` (default),
                     ``"gzip"``, ``"lz4"``, ``"zstd"``, ``"brotli"``.
    """

    compression: Literal["lz4", "uncompressed", "snappy", "gzip", "brotli", "zstd"] = "snappy"


WriteOptions = CsvWriteOptions | ParquetWriteOptions
"""Union of all supported per-target file write option types."""
