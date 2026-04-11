"""Per-source read options for file-based ETL sources.

Declared on :class:`~loom.etl.FromFile` via ``.with_options()``::

    sources = Sources(
        report=FromFile("s3://raw/export.csv", format=Format.CSV)
            .with_options(CsvReadOptions(separator=";", has_header=False)),
        events=FromFile("s3://raw/events.json", format=Format.JSON)
            .with_options(JsonReadOptions(infer_schema_length=None)),
    )

Options are format-specific and forwarded to the backend reader at execution
time.  They are part of :class:`~loom.etl._source.SourceSpec` and therefore
visible to the compiler.

All option classes are immutable frozen dataclasses.  They accept only the
subset of reader options that is useful at the ETL declaration level — full
backend-specific parameter bags are intentionally not exposed.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CsvReadOptions:
    """Read options for CSV / TSV file sources.

    All fields default to the most common CSV convention.

    Args:
        separator:           Column delimiter character.  Defaults to ``","``.
        has_header:          Whether the first row is a header.  Defaults to
                             ``True``.
        null_values:         Strings that should be interpreted as ``null``.
        encoding:            File encoding.  Defaults to ``"utf8"``.
        infer_schema_length: Number of rows used to infer column types.
                             ``None`` scans the whole file.  Defaults to
                             ``100``.
        skip_rows:           Number of rows to skip before reading.
                             Useful for files with metadata preambles.
    """

    separator: str = ","
    has_header: bool = True
    null_values: tuple[str, ...] = field(default_factory=tuple)
    encoding: str = "utf8"
    infer_schema_length: int | None = 100
    skip_rows: int = 0


@dataclass(frozen=True)
class JsonReadOptions:
    """Read options for newline-delimited JSON (NDJSON) file sources.

    Args:
        infer_schema_length: Number of rows used to infer column types.
                             ``None`` reads the whole file for inference.
                             Defaults to ``100``.
    """

    infer_schema_length: int | None = 100


@dataclass(frozen=True)
class ExcelReadOptions:
    """Read options for Excel (``.xlsx``) file sources.

    Args:
        sheet_name: Sheet to read by name.  ``None`` reads the first sheet
                    (default).
        has_header: Whether the first row is a header.  Defaults to ``True``.
    """

    sheet_name: str | None = None
    has_header: bool = True


@dataclass(frozen=True)
class ParquetReadOptions:
    """Read options for Parquet file sources.

    Parquet is self-describing — schema and types are embedded in the file
    metadata.  Use :meth:`~loom.etl.FromFile.with_schema` when you need to
    override or enforce a specific type mapping.
    """


ReadOptions = CsvReadOptions | JsonReadOptions | ExcelReadOptions | ParquetReadOptions
"""Union of all supported per-source file read option types."""
