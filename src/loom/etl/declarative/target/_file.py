"""FILE target variant spec.

Internal module — import from :mod:`loom.etl.declarative.target`.
"""

from __future__ import annotations

from dataclasses import dataclass

from loom.etl.declarative._format import Format
from loom.etl.declarative._write_options import WriteOptions


@dataclass(frozen=True)
class FileSpec:
    """Write result to a file path (CSV, JSON, Parquet, XLSX).

    The write mode is always full-replace (overwrite).

    Args:
        path:          File path or template, e.g.
                       ``"s3://exports/orders_{run_date}.csv"``.
        format:        Output format.
        write_options: Format-specific options (e.g.
                       :class:`~loom.etl.CsvWriteOptions`).
    """

    path: str
    format: Format
    write_options: WriteOptions | None = None
