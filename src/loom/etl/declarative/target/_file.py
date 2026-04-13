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
        path:          Literal file path/template, or logical alias when
                       ``is_alias=True``.
        format:        Output format.
        is_alias:      When ``True``, *path* is a logical alias resolved via
                       :class:`~loom.etl.storage.FileLocator` at runtime.
                       Set automatically by :meth:`~loom.etl.IntoFile.alias`.
        write_options: Format-specific options (e.g.
                       :class:`~loom.etl.CsvWriteOptions`).
    """

    path: str
    format: Format
    is_alias: bool = False
    write_options: WriteOptions | None = None
