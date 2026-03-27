from enum import StrEnum


class Format(StrEnum):
    """Supported I/O formats for ETL sources and targets.

    Used by :class:`~loom.etl.FromFile` and :class:`~loom.etl.IntoFile` to
    declare the physical format of a file-based source or target.
    :attr:`DELTA` is the implicit format for :class:`~loom.etl.FromTable` and
    :class:`~loom.etl.IntoTable` — it does not need to be set explicitly there.

    Example::

        target = IntoFile("s3://exports/report_{run_date}.csv", format=Format.CSV)
    """

    DELTA = "delta"
    CSV = "csv"
    JSON = "json"
    XLSX = "xlsx"
    PARQUET = "parquet"
