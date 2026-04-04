"""Spark file target writer (CSV/JSON/PARQUET/DELTA)."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame

from loom.etl.io._format import Format
from loom.etl.io._write_options import CsvWriteOptions, ParquetWriteOptions
from loom.etl.io.target._file import FileSpec


class SparkFileWriter:
    """Write FILE targets with Spark DataFrameWriter."""

    def write(self, frame: DataFrame, spec: FileSpec) -> None:
        """Write one file target according to the requested format/options."""
        _FILE_WRITERS[spec.format](frame, spec.path, spec.write_options)


def _write_delta(df: DataFrame, path: str, _options: Any) -> None:
    df.write.format("delta").mode("overwrite").save(path)


def _write_csv(df: DataFrame, path: str, options: Any) -> None:
    opts = options if isinstance(options, CsvWriteOptions) else CsvWriteOptions()
    (
        df.write.mode("overwrite")
        .option("sep", opts.separator)
        .option("header", str(opts.has_header).lower())
        .csv(path)
    )


def _write_json(df: DataFrame, path: str, _options: Any) -> None:
    df.write.mode("overwrite").json(path)


def _write_parquet(df: DataFrame, path: str, options: Any) -> None:
    opts = options if isinstance(options, ParquetWriteOptions) else ParquetWriteOptions()
    df.write.mode("overwrite").option("compression", opts.compression).parquet(path)


def _write_xlsx(_df: DataFrame, _path: str, _options: Any) -> None:
    raise TypeError(
        "Spark file writer does not support XLSX natively. "
        "Use CSV/JSON/PARQUET, or add spark-excel and a custom TargetWriter."
    )


_FILE_WRITERS: dict[Format, Any] = {
    Format.DELTA: _write_delta,
    Format.CSV: _write_csv,
    Format.JSON: _write_json,
    Format.PARQUET: _write_parquet,
    Format.XLSX: _write_xlsx,
}
