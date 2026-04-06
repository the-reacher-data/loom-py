"""Spark file target writer (CSV / JSON / PARQUET / DELTA)."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pyspark.sql import DataFrame, DataFrameWriter

from loom.etl.io._format import Format
from loom.etl.io._write_options import CsvWriteOptions, JsonWriteOptions, ParquetWriteOptions
from loom.etl.io.target._file import FileSpec


class SparkFileWriter:
    """Write FILE targets with Spark DataFrameWriter.

    Supports CSV, JSON, Parquet and Delta formats.  Backend-specific options
    are forwarded via ``write_options.kwargs`` as ``DataFrameWriter.option()``
    calls — values are coerced to ``str`` as required by the Spark API.

    Example::

        writer = SparkFileWriter()
        writer.write(spark_frame, file_spec)
    """

    def write(self, frame: DataFrame, spec: FileSpec) -> None:
        """Write one file target according to the requested format and options.

        Args:
            frame: Spark DataFrame produced by the step's ``execute()``.
            spec:  File target spec carrying path, format, and write options.

        Raises:
            KeyError: When ``spec.format`` is not a supported file format.
        """
        _FILE_WRITERS[spec.format](frame, spec.path, spec.write_options)


def _apply_kwargs(writer: DataFrameWriter, kwargs: tuple[tuple[str, Any], ...]) -> DataFrameWriter:
    for key, value in kwargs:
        writer = writer.option(key, str(value))
    return writer


def _write_delta(df: DataFrame, path: str, _options: object) -> None:
    df.write.format("delta").mode("overwrite").save(path)


def _write_csv(df: DataFrame, path: str, options: CsvWriteOptions | None) -> None:
    opts = options or CsvWriteOptions()
    writer = (
        df.write.mode("overwrite")
        .option("sep", opts.separator)
        .option("header", str(opts.has_header).lower())
    )
    _apply_kwargs(writer, opts.kwargs).csv(path)


def _write_json(df: DataFrame, path: str, options: JsonWriteOptions | None) -> None:
    opts = options or JsonWriteOptions()
    _apply_kwargs(df.write.mode("overwrite"), opts.kwargs).json(path)


def _write_parquet(df: DataFrame, path: str, options: ParquetWriteOptions | None) -> None:
    opts = options or ParquetWriteOptions()
    writer = df.write.mode("overwrite").option("compression", opts.compression)
    _apply_kwargs(writer, opts.kwargs).parquet(path)


def _write_xlsx(_df: DataFrame, _path: str, _options: object) -> None:
    raise TypeError(
        "Spark file writer does not support XLSX natively. "
        "Use CSV/JSON/PARQUET, or add spark-excel and a custom TargetWriter."
    )


_FILE_WRITERS: dict[Format, Callable[..., None]] = {
    Format.DELTA: _write_delta,
    Format.CSV: _write_csv,
    Format.JSON: _write_json,
    Format.PARQUET: _write_parquet,
    Format.XLSX: _write_xlsx,
}
