"""Spark file source reader (CSV/JSON/PARQUET/DELTA)."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, DataFrameReader, SparkSession

from loom.etl.io._format import Format
from loom.etl.io._read_options import CsvReadOptions, JsonReadOptions
from loom.etl.io.source import FileSourceSpec

from ._shared import apply_json_decode_spark, apply_source_schema_spark


class SparkFileReader:
    """Read FILE sources into Spark DataFrames."""

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def read(self, spec: FileSourceSpec, _params_instance: Any) -> DataFrame:
        """Read a FILE source spec with Spark-native readers."""
        df = _FILE_READERS[spec.format](self._spark.read, spec.path, spec.read_options)
        if spec.columns:
            df = df.select(list(spec.columns))
        return apply_json_decode_spark(
            apply_source_schema_spark(df, spec.schema),
            spec.json_columns,
        )


def _read_delta(reader: DataFrameReader, path: str, _options: Any) -> DataFrame:
    return reader.format("delta").load(path)


def _read_csv(reader: DataFrameReader, path: str, options: Any) -> DataFrame:
    opts = options if isinstance(options, CsvReadOptions) else CsvReadOptions()
    if opts.skip_rows != 0:
        raise ValueError("Spark CSV reader does not support skip_rows; preprocess the file first")
    if len(opts.null_values) > 1:
        raise ValueError("Spark CSV reader supports at most one null value token")

    csv_reader = (
        reader.option("sep", opts.separator)
        .option("header", str(opts.has_header).lower())
        .option("encoding", opts.encoding)
        .option("inferSchema", "true")
    )
    if opts.null_values:
        csv_reader = csv_reader.option("nullValue", opts.null_values[0])
    if opts.infer_schema_length is None:
        csv_reader = csv_reader.option("samplingRatio", "1.0")
    return csv_reader.csv(path)


def _read_json(reader: DataFrameReader, path: str, options: Any) -> DataFrame:
    opts = options if isinstance(options, JsonReadOptions) else JsonReadOptions()
    json_reader = reader.option("inferSchema", "true")
    if opts.infer_schema_length is None:
        json_reader = json_reader.option("samplingRatio", "1.0")
    return json_reader.json(path)


def _read_parquet(reader: DataFrameReader, path: str, _options: Any) -> DataFrame:
    return reader.parquet(path)


def _read_xlsx(_reader: DataFrameReader, _path: str, _options: Any) -> DataFrame:
    raise TypeError(
        "Spark file reader does not support XLSX natively. "
        "Use CSV/JSON/PARQUET, or add spark-excel and a custom SourceReader."
    )


_FILE_READERS: dict[Format, Any] = {
    Format.DELTA: _read_delta,
    Format.CSV: _read_csv,
    Format.JSON: _read_json,
    Format.PARQUET: _read_parquet,
    Format.XLSX: _read_xlsx,
}
