"""Unit tests for PolarsFileWriter dispatch and streaming behavior."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from loom.etl.backends.polars.writer import file as file_mod
from loom.etl.backends.polars.writer.file import PolarsFileWriter
from loom.etl.io._format import Format
from loom.etl.io._write_options import CsvWriteOptions
from loom.etl.io.target._file import FileSpec


def test_write_streaming_dispatches_to_sink_writer(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def _spy_sink(frame: pl.LazyFrame, _path: str, _options: object) -> None:
        calls.append(("sink", type(frame).__name__))

    def _spy_collect(_df: pl.DataFrame, _path: str, _options: object) -> None:
        calls.append(("collect", "DataFrame"))

    monkeypatch.setitem(file_mod._STREAMING_WRITERS, Format.CSV, _spy_sink)
    monkeypatch.setitem(file_mod._FILE_WRITERS, Format.CSV, _spy_collect)

    writer = PolarsFileWriter()
    writer.write(
        pl.DataFrame({"id": [1]}).lazy(),
        FileSpec(path="out.csv", format=Format.CSV),
        streaming=True,
    )

    assert calls == [("sink", "LazyFrame")]


def test_write_non_streaming_dispatches_to_collect_writer(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def _spy_sink(_frame: pl.LazyFrame, _path: str, _options: object) -> None:
        calls.append(("sink", "LazyFrame"))

    def _spy_collect(df: pl.DataFrame, _path: str, _options: object) -> None:
        calls.append(("collect", type(df).__name__))

    monkeypatch.setitem(file_mod._STREAMING_WRITERS, Format.CSV, _spy_sink)
    monkeypatch.setitem(file_mod._FILE_WRITERS, Format.CSV, _spy_collect)

    writer = PolarsFileWriter()
    writer.write(
        pl.DataFrame({"id": [1]}).lazy(),
        FileSpec(path="out.csv", format=Format.CSV),
        streaming=False,
    )

    assert calls == [("collect", "DataFrame")]


def test_streaming_csv_write_applies_options(tmp_path: Path) -> None:
    out = tmp_path / "sink.csv"
    writer = PolarsFileWriter()
    spec = FileSpec(
        path=str(out),
        format=Format.CSV,
        write_options=CsvWriteOptions(separator=";", has_header=False),
    )

    writer.write(pl.DataFrame({"id": [1], "value": [10]}).lazy(), spec, streaming=True)

    assert out.read_text(encoding="utf-8").strip() == "1;10"
