"""Tests for TableDiscovery, SourceReader, TargetWriter protocols and stubs."""

from __future__ import annotations

import pytest

from loom.etl._format import Format
from loom.etl._io import SourceReader, TableDiscovery, TargetWriter
from loom.etl._source import SourceKind, SourceSpec
from loom.etl._table import TableRef
from loom.etl._target import TargetSpec, WriteMode
from loom.etl.testing import StubCatalog, StubSourceReader, StubTargetWriter

_SENTINEL = object()


def _make_source_spec(alias: str) -> SourceSpec:
    return SourceSpec(
        alias=alias,
        kind=SourceKind.TABLE,
        format=Format.DELTA,
        table_ref=TableRef(f"raw.{alias}"),
    )


def _make_target_spec() -> TargetSpec:
    return TargetSpec(
        mode=WriteMode.REPLACE,
        format=Format.DELTA,
        table_ref=TableRef("staging.out"),
    )


@pytest.mark.parametrize(
    "tables,ref,expected",
    [
        ({"raw.orders": ("id", "amount")}, "raw.orders", True),
        ({"raw.orders": ("id",)}, "raw.missing", False),
        ({}, "raw.orders", False),
    ],
)
def test_stub_catalog_exists(tables: dict, ref: str, expected: bool) -> None:
    assert StubCatalog(tables).exists(TableRef(ref)) is expected


def test_stub_catalog_table_no_columns() -> None:
    catalog = StubCatalog({"staging.out": ()})
    assert catalog.exists(TableRef("staging.out")) is True
    assert catalog.columns(TableRef("staging.out")) == ()


@pytest.mark.parametrize(
    "tables,ref,expected",
    [
        ({"raw.orders": ("id", "amount", "year")}, "raw.orders", ("id", "amount", "year")),
        ({"raw.orders": ("id",)}, "raw.missing", ()),
    ],
)
def test_stub_catalog_columns(tables: dict, ref: str, expected: tuple) -> None:
    assert StubCatalog(tables).columns(TableRef(ref)) == expected


def test_stub_catalog_satisfies_protocol() -> None:
    assert isinstance(StubCatalog(), TableDiscovery)


@pytest.mark.parametrize(
    "seeds,alias,expected",
    [
        ({"orders": _SENTINEL}, "orders", _SENTINEL),
        ({"orders": object()}, "customers", None),
        ({}, "orders", None),
    ],
)
def test_stub_source_reader_read(seeds: dict, alias: str, expected: object) -> None:
    reader = StubSourceReader(seeds)
    result = reader.read(_make_source_spec(alias), _params_instance=None)
    assert result is expected


def test_stub_source_reader_satisfies_protocol() -> None:
    assert isinstance(StubSourceReader(), SourceReader)


@pytest.mark.parametrize("n_writes", [0, 1, 3])
def test_stub_target_writer_captures_writes_in_order(n_writes: int) -> None:
    writer = StubTargetWriter()
    frames = [object() for _ in range(n_writes)]
    spec = _make_target_spec()
    for f in frames:
        writer.write(f, spec, _params_instance=None)
    assert [w[0] for w in writer.written] == frames


def test_stub_target_writer_satisfies_protocol() -> None:
    assert isinstance(StubTargetWriter(), TargetWriter)
