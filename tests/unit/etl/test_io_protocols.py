"""Tests for TableDiscovery, SourceReader, TargetWriter protocols and stubs."""

from __future__ import annotations

from loom.etl._format import Format
from loom.etl._io import SourceReader, TableDiscovery, TargetWriter
from loom.etl._source import SourceKind, SourceSpec
from loom.etl._table import TableRef
from loom.etl._target import TargetSpec, WriteMode
from loom.etl.testing import StubCatalog, StubSourceReader, StubTargetWriter


def test_stub_catalog_exists_true() -> None:
    catalog = StubCatalog({"raw.orders": ("id", "amount")})
    assert catalog.exists(TableRef("raw.orders")) is True


def test_stub_catalog_exists_false() -> None:
    catalog = StubCatalog({"raw.orders": ("id",)})
    assert catalog.exists(TableRef("raw.missing")) is False


def test_stub_catalog_empty_init_nothing_exists() -> None:
    catalog = StubCatalog()
    assert catalog.exists(TableRef("raw.orders")) is False


def test_stub_catalog_columns_known() -> None:
    catalog = StubCatalog({"raw.orders": ("id", "amount", "year")})
    assert catalog.columns(TableRef("raw.orders")) == ("id", "amount", "year")


def test_stub_catalog_columns_unknown_returns_empty() -> None:
    catalog = StubCatalog({"raw.orders": ("id",)})
    assert catalog.columns(TableRef("raw.missing")) == ()


def test_stub_catalog_table_no_columns() -> None:
    catalog = StubCatalog({"staging.out": ()})
    assert catalog.exists(TableRef("staging.out")) is True
    assert catalog.columns(TableRef("staging.out")) == ()


def test_stub_catalog_satisfies_protocol() -> None:
    catalog = StubCatalog()
    assert isinstance(catalog, TableDiscovery)


def _make_source_spec(alias: str) -> SourceSpec:
    return SourceSpec(
        alias=alias,
        kind=SourceKind.TABLE,
        format=Format.DELTA,
        table_ref=TableRef(f"raw.{alias}"),
    )


def test_stub_source_reader_returns_seeded_frame() -> None:
    sentinel = object()
    reader = StubSourceReader({"orders": sentinel})
    result = reader.read(_make_source_spec("orders"), _params_instance=None)
    assert result is sentinel


def test_stub_source_reader_missing_alias_returns_none() -> None:
    reader = StubSourceReader({"orders": object()})
    result = reader.read(_make_source_spec("customers"), _params_instance=None)
    assert result is None


def test_stub_source_reader_empty_init_always_none() -> None:
    reader = StubSourceReader()
    result = reader.read(_make_source_spec("orders"), _params_instance=None)
    assert result is None


def test_stub_source_reader_satisfies_protocol() -> None:
    reader = StubSourceReader()
    assert isinstance(reader, SourceReader)


def _make_target_spec() -> TargetSpec:
    return TargetSpec(
        mode=WriteMode.REPLACE,
        format=Format.DELTA,
        table_ref=TableRef("staging.out"),
    )


def test_stub_target_writer_captures_write() -> None:
    writer = StubTargetWriter()
    frame = object()
    spec = _make_target_spec()
    writer.write(frame, spec, _params_instance=None)
    assert len(writer.written) == 1
    assert writer.written[0] == (frame, spec)


def test_stub_target_writer_captures_multiple_writes_in_order() -> None:
    writer = StubTargetWriter()
    frames = [object(), object()]
    spec = _make_target_spec()
    for f in frames:
        writer.write(f, spec, _params_instance=None)
    assert [w[0] for w in writer.written] == frames


def test_stub_target_writer_empty_on_init() -> None:
    writer = StubTargetWriter()
    assert writer.written == []


def test_stub_target_writer_satisfies_protocol() -> None:
    writer = StubTargetWriter()
    assert isinstance(writer, TargetWriter)
