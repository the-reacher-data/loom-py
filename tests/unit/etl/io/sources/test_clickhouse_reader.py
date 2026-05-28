"""Tests for the ClickHouse source reader."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import polars as pl
import pyarrow as pa
import pytest

from loom.etl.declarative.expr import col
from loom.etl.declarative.source import FromClickHouse
from loom.etl.io.sources._clickhouse import ClickHouseSourceReader
from loom.etl.schema._schema import ColumnSchema, LoomDtype


@dataclass(frozen=True)
class _Params:
    updated_at_from: datetime


class _FakeArrowClient:
    def __init__(self, table: pa.Table) -> None:
        self.table = table
        self.queries: list[str] = []

    def query_arrow(self, query: str) -> pa.Table:
        self.queries.append(query)
        return self.table


class TestClickHouseReaderArrowPath:
    def test_builds_query_and_applies_schema(self) -> None:
        table = pa.table({"id": ["1"], "amount": [1]})
        client = _FakeArrowClient(table)
        reader = ClickHouseSourceReader(client=client)

        spec = (
            FromClickHouse("analytics.cdc_events")
            .where(col("year") == 2024)
            .columns("id", "amount")
            .with_schema(
                (
                    ColumnSchema("id", LoomDtype.INT64, nullable=False),
                    ColumnSchema("amount", LoomDtype.FLOAT64),
                )
            )
            .distinct()
            ._to_spec("events")
        )

        result = reader.read(spec, object()).collect()

        assert client.queries == [
            "SELECT DISTINCT `id`, `amount` FROM `analytics`.`cdc_events` WHERE (year = 2024)"
        ]
        assert result.schema == {"id": pl.Int64, "amount": pl.Float64}
        assert result["id"].to_list() == [1]
        assert result["amount"].to_list() == [1.0]

    def test_quotes_datetime_predicates_in_query(self) -> None:
        table = pa.table({"id": ["1"]})
        client = _FakeArrowClient(table)
        reader = ClickHouseSourceReader(client=client)

        spec = (
            FromClickHouse("analytics.cdc_events")
            .where(
                col("source_time")
                >= _Params(updated_at_from=datetime(2026, 5, 25, tzinfo=UTC)).updated_at_from
            )
            .unbounded()
            ._to_spec("events")
        )

        reader.read(spec, object()).collect()

        assert client.queries == [
            "SELECT * FROM `analytics`.`cdc_events`"
            " WHERE (source_time >= toDateTime64('2026-05-25 00:00:00', 3, 'UTC'))"
        ]

    def test_select_star_when_no_columns(self) -> None:
        table = pa.table({"x": [1], "y": [2]})
        client = _FakeArrowClient(table)
        spec = FromClickHouse("t").unbounded()._to_spec("t")

        result = ClickHouseSourceReader(client=client).read(spec, None).collect()

        assert "SELECT * FROM" in client.queries[0]
        assert result.columns == ["x", "y"]

    def test_no_schema_returns_frame_as_is(self) -> None:
        table = pa.table({"val": [42]})
        client = _FakeArrowClient(table)
        spec = FromClickHouse("t").unbounded()._to_spec("t")

        result = ClickHouseSourceReader(client=client).read(spec, None).collect()

        assert result["val"].to_list() == [42]
        assert result["val"].dtype == pl.Int64


class TestClickHouseReaderClientInit:
    def test_raises_without_url_or_client(self) -> None:
        reader = ClickHouseSourceReader()
        spec = FromClickHouse("t").unbounded()._to_spec("t")

        with pytest.raises(ValueError, match="clickhouse.url"):
            reader.read(spec, None)

    def test_unknown_client_interface_raises(self) -> None:
        class _BadClient:
            pass

        reader = ClickHouseSourceReader(client=_BadClient())
        spec = FromClickHouse("t").unbounded()._to_spec("t")

        with pytest.raises(TypeError, match="query_arrow"):
            reader.read(spec, None)
