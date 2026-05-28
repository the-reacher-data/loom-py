"""Tests for the ClickHouse source reader."""

from __future__ import annotations

import polars as pl
import pyarrow as pa

from loom.etl.declarative.expr import col
from loom.etl.declarative.source import FromClickHouse
from loom.etl.io.sources._clickhouse import ClickHouseSourceReader
from loom.etl.schema._schema import ColumnSchema, LoomDtype


class _FakeClickHouseClient:
    def __init__(self, table: pa.Table) -> None:
        self.table = table
        self.queries: list[str] = []

    def query_arrow(self, query: str) -> pa.Table:
        self.queries.append(query)
        return self.table


class TestClickHouseReader:
    def test_builds_query_and_applies_schema(self) -> None:
        table = pa.table({"id": ["1"], "amount": [1]})
        client = _FakeClickHouseClient(table)
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

        frame = reader.read(spec, object())
        result = frame.collect()

        assert client.queries == [
            "SELECT DISTINCT `id`, `amount` FROM `analytics`.`cdc_events` WHERE (year = 2024)"
        ]
        assert result.schema == {"id": pl.Int64, "amount": pl.Float64}
        assert result["id"].to_list() == [1]
        assert result["amount"].to_list() == [1.0]
