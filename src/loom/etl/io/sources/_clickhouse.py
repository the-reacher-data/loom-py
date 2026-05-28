"""ClickHouse source reader and compatibility re-exports."""

from __future__ import annotations

from typing import Any

import polars as pl

from loom.etl.backends._predicate import predicate_to_sql
from loom.etl.backends.polars._dtype import loom_type_to_polars
from loom.etl.declarative.source._from_clickhouse import FromClickHouse
from loom.etl.declarative.source._specs import ClickHouseSourceSpec
from loom.etl.runtime.contracts import SourceReader
from loom.etl.schema._schema import ColumnSchema


class ClickHouseSourceReader(SourceReader):
    """Read ClickHouse sources through the native clickhouse-connect client."""

    def __init__(
        self,
        url: str | None = None,
        *,
        client: Any | None = None,
    ) -> None:
        self._url = url
        self._client = client

    def read(self, spec: Any, params_instance: Any, /) -> pl.LazyFrame:
        """Execute the ClickHouse source query and return a lazy Polars frame."""
        client = self._client or self._get_client()
        query = self._build_query(spec, params_instance)
        frame = self._query_to_frame(client, query)
        if spec.columns:
            frame = frame.select(list(spec.columns))
        if spec.schema:
            frame = self._apply_source_schema(frame, spec.schema)
        return frame

    def _get_client(self) -> Any:
        if not self._url:
            raise ValueError(
                "ClickHouseSourceReader requires storage.clickhouse.url to be configured "
                "or an explicit client injected at construction time."
            )
        try:
            import clickhouse_connect as _cc  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "ClickHouseSourceReader requires 'clickhouse-connect'. "
                "Install it with: pip install 'loom-py[clickhouse]'"
            ) from exc
        self._client = _cc.get_client(dsn=self._url)
        return self._client

    def _build_query(self, spec: ClickHouseSourceSpec, params_instance: Any) -> str:
        columns = (
            ", ".join(self._quote_identifier(col) for col in spec.columns) if spec.columns else "*"
        )
        distinct = "DISTINCT " if spec.distinct else ""
        query = f"SELECT {distinct}{columns} FROM {self._quote_table_ref(spec.table_ref.ref)}"

        predicates = tuple(predicate_to_sql(pred, params_instance) for pred in spec.predicates)
        if predicates:
            query += " WHERE " + " AND ".join(f"({pred})" for pred in predicates)
        return query

    @staticmethod
    def _quote_identifier(name: str) -> str:
        parts = [part for part in name.split(".") if part]
        if not parts:
            raise ValueError("ClickHouse identifiers must not be empty.")
        return ".".join(f"`{part.replace('`', '``')}`" for part in parts)

    @staticmethod
    def _quote_table_ref(ref: str) -> str:
        return ClickHouseSourceReader._quote_identifier(ref)

    @staticmethod
    def _query_to_frame(client: Any, query: str) -> pl.LazyFrame:
        if hasattr(client, "query_arrow"):
            result = pl.from_arrow(client.query_arrow(query))
            if not isinstance(result, pl.DataFrame):
                raise TypeError("Expected a PyArrow Table from query_arrow(), not a scalar array.")
            return result.lazy()
        if hasattr(client, "query_df"):
            return pl.from_pandas(client.query_df(query)).lazy()
        raise TypeError(
            "ClickHouse client must expose query_arrow() or query_df() to read sources."
        )

    @staticmethod
    def _apply_source_schema(frame: pl.LazyFrame, schema: tuple[ColumnSchema, ...]) -> pl.LazyFrame:
        if not schema:
            return frame
        exprs = [pl.col(col.name).cast(loom_type_to_polars(col.dtype)) for col in schema]
        return frame.with_columns(exprs)


__all__ = ["ClickHouseSourceReader", "ClickHouseSourceSpec", "FromClickHouse"]
