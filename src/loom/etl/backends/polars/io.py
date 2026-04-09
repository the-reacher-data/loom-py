"""Polars SourceReader/TargetWriter built on the generic backend IO adapter."""

from __future__ import annotations

import os

import polars as pl

from loom.etl.backends.io import GenericSourceReader, GenericTargetWriter
from loom.etl.storage._config import MissingTablePolicy
from loom.etl.storage._locator import TableLocator, _as_locator
from loom.etl.storage.routing import PathRouteResolver, TableRouteResolver
from loom.etl.storage.schema import SchemaReader

from ._backend import PolarsBackend


class PolarsSourceReader(GenericSourceReader[pl.LazyFrame]):
    """Polars SourceReader implementation."""

    def __init__(
        self,
        locator: str | os.PathLike[str] | TableLocator,
        *,
        route_resolver: TableRouteResolver | None = None,
    ) -> None:
        normalized_locator = _as_locator(locator)
        resolver = route_resolver or PathRouteResolver(normalized_locator)
        super().__init__(
            backend=PolarsBackend(normalized_locator),
            resolver=resolver,
            reader_name="PolarsSourceReader",
            unsupported_source_hint="TEMP sources are handled by IntermediateStore.",
        )


class PolarsTargetWriter(GenericTargetWriter[pl.LazyFrame]):
    """Polars TargetWriter implementation."""

    def __init__(
        self,
        locator: str | os.PathLike[str] | TableLocator,
        *,
        route_resolver: TableRouteResolver | None = None,
        schema_reader: SchemaReader | None = None,
        missing_table_policy: MissingTablePolicy = MissingTablePolicy.SCHEMA_MODE,
    ) -> None:
        normalized_locator = _as_locator(locator)
        resolver = route_resolver or PathRouteResolver(normalized_locator)
        super().__init__(
            backend=PolarsBackend(normalized_locator, schema_reader=schema_reader),
            resolver=resolver,
            missing_table_policy=missing_table_policy,
            writer_name="PolarsTargetWriter",
        )
