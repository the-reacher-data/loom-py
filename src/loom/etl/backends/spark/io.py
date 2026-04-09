"""Spark SourceReader/TargetWriter built on the generic backend IO adapter."""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession

from loom.etl.backends.io import GenericSourceReader, GenericTargetWriter
from loom.etl.storage._config import MissingTablePolicy
from loom.etl.storage._locator import TableLocator, _as_locator
from loom.etl.storage.route import (
    CatalogRouteResolver,
    PathRouteResolver,
    TableRouteResolver,
)

from ._backend import SparkBackend


class SparkSourceReader(GenericSourceReader[DataFrame]):
    """Spark SourceReader implementation."""

    def __init__(
        self,
        spark: SparkSession,
        locator: str | os.PathLike[str] | TableLocator | None = None,
        *,
        route_resolver: TableRouteResolver | None = None,
    ) -> None:
        if route_resolver is None:
            if locator is None:
                resolver: TableRouteResolver = CatalogRouteResolver()
            else:
                resolver = PathRouteResolver(_as_locator(locator))
        else:
            resolver = route_resolver
        super().__init__(
            backend=SparkBackend(spark), resolver=resolver, reader_name="SparkSourceReader"
        )


class SparkTargetWriter(GenericTargetWriter[DataFrame]):
    """Spark TargetWriter implementation."""

    def __init__(
        self,
        spark: SparkSession,
        locator: str | os.PathLike[str] | TableLocator | None,
        *,
        route_resolver: TableRouteResolver | None = None,
        missing_table_policy: MissingTablePolicy = MissingTablePolicy.SCHEMA_MODE,
    ) -> None:
        if route_resolver is None:
            if locator is None:
                resolver: TableRouteResolver = CatalogRouteResolver()
            else:
                resolver = PathRouteResolver(_as_locator(locator))
        else:
            resolver = route_resolver
        super().__init__(
            backend=SparkBackend(spark),
            resolver=resolver,
            missing_table_policy=missing_table_policy,
            writer_name="SparkTargetWriter",
        )
