"""Spark Delta table writer adapter."""

from __future__ import annotations

import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from loom.etl.io.target import SchemaMode
from loom.etl.io.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpsertSpec,
)
from loom.etl.schema._table import TableRef
from loom.etl.storage._locator import TableLocator
from loom.etl.storage.route import CatalogRouteResolver, PathRouteResolver, TableRouteResolver
from loom.etl.storage.schema.reader import SchemaReader
from loom.etl.storage.schema.spark import SparkSchemaReader
from loom.etl.storage.write import GenericTargetWriter, ReplaceOp, WritePlanner

from .exec import SparkWriteExecutor


class SparkDeltaTableWriter:
    """Adapter exposing table-only writes."""

    def __init__(
        self,
        spark: SparkSession,
        locator: str | os.PathLike[str] | TableLocator | None,
        *,
        route_resolver: TableRouteResolver | None = None,
        schema_reader: SchemaReader | None = None,
    ) -> None:
        if route_resolver is None:
            if locator is None:
                route_resolver = CatalogRouteResolver()
            else:
                route_resolver = PathRouteResolver(_as_locator(locator))
        self._planner = WritePlanner(route_resolver, schema_reader or SparkSchemaReader(spark))
        self._executor = SparkWriteExecutor(spark)
        self._writer: GenericTargetWriter[DataFrame] = GenericTargetWriter(
            planner=self._planner,
            executor=self._executor,
        )

    def write(
        self, frame: DataFrame, spec: Any, params_instance: Any, *, streaming: bool = False
    ) -> None:
        """Write a Delta table target spec."""
        if not isinstance(
            spec, (AppendSpec, ReplaceSpec, ReplacePartitionsSpec, ReplaceWhereSpec, UpsertSpec)
        ):
            raise TypeError(
                f"SparkDeltaTableWriter only supports TABLE targets; got: {type(spec)!r}"
            )
        self._writer.write(frame, spec, params_instance, streaming=streaming)

    def append(
        self,
        frame: DataFrame,
        table_ref: TableRef,
        params_instance: Any,
        *,
        streaming: bool = False,
    ) -> None:
        """Append rows to *table_ref*, creating the table on first write."""
        op = self._planner.plan(
            AppendSpec(table_ref=table_ref, schema_mode=SchemaMode.EVOLVE),
            streaming=streaming,
        )
        if op.existing_schema is None:
            self._executor.execute(
                frame,
                ReplaceOp(
                    target=op.target,
                    schema_mode=SchemaMode.OVERWRITE,
                    streaming=streaming,
                    existing_schema=None,
                ),
                params_instance,
            )
            return
        self._executor.execute(frame, op, params_instance)


def _as_locator(locator: str | os.PathLike[str] | TableLocator) -> TableLocator:
    from loom.etl.storage._locator import _as_locator as coerce_locator

    return coerce_locator(locator)
