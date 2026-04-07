"""Polars Delta table writer adapter."""

from __future__ import annotations

import os
from typing import Any

import polars as pl

from loom.etl.io.target import SchemaMode
from loom.etl.io.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpsertSpec,
)
from loom.etl.schema._table import TableRef
from loom.etl.storage._locator import TableLocator, _as_locator
from loom.etl.storage.route import PathRouteResolver, TableRouteResolver
from loom.etl.storage.schema.delta import DeltaSchemaReader
from loom.etl.storage.schema.reader import SchemaReader
from loom.etl.storage.write import GenericTargetWriter, ReplaceOp, WritePlanner

from .exec import PolarsWriteExecutor


class PolarsDeltaTableWriter:
    """Adapter exposing table-only writes."""

    def __init__(
        self,
        locator: str | os.PathLike[str] | TableLocator,
        *,
        route_resolver: TableRouteResolver | None = None,
        schema_reader: SchemaReader | None = None,
    ) -> None:
        normalized_locator = _as_locator(locator)
        if route_resolver is None:
            route_resolver = PathRouteResolver(normalized_locator)
        self._planner = WritePlanner(route_resolver, schema_reader or DeltaSchemaReader())
        self._executor = PolarsWriteExecutor()
        self._writer: GenericTargetWriter[pl.LazyFrame] = GenericTargetWriter(
            planner=self._planner,
            executor=self._executor,
        )

    def write(
        self,
        frame: pl.LazyFrame,
        spec: object,
        params_instance: Any,
        *,
        streaming: bool = False,
    ) -> None:
        """Write a Delta table target spec."""
        if not isinstance(
            spec, (AppendSpec, ReplaceSpec, ReplacePartitionsSpec, ReplaceWhereSpec, UpsertSpec)
        ):
            raise TypeError(
                f"PolarsDeltaTableWriter only supports TABLE targets; got: {type(spec)!r}"
            )
        self._writer.write(frame, spec, params_instance, streaming=streaming)

    def append(
        self,
        frame: pl.LazyFrame,
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
