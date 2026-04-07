"""SparkTargetWriter dispatching TABLE and FILE target specs."""

from __future__ import annotations

import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from loom.etl.io.target import TargetSpec
from loom.etl.io.target._file import FileSpec
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

from .file import SparkFileWriter
from .table import SparkDeltaTableWriter


class SparkTargetWriter:
    """Spark implementation of ``TargetWriter`` with TABLE + FILE support.

    Dispatches to :class:`SparkDeltaTableWriter` for Delta table writes and
    :class:`SparkFileWriter` for file writes.  Uses composition — no
    inheritance from the underlying writers.

    Args:
        spark:   Active :class:`~pyspark.sql.SparkSession`.
        locator: Root URI string, :class:`pathlib.Path`, or any
                 :class:`~loom.etl.storage._locator.TableLocator`.
                 ``None`` is accepted when Unity Catalog manages table paths.

    Example::

        writer = SparkTargetWriter(spark, "s3://my-lake/")
        writer.write(frame, append_spec, params)
    """

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
        self._table_writer = SparkDeltaTableWriter(
            spark,
            locator,
            route_resolver=route_resolver,
            schema_reader=schema_reader,
        )
        self._file_writer = SparkFileWriter()

    def write(
        self,
        frame: DataFrame,
        spec: TargetSpec,
        params_instance: Any,
        /,
        *,
        streaming: bool = False,
    ) -> None:
        """Write target spec with Spark according to its spec variant.

        Args:
            frame:           Spark DataFrame produced by the step's ``execute()``.
            spec:            Compiled target spec variant.
            params_instance: Concrete params for predicate resolution.
            streaming:       Ignored — Spark manages its own execution model.

        Raises:
            TypeError: When *spec* is not a supported target spec type.
        """
        _ = streaming
        if isinstance(spec, FileSpec):
            self._file_writer.write(frame, spec)
            return
        if isinstance(
            spec, (AppendSpec, ReplaceSpec, ReplacePartitionsSpec, ReplaceWhereSpec, UpsertSpec)
        ):
            self._table_writer.write(frame, spec, params_instance)
            return
        raise TypeError(f"SparkTargetWriter does not support target spec: {type(spec)!r}")

    def append(
        self,
        frame: DataFrame,
        table_ref: TableRef,
        params_instance: Any,
        /,
        *,
        streaming: bool = False,
    ) -> None:
        """Append rows to *table_ref*, creating the table on first write."""
        self._table_writer.append(frame, table_ref, params_instance, streaming=streaming)


def _as_locator(locator: str | os.PathLike[str] | TableLocator) -> TableLocator:
    from loom.etl.storage._locator import _as_locator as coerce_locator

    return coerce_locator(locator)
