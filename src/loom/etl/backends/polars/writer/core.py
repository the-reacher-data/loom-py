"""PolarsTargetWriter dispatching TABLE and FILE target specs."""

from __future__ import annotations

import os
from typing import Any

import polars as pl

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
from loom.etl.storage.route import PathRouteResolver, TableRouteResolver
from loom.etl.storage.schema.delta import DeltaSchemaReader
from loom.etl.storage.schema.reader import SchemaReader

from .file import PolarsFileWriter
from .table import PolarsDeltaTableWriter


class PolarsTargetWriter:
    """Polars implementation of ``TargetWriter`` with TABLE + FILE support.

    Dispatches to :class:`PolarsDeltaTableWriter` for Delta table writes and
    :class:`PolarsFileWriter` for file writes.  Uses composition — no
    inheritance from the underlying writers.

    Args:
        locator: Root URI string, :class:`pathlib.Path`, or any
                 :class:`~loom.etl.storage._locator.TableLocator`.

    Example::

        writer = PolarsTargetWriter("s3://my-lake/")
        writer.write(frame, append_spec, params)
    """

    def __init__(
        self,
        locator: str | os.PathLike[str] | TableLocator,
        *,
        route_resolver: TableRouteResolver | None = None,
        schema_reader: SchemaReader | None = None,
    ) -> None:
        if route_resolver is None:
            from loom.etl.storage._locator import _as_locator

            route_resolver = PathRouteResolver(_as_locator(locator))
        self._table_writer = PolarsDeltaTableWriter(
            locator,
            route_resolver=route_resolver,
            schema_reader=schema_reader or DeltaSchemaReader(),
        )
        self._file_writer = PolarsFileWriter()

    def write(
        self,
        frame: pl.LazyFrame,
        spec: TargetSpec,
        params_instance: Any,
        /,
        *,
        streaming: bool = False,
    ) -> None:
        """Write target spec with Polars according to its spec variant.

        Args:
            frame:           Lazy frame produced by the step's ``execute()``.
            spec:            Compiled target spec variant.
            params_instance: Concrete params for predicate resolution.
            streaming:       When ``True``, file targets use ``sink_*`` and
                             Delta table targets use ``collect(engine="streaming")``.
                             Ignored for UPSERT — MERGE always requires full
                             materialisation.

        Raises:
            TypeError: When *spec* is not a supported target spec type.
        """
        if isinstance(spec, FileSpec):
            self._file_writer.write(frame, spec, streaming=streaming)
            return
        if isinstance(
            spec, (AppendSpec, ReplaceSpec, ReplacePartitionsSpec, ReplaceWhereSpec, UpsertSpec)
        ):
            self._table_writer.write(frame, spec, params_instance, streaming=streaming)
            return
        raise TypeError(f"PolarsTargetWriter does not support target spec: {type(spec)!r}")

    def append(
        self,
        frame: pl.LazyFrame,
        table_ref: TableRef,
        params_instance: Any,
        /,
        *,
        streaming: bool = False,
    ) -> None:
        """Append rows to *table_ref*, creating the table on first write."""
        self._table_writer.append(frame, table_ref, params_instance, streaming=streaming)
