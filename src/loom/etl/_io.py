"""ETL I/O protocols — TableDiscovery, SourceReader, TargetWriter.

These protocols form the extension contract between the ETL framework and
backend-specific I/O implementations.  They are intentionally thin to allow
diverse implementations (delta-rs, Spark, in-memory stubs).

Dependency direction
--------------------
``_io.py`` depends only on domain types (``TableRef``, ``SourceSpec``,
``TargetSpec``).  It does **not** import from the compiler layer, avoiding
circular dependencies.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from loom.etl._schema import ColumnSchema
from loom.etl._source import SourceSpec
from loom.etl._table import TableRef
from loom.etl._target import TargetSpec


@runtime_checkable
class TableDiscovery(Protocol):
    """Protocol for catalog-backed table existence and schema discovery.

    Used by :class:`~loom.etl.compiler.ETLCompiler` when a catalog is
    injected at construction time to validate that all declared source
    tables and targets exist before execution begins.

    Example::

        class HiveCatalog:
            def exists(self, ref: TableRef) -> bool: ...
            def columns(self, ref: TableRef) -> tuple[str, ...]: ...

        plan = ETLCompiler(catalog=HiveCatalog()).compile(MyPipeline)
    """

    def exists(self, ref: TableRef) -> bool:
        """Return ``True`` if the table exists in the catalog.

        Args:
            ref: Logical table reference to check.

        Returns:
            ``True`` if the table is registered in the catalog.
        """
        ...

    def columns(self, ref: TableRef) -> tuple[str, ...]:
        """Return the column names of the table.

        Args:
            ref: Logical table reference.

        Returns:
            Tuple of column names in schema order, or ``()`` if unknown.
        """
        ...

    def schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        """Return the full column schema of the table.

        Args:
            ref: Logical table reference.

        Returns:
            Ordered tuple of :class:`~loom.etl._schema.ColumnSchema` entries,
            or ``None`` if the table does not yet exist in the catalog
            (first write).
        """
        ...

    def update_schema(self, ref: TableRef, schema: tuple[ColumnSchema, ...]) -> None:
        """Persist the table schema after a successful write.

        Called by the target writer once a write completes so the catalog
        reflects the current schema for subsequent steps.

        Args:
            ref:    Logical table reference.
            schema: New authoritative schema for the table.
        """
        ...


@runtime_checkable
class SourceReader(Protocol):
    """Protocol for reading a declared ETL source into a frame.

    Implementations are responsible for:

    * Choosing the correct scan API — lazy scan is **strongly preferred**
      over eager read to avoid loading full datasets into memory.
    * Resolving :class:`~loom.etl._proxy.ParamExpr` values from
      ``params_instance`` via :func:`~loom.etl._proxy.resolve_param_expr`
      and translating :data:`~loom.etl._predicate.PredicateNode` predicates
      to native filter expressions for pushdown at scan time.
    * Returning a *lazy* frame where the backend supports it
      (e.g. ``polars.LazyFrame``, ``pyspark.sql.DataFrame``).

    Example (Polars Delta)::

        class PolarsDeltaReader:
            def read(self, spec: SourceSpec, params_instance: Any) -> pl.LazyFrame:
                filters = translate_predicates(spec.predicates, params_instance)
                return pl.scan_delta(catalog_path(spec.table_ref)).filter(filters)
    """

    def read(self, spec: SourceSpec, params_instance: Any) -> Any:
        """Read the source and return a frame.

        Args:
            spec:            Compiled source specification (kind, path/ref, predicates).
            params_instance: Concrete params for the current run — used to
                             resolve :class:`~loom.etl._proxy.ParamExpr` nodes
                             inside predicates.

        Returns:
            A frame — ``polars.LazyFrame``, ``pyspark.sql.DataFrame``, or any
            type accepted by the step's ``execute()`` method.
        """
        ...


@runtime_checkable
class TargetWriter(Protocol):
    """Protocol for writing a frame produced by ``execute()`` to the target.

    The writer receives the frame exactly as returned by ``execute()``.
    For lazy backends the writer is responsible for triggering materialisation
    (e.g. ``LazyFrame.collect()``) **only at write time** — the framework
    never materialises frames before passing them here.

    Example (Polars Delta)::

        class PolarsDeltaWriter:
            def write(
                self, frame: pl.LazyFrame, spec: TargetSpec, params_instance: Any
            ) -> None:
                partition_val = resolve_param_expr(spec.partition_by, params_instance)
                write_deltalake(catalog_path(spec.table_ref), frame.collect(), ...)
    """

    def write(self, frame: Any, spec: TargetSpec, params_instance: Any) -> None:
        """Write the frame to the target.

        Args:
            frame:           Frame produced by the step's ``execute()`` method.
            spec:            Compiled target specification (mode, path/ref,
                             partition_by, upsert_keys, etc.).
            params_instance: Concrete params for the current run.
        """
        ...
