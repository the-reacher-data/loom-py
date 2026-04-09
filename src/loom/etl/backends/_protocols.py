"""Protocols for backend composition.

These protocols define the contract that any compute backend must satisfy
to work with GenericSourceReader and GenericTargetWriter. They provide
static type checking without runtime overhead.

Example:
    >>> from loom.etl.backends._protocols import ComputeBackend
    >>> def process(backend: ComputeBackend[DataFrame]) -> DataFrame:
    ...     return backend.read.table(target)
"""

from __future__ import annotations

from typing import Any, Protocol, TypeVar, runtime_checkable

from loom.etl.io.target import SchemaMode
from loom.etl.storage.routing import ResolvedTarget

FrameT = TypeVar("FrameT")
PhysicalSchemaT = TypeVar("PhysicalSchemaT")


class ReadOps(Protocol[FrameT]):
    """Read operations for tables and files."""

    def table(
        self,
        target: ResolvedTarget,
        *,
        columns: tuple[str, ...] | None = None,
        predicates: tuple[Any, ...] = (),
        params: Any = None,
        schema: tuple[Any, ...] = (),
        json_columns: tuple[Any, ...] = (),
    ) -> FrameT:
        """Read Delta table from catalog or path."""
        ...

    def file(
        self,
        path: str,
        format: str | Any,
        options: Any = None,
        *,
        columns: tuple[str, ...] | None = None,
        schema: tuple[Any, ...] | None = None,
        json_columns: tuple[Any, ...] = (),
    ) -> FrameT:
        """Read file (CSV, JSON, Parquet, Delta)."""
        ...

    def sql(self, frames: dict[str, FrameT], query: str) -> FrameT:
        """Execute SQL query over in-memory aliased frames."""
        ...


class SchemaOps(Protocol[FrameT, PhysicalSchemaT]):
    """Schema discovery and alignment operations."""

    def physical(self, target: ResolvedTarget) -> PhysicalSchemaT | None:
        """Read physical schema for validation."""
        ...

    def align(
        self,
        frame: FrameT,
        existing_schema: PhysicalSchemaT | None,
        mode: SchemaMode,
    ) -> FrameT:
        """Align frame schema with existing."""
        ...

    def materialize(self, frame: FrameT, streaming: bool) -> FrameT:
        """Materialize frame (collect if lazy, noop if eager)."""
        ...

    def to_sql(self, predicate: Any, params: Any) -> str:
        """Translate predicate node to SQL expression."""
        ...


class WriteOps(Protocol[FrameT, PhysicalSchemaT]):
    """Write operations for tables and files."""

    def create(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
        partition_cols: tuple[str, ...] = (),
    ) -> None:
        """Create new table (first run)."""
        ...

    def append(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Append to table."""
        ...

    def replace(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Replace table."""
        ...

    def replace_partitions(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
    ) -> None:
        """Replace partitions."""
        ...

    def replace_where(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        predicate: str,
        schema_mode: SchemaMode,
    ) -> None:
        """Replace where predicate matches."""
        ...

    def upsert(
        self,
        frame: FrameT,
        target: ResolvedTarget,
        *,
        keys: tuple[str, ...],
        partition_cols: tuple[str, ...],
        upsert_exclude: tuple[str, ...] = (),
        upsert_include: tuple[str, ...] = (),
        schema_mode: SchemaMode,
        existing_schema: PhysicalSchemaT | None,
        streaming: bool = False,
    ) -> None:
        """Upsert/merge."""
        ...

    def file(
        self,
        frame: FrameT,
        path: str,
        format: str | Any,
        options: Any = None,
        *,
        streaming: bool = False,
    ) -> None:
        """Write file."""
        ...


@runtime_checkable
class ComputeBackend(Protocol[FrameT, PhysicalSchemaT]):
    """Unified compute backend with read, schema, and write operations.

    This protocol defines the contract for all compute backends (Spark, Polars).
    It uses composition to group related operations into three categories:

    - ``read``: Data loading operations (table, file)
    - ``schema``: Schema introspection and alignment
    - ``write``: Data persistence operations

    Example:
        >>> backend = SparkBackend(spark_session)
        >>> df = backend.read.table(target)
        >>> backend.write.append(df, target, schema_mode=SchemaMode.EVOLVE)

    The ``runtime_checkable`` decorator allows isinstance checks:
        >>> isinstance(backend, ComputeBackend)  # True for SparkBackend/PolarsBackend
    """

    read: ReadOps[FrameT]
    schema: SchemaOps[FrameT, PhysicalSchemaT]
    write: WriteOps[FrameT, PhysicalSchemaT]
