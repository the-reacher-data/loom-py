"""ETL runtime contracts for catalog discovery and I/O operations.

These protocols define the integration surface between the ETL framework and
backend implementations (Spark, Polars, stubs).

Dependency direction
--------------------
``contracts.py`` depends only on ETL domain types (``TableRef``, ``SourceSpec``,
``TargetSpec``). It does not import compiler or runner modules.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.source import SourceSpec
from loom.etl.declarative.target import TargetSpec
from loom.etl.schema._schema import ColumnSchema


@runtime_checkable
class TableDiscovery(Protocol):
    """Protocol for catalog-backed table existence and schema discovery.

    Used by :class:`~loom.etl.compiler.ETLCompiler` when a catalog is injected
    at construction time to validate that source and target tables are valid
    before execution begins.
    """

    def exists(self, ref: TableRef) -> bool:
        """Return ``True`` when the table exists in the catalog.

        Args:
            ref: Logical table reference to check.

        Returns:
            ``True`` when the table is registered.
        """
        ...

    def columns(self, ref: TableRef) -> tuple[str, ...]:
        """Return table column names in schema order.

        Args:
            ref: Logical table reference.

        Returns:
            Tuple of column names, or ``()`` when unknown.
        """
        ...

    def schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        """Return full table schema.

        Args:
            ref: Logical table reference.

        Returns:
            Ordered column schema tuple, or ``None`` when table is missing.
        """
        ...

    def update_schema(self, ref: TableRef, schema: tuple[ColumnSchema, ...]) -> None:
        """Persist table schema after a successful write.

        Args:
            ref: Logical table reference.
            schema: Authoritative table schema.
        """
        ...


@runtime_checkable
class SourceReader(Protocol):
    """Protocol for reading one ETL source specification into a frame."""

    def read(self, spec: SourceSpec, params_instance: Any, /) -> Any:
        """Read the source and return a frame.

        Args:
            spec: Compiled source specification.
            params_instance: Concrete params for current run.

        Returns:
            Backend frame type accepted by the step ``execute()`` method.
        """
        ...


@runtime_checkable
class SQLExecutor(Protocol):
    """Optional capability protocol for SQL execution over source frames."""

    def execute_sql(self, frames: dict[str, Any], query: str, /) -> Any:
        """Execute a SQL query against backend frames.

        Used by ``StepSQL`` execution path in :class:`~loom.etl.executor.ETLExecutor`.

        Args:
            frames: Alias -> frame mapping from resolved step sources.
            query: SQL query text.

        Returns:
            Backend frame type with query results.
        """
        ...


@runtime_checkable
class TargetWriter(Protocol):
    """Protocol for writing one frame into one ETL target specification."""

    def write(
        self, frame: Any, spec: TargetSpec, params_instance: Any, /, *, streaming: bool = False
    ) -> None:
        """Write frame to target.

        Args:
            frame: Frame returned by step ``execute()``.
            spec: Compiled target specification.
            params_instance: Concrete params for current run.
            streaming: Hint for lazy backends to use streaming materialization.
        """
        ...


__all__ = ["TableDiscovery", "SourceReader", "SQLExecutor", "TargetWriter"]
