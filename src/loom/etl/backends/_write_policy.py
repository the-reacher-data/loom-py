"""Shared write policy via Template Method pattern.

This module provides the base class for all backend-specific target writers.
It implements the common "check-exists → create OR align+write" policy,
while delegating backend-specific operations to abstract hooks.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any, Generic, TypeVar

from loom.etl.declarative.target import (
    AppendSpec,
    FileSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    SchemaMode,
    TargetSpec,
    TempFanInSpec,
    TempSpec,
    UpsertSpec,
)
from loom.etl.declarative.target._history import HistorifyRepairReport, HistorifySpec
from loom.etl.runtime.contracts import TargetWriter
from loom.etl.schema._schema import SchemaNotFoundError
from loom.etl.storage._config import MissingTablePolicy
from loom.etl.storage.routing import ResolvedTarget, TableRouteResolver

InputFrameT = TypeVar("InputFrameT")
WriteFrameT = TypeVar("WriteFrameT")
PhysicalSchemaT = TypeVar("PhysicalSchemaT")


def _ensure_can_create_missing_table(
    *,
    target: ResolvedTarget,
    schema_mode: SchemaMode,
    missing_table_policy: MissingTablePolicy,
) -> None:
    """Validate whether the write path may create a missing destination table."""
    if _can_create_missing_table(
        schema_mode=schema_mode, missing_table_policy=missing_table_policy
    ):
        return
    raise SchemaNotFoundError(
        f"Destination table does not yet exist: {target}. "
        "Use SchemaMode.OVERWRITE or set storage.missing_table_policy='create'."
    )


def _can_create_missing_table(
    *,
    schema_mode: SchemaMode,
    missing_table_policy: MissingTablePolicy,
) -> bool:
    """Return ``True`` when table creation is allowed for missing destination."""
    return missing_table_policy is MissingTablePolicy.CREATE or schema_mode is SchemaMode.OVERWRITE


class _WritePolicy(TargetWriter, Generic[InputFrameT, WriteFrameT, PhysicalSchemaT]):
    """Base class for backend-specific target writers using Template Method pattern.

    Implements the shared write policy:
    1. Check if target exists
    2. If not exists → validate can create → create
    3. If exists → align schema → materialize → write

    Backend subclasses implement the hooks (_physical_schema, _append, etc.).
    """

    def __init__(
        self,
        *,
        resolver: TableRouteResolver,
        missing_table_policy: MissingTablePolicy,
    ) -> None:
        self._resolver = resolver
        self._missing_table_policy = missing_table_policy

    # ========================================================================
    # Public API (from TargetWriter)
    # ========================================================================

    def write(
        self,
        frame: InputFrameT,
        spec: TargetSpec,
        params_instance: Any,
        /,
        *,
        streaming: bool = False,
    ) -> None:
        """Write frame according to spec."""
        if isinstance(spec, FileSpec):
            self._write_file(frame, spec, streaming=streaming)
            return
        if isinstance(spec, (TempSpec, TempFanInSpec)):
            raise TypeError(
                f"{type(self).__name__} does not support temp targets; "
                "TEMP writes are handled by CheckpointStore in ETLExecutor."
            )

        target = self._resolver.resolve(spec.table_ref)
        self._dispatch_table_write(frame, target, spec, params_instance, streaming)

    def _dispatch_table_write(
        self,
        frame: InputFrameT,
        target: ResolvedTarget,
        spec: TargetSpec,
        params_instance: Any,
        streaming: bool,
    ) -> None:
        """Dispatch table write to specific handler.

        This method centralizes the dispatch logic for all table write operations.
        When adding a new write mode (e.g., MergeSpec, DeleteWhereSpec), add a
        new case here and implement the corresponding ``_do_*`` method.
        """
        match spec:
            case AppendSpec():
                self._do_append(frame, target, spec, streaming)
            case ReplaceSpec():
                self._do_replace(frame, target, spec, streaming)
            case ReplacePartitionsSpec():
                self._do_replace_partitions(frame, target, spec, streaming)
            case ReplaceWhereSpec():
                self._do_replace_where(frame, target, spec, params_instance, streaming)
            case UpsertSpec():
                self._do_upsert(frame, target, spec, streaming)
            case HistorifySpec():
                self._do_historify(frame, target, spec, params_instance)
            case _:
                raise TypeError(f"Unsupported target spec: {type(spec)!r}")

    # ========================================================================
    # Template Methods (shared policy)
    # ========================================================================

    def _do_append(
        self,
        frame: InputFrameT,
        target: ResolvedTarget,
        spec: AppendSpec,
        streaming: bool,
    ) -> None:
        """Append policy: check exists → create OR align → write."""
        existing = self._physical_schema(target)
        if existing is None:
            _ensure_can_create_missing_table(
                target=target,
                schema_mode=spec.schema_mode,
                missing_table_policy=self._missing_table_policy,
            )
            materialized = self._materialize_for_write(frame, streaming)
            self._create(materialized, target, schema_mode=spec.schema_mode)
            return
        aligned = self._align(frame, existing, spec.schema_mode)
        materialized = self._materialize_for_write(aligned, streaming)
        self._append(materialized, target, schema_mode=spec.schema_mode)

    def _do_replace(
        self,
        frame: InputFrameT,
        target: ResolvedTarget,
        spec: ReplaceSpec,
        streaming: bool,
    ) -> None:
        """Replace policy: check exists → create OR align → write."""
        existing = self._physical_schema(target)
        if existing is None:
            _ensure_can_create_missing_table(
                target=target,
                schema_mode=spec.schema_mode,
                missing_table_policy=self._missing_table_policy,
            )
            materialized = self._materialize_for_write(frame, streaming)
            self._create(materialized, target, schema_mode=spec.schema_mode)
            return
        aligned = self._align(frame, existing, spec.schema_mode)
        materialized = self._materialize_for_write(aligned, streaming)
        self._replace(materialized, target, schema_mode=spec.schema_mode)

    def _do_replace_partitions(
        self,
        frame: InputFrameT,
        target: ResolvedTarget,
        spec: ReplacePartitionsSpec,
        streaming: bool,
    ) -> None:
        """Replace partitions policy: check exists → create OR align → write."""
        existing = self._physical_schema(target)
        if existing is None:
            _ensure_can_create_missing_table(
                target=target,
                schema_mode=spec.schema_mode,
                missing_table_policy=self._missing_table_policy,
            )
            materialized = self._materialize_for_write(frame, streaming)
            self._create(
                materialized,
                target,
                schema_mode=spec.schema_mode,
                partition_cols=spec.partition_cols,
            )
            return
        aligned = self._align(frame, existing, spec.schema_mode)
        materialized = self._materialize_for_write(aligned, streaming)
        self._replace_partitions(
            materialized,
            target,
            partition_cols=spec.partition_cols,
            schema_mode=spec.schema_mode,
        )

    def _do_replace_where(
        self,
        frame: InputFrameT,
        target: ResolvedTarget,
        spec: ReplaceWhereSpec,
        params_instance: Any,
        streaming: bool,
    ) -> None:
        """Replace where policy: check exists → create OR align → write."""
        existing = self._physical_schema(target)
        if existing is None:
            _ensure_can_create_missing_table(
                target=target,
                schema_mode=spec.schema_mode,
                missing_table_policy=self._missing_table_policy,
            )
            materialized = self._materialize_for_write(frame, streaming)
            self._create(materialized, target, schema_mode=spec.schema_mode)
            return
        predicate = self._predicate_to_sql(spec.replace_predicate, params_instance)
        aligned = self._align(frame, existing, spec.schema_mode)
        materialized = self._materialize_for_write(aligned, streaming)
        self._replace_where(
            materialized,
            target,
            predicate=predicate,
            schema_mode=spec.schema_mode,
        )

    def _do_upsert(
        self,
        frame: InputFrameT,
        target: ResolvedTarget,
        spec: UpsertSpec,
        streaming: bool,
    ) -> None:
        """Upsert policy: check exists → create OR align+write."""
        existing = self._physical_schema(target)
        if existing is None:
            _ensure_can_create_missing_table(
                target=target,
                schema_mode=spec.schema_mode,
                missing_table_policy=self._missing_table_policy,
            )
            materialized = self._materialize_for_write(frame, streaming=False)
            self._create(
                materialized,
                target,
                schema_mode=spec.schema_mode,
                partition_cols=spec.partition_cols,
            )
            return
        aligned = self._align(frame, existing, spec.schema_mode)
        materialized = self._materialize_for_write(aligned, streaming=False)
        self._upsert(
            materialized,
            target,
            spec=spec,
            existing_schema=existing,
        )

    def _do_historify(
        self,
        frame: InputFrameT,
        target: ResolvedTarget,
        spec: HistorifySpec,
        params_instance: Any,
    ) -> HistorifyRepairReport | None:
        """SCD Type 2 historify policy: read existing, validate, transform, write.

        Reads existing target data, validates creation rights when absent, then
        delegates to :meth:`_historify` with the existing frame so the backend
        only handles the transform + write — not the read.

        Args:
            frame:           Incoming input frame.
            target:          Resolved Delta target.
            spec:            Compiled historify spec.
            params_instance: Runtime params; forwarded to the engine for
                             ``effective_date`` resolution.

        Returns:
            A :class:`~loom.etl.HistorifyRepairReport` when a re-weave was
            performed, or ``None`` for a normal forward-only run.
        """
        existing = self._read_existing_data(target, frame, spec)
        if existing is None:
            _ensure_can_create_missing_table(
                target=target,
                schema_mode=spec.schema_mode,
                missing_table_policy=self._missing_table_policy,
            )
        materialized = self._materialize_for_write(frame, streaming=False)
        return self._historify(
            materialized, existing, target, spec=spec, params_instance=params_instance
        )

    # ========================================================================
    # Abstract Hooks (backend-specific implementations)
    # ========================================================================

    @abstractmethod
    def _physical_schema(self, target: ResolvedTarget) -> PhysicalSchemaT | None:
        """Read physical schema for target, or None if not exists."""

    @abstractmethod
    def _read_existing_data(
        self,
        target: ResolvedTarget,
        frame: InputFrameT,
        spec: HistorifySpec,
    ) -> WriteFrameT | None:
        """Read existing target data for SCD Type 2, pruned to relevant partitions.

        Called before the incoming frame is fully materialized so that backends
        can extract partition-column values cheaply (e.g. from a LazyFrame) and
        use them to push down a partition filter — reading only the files that
        could be affected by the incoming delta, not the entire history table.

        When ``spec.partition_scope`` is set the implementation SHOULD restrict
        the read to the partitions present in ``frame``.  When it is ``None``
        the full table must be returned.

        Args:
            target: Resolved Delta target.
            frame:  Incoming input frame, not yet fully materialized.  Use it
                    only to extract distinct partition-column values.
            spec:   Compiled historify spec; ``spec.partition_scope`` carries
                    the partition column names.

        Returns:
            Backend frame with the (optionally pruned) current state, or
            ``None`` when the target table does not yet exist.
        """

    @abstractmethod
    def _align(
        self,
        frame: InputFrameT,
        existing_schema: PhysicalSchemaT | None,
        mode: SchemaMode,
    ) -> InputFrameT:
        """Align frame schema with existing."""

    @abstractmethod
    def _materialize_for_write(self, frame: InputFrameT, streaming: bool) -> WriteFrameT:
        """Convert input frame into write-ready frame for backend sinks."""

    @abstractmethod
    def _predicate_to_sql(self, predicate: Any, params: Any) -> str:
        """Convert predicate to SQL string."""

    @abstractmethod
    def _create(
        self,
        frame: WriteFrameT,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
        partition_cols: tuple[str, ...] = (),
    ) -> None:
        """Create new table."""

    @abstractmethod
    def _append(
        self,
        frame: WriteFrameT,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Append to existing table."""

    @abstractmethod
    def _replace(
        self,
        frame: WriteFrameT,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Replace existing table."""

    @abstractmethod
    def _replace_partitions(
        self,
        frame: WriteFrameT,
        target: ResolvedTarget,
        *,
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
    ) -> None:
        """Replace partitions in existing table."""

    @abstractmethod
    def _replace_where(
        self,
        frame: WriteFrameT,
        target: ResolvedTarget,
        *,
        predicate: str,
        schema_mode: SchemaMode,
    ) -> None:
        """Replace where predicate matches."""

    @abstractmethod
    def _upsert(
        self,
        frame: WriteFrameT,
        target: ResolvedTarget,
        *,
        spec: UpsertSpec,
        existing_schema: PhysicalSchemaT,
    ) -> None:
        """Upsert/merge into existing table."""

    @abstractmethod
    def _historify(
        self,
        frame: WriteFrameT,
        existing: WriteFrameT | None,
        target: ResolvedTarget,
        *,
        spec: HistorifySpec,
        params_instance: Any,
    ) -> HistorifyRepairReport | None:
        """Apply SCD Type 2 transform and write result to target.

        Called after :meth:`_read_existing_data` has already fetched the current
        target state.  The implementation must:

        * Run the SCD2 algorithm (via :class:`~loom.etl.backends._historify.HistorifyEngine`).
        * Write the result using the existing write hooks (``_create``,
          ``_replace``, or ``_replace_partitions``).

        Args:
            frame:           Materialised incoming frame (write-ready).
            existing:        Current target frame, or ``None`` for first run.
            target:          Resolved Delta target.
            spec:            Compiled historify spec.
            params_instance: Runtime params; used to resolve
                             ``spec.effective_date`` when it is a
                             :class:`~loom.etl.ParamExpr`.

        Returns:
            A :class:`~loom.etl.HistorifyRepairReport` when re-weave was
            triggered, or ``None`` for a normal forward-only run.

        Raises:
            HistorifyKeyConflictError:     Duplicate entity state vectors.
            HistorifyDateCollisionError:   Same-date ties in LOG mode.
            HistorifyTemporalConflictError: Future-open records detected.
        """

    @abstractmethod
    def _write_file(
        self,
        frame: InputFrameT,
        spec: FileSpec,
        *,
        streaming: bool,
    ) -> None:
        """Write to file (CSV, JSON, Parquet)."""


__all__ = ["_WritePolicy"]
