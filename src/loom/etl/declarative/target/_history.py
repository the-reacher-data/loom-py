"""SCD Type 2 (historify) target — builder, spec, engine protocol, and error types.

Public API — import from :mod:`loom.etl.declarative.target` or :mod:`loom.etl`.

:class:`IntoHistory` declares a Delta table as a Slowly Changing Dimension
Type 2 target.  Each change in ``track`` columns produces a new immutable history
row; unchanged entities accumulate no new data.

Multiple simultaneously open vectors for the same entity key are natively
supported when the distinguishing dimension is included in ``track`` — for
example, a player on loan (``team_id`` + ``role`` in ``track``) will have two
independent history vectors while both relationships are active.

Example::

    from loom.etl import IntoHistory, params

    class DimPlayers(ETLStep[DailyParams]):
        players = FromTable("raw.players")
        target = IntoHistory(
            "warehouse.dim_players",
            keys=("player_id",),
            track=("team_id", "contract_value"),
            effective_date=params.run_date,
            mode="snapshot",
            delete_policy="close",
            partition_scope=("season",),
        )

        def execute(self, p: DailyParams, *, players: pl.LazyFrame) -> pl.LazyFrame:
            return players.select("player_id", "team_id", "contract_value", "season")
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Literal, Protocol, TypeVar, runtime_checkable

from loom.etl.declarative.expr._params import ParamExpr
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target._schema_mode import SchemaMode

_FrameT = TypeVar("_FrameT")


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class HistorifyInputMode(StrEnum):
    """Input semantics for the SCD2 writer.

    Values:

    * ``SNAPSHOT`` — the frame is a **full snapshot** of the entity dimension.
      Keys absent from the snapshot are handled according to ``delete_policy``.
    * ``LOG`` — the frame carries individual **change events**; each row has an
      ``effective_date`` column that determines when the change took effect.
    """

    SNAPSHOT = "snapshot"
    LOG = "log"


class DeletePolicy(StrEnum):
    """Action applied to entity keys absent from an incoming snapshot.

    Only meaningful when ``mode=SNAPSHOT``; LOG mode ignores this setting.

    Values:

    * ``IGNORE``      — leave open vectors open.  Use for partial or incremental
                        snapshots where absence does not imply deletion.
    * ``CLOSE``       — close the open vector by setting
                        ``valid_to = effective_date - 1``.  Standard SCD2
                        behavior for full-dimension snapshots.
    * ``SOFT_DELETE`` — close the open vector and stamp a ``deleted_at`` column.
                        Use when downstream systems require explicit deletion
                        audit trails.
    """

    IGNORE = "ignore"
    CLOSE = "close"
    SOFT_DELETE = "soft_delete"


class HistoryDateType(StrEnum):
    """Column type used for ``valid_from`` / ``valid_to`` boundary columns.

    Values:

    * ``DATE``      — calendar date precision.  Suitable for daily SCD2 pipelines.
    * ``TIMESTAMP`` — microsecond precision.  Use for event-driven or sub-daily
                      update patterns.
    """

    DATE = "date"
    TIMESTAMP = "timestamp"


# ---------------------------------------------------------------------------
# Runtime error types
# ---------------------------------------------------------------------------


class HistorifyKeyConflictError(ValueError):
    """Raised when the incoming snapshot contains duplicate ``keys + track`` combinations.

    SCD2 semantics require that each entity state vector is unique within a
    snapshot.  Duplicates indicate a data-modelling error: the distinguishing
    dimension must be included in ``keys`` or ``track``.

    If the same entity legitimately belongs to multiple simultaneous states
    (e.g. a player on loan to two clubs at once), include the distinguishing
    column in ``track`` — both vectors will then be tracked independently.

    Args:
        duplicates: Human-readable representation of the conflicting key tuples.
    """

    def __init__(self, duplicates: str) -> None:
        super().__init__(
            f"IntoHistory: duplicate entity state vectors found in snapshot — "
            f"each (keys + track) combination must be unique. Conflicting rows: {duplicates}. "
            f"If the same entity legitimately occupies multiple simultaneous states "
            f"(e.g. a player on loan), include the distinguishing column in 'track'."
        )


class HistorifyDateCollisionError(ValueError):
    """Raised in LOG mode when two events share the same ``(keys + track, effective_date)``.

    At DATE granularity, two events on the same calendar day for the same entity
    state vector are ambiguous — relative ordering is lost and idempotency
    cannot be guaranteed.

    To resolve: switch to ``date_type="timestamp"`` for sub-daily precision, or
    deduplicate events upstream before loading.

    Args:
        collisions: Human-readable description of the colliding key/date pairs.
    """

    def __init__(self, collisions: str) -> None:
        super().__init__(
            f"IntoHistory (LOG mode): same-date collisions detected — two events share "
            f"the same (keys + track, effective_date) value at DATE granularity. "
            f"Use date_type='timestamp' for sub-daily precision, or deduplicate upstream. "
            f"Collisions: {collisions}"
        )


class HistorifyTemporalConflictError(ValueError):
    """Raised when the target contains future-open vectors and re-weave is disabled.

    A temporal conflict occurs when ``valid_from > effective_date``, meaning the
    incoming data must be inserted *before* an already-committed record.  This
    requires explicit opt-in via ``allow_temporal_rerun=True``.

    Args:
        min_conflict_date: Earliest ``valid_from`` date found in the conflict.
        effective_date:    The ``effective_date`` of the current run.
    """

    def __init__(self, min_conflict_date: object, effective_date: object) -> None:
        super().__init__(
            f"IntoHistory: temporal conflict detected — the target table contains records "
            f"with valid_from={min_conflict_date!r} after the current "
            f"effective_date={effective_date!r}. "
            f"Set allow_temporal_rerun=True to enable re-weave of historical data."
        )


# ---------------------------------------------------------------------------
# Repair report
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HistorifyRepairReport:
    """Structured report produced by a successful re-weave operation.

    Returned by the backend engine when ``allow_temporal_rerun=True`` and past-date
    data was corrected.  Consumers may use this to schedule downstream re-runs
    for the affected date range.

    Attributes:
        affected_keys:         Frozenset of entity key tuples that were modified.
        dates_requiring_rerun: Sorted tuple of dates where history was repaired.
        warnings:              Human-readable description of each repair action.
    """

    affected_keys: frozenset[tuple[object, ...]]
    dates_requiring_rerun: tuple[object, ...]
    warnings: tuple[str, ...]


# ---------------------------------------------------------------------------
# Engine protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class HistorifyEngine(Protocol[_FrameT]):  # type: ignore[misc]
    """Protocol implemented by backend-specific SCD2 engines.

    Backend implementations (Polars, Spark) inject this protocol and are resolved
    at runtime by the target writer.  The domain layer depends only on this
    protocol — never on concrete backend types.

    Type parameter ``_FrameT`` is bound to the backend's frame type
    (``pl.LazyFrame`` for Polars, ``pyspark.sql.DataFrame`` for Spark).

    Example::

        class PolarsHistorifyEngine:
            def apply(
                self,
                frame: pl.LazyFrame,
                spec: HistorifySpec,
                params: object,
            ) -> HistorifyRepairReport | None:
                ...
    """

    def apply(
        self,
        frame: _FrameT,
        spec: HistorifySpec,
        params: object,
    ) -> HistorifyRepairReport | None:
        """Apply SCD2 logic to ``frame`` and commit to the Delta target.

        Args:
            frame:  Incoming data frame — Polars ``LazyFrame`` or Spark ``DataFrame``.
            spec:   Compiled :class:`HistorifySpec` carrying all configuration.
            params: Runtime params object; used to resolve :class:`~loom.etl.ParamExpr`
                    values (e.g. ``effective_date``).

        Returns:
            A :class:`HistorifyRepairReport` when re-weave was performed, or
            ``None`` for a normal forward-only run.

        Raises:
            HistorifyKeyConflictError:     Duplicate entity state vectors in snapshot.
            HistorifyDateCollisionError:   Same-date collisions in LOG mode.
            HistorifyTemporalConflictError: Future-open records detected and re-weave
                                           is disabled.
        """
        ...


# ---------------------------------------------------------------------------
# Compiled spec
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HistorifySpec:
    """Compiled SCD Type 2 write spec.  Produced by :meth:`IntoHistory._to_spec`.

    This frozen dataclass is the sole input to the backend engine.  All
    configuration is resolved at declaration time — no runtime reflection occurs.

    Both ``table_ref`` and ``schema_mode`` are required for duck-type detection
    by the write-policy dispatcher (``_is_table_target_spec``).

    Args:
        table_ref:            Logical Delta table reference.
        keys:                 Columns that uniquely identify the entity
                              (e.g. ``("player_id",)``).
        effective_date:       Column name in LOG mode, or a
                              :class:`~loom.etl.ParamExpr` in SNAPSHOT mode.
        mode:                 Input semantics — ``SNAPSHOT`` or ``LOG``.
        track:                Columns whose value change triggers a new history
                              row.  ``None`` means every non-key column is tracked.
        delete_policy:        Action for absent keys in SNAPSHOT mode.
                              Ignored in LOG mode.
        partition_scope:      Partition columns used to limit Delta reads/writes.
                              Strongly recommended for large tables.
        valid_from:           Name of the period-start boundary column.
        valid_to:             Name of the period-end boundary column.
                              ``NULL`` in the table means the vector is currently open.
        date_type:            Precision of the ``valid_from`` / ``valid_to`` columns.
        schema_mode:          Schema evolution strategy.
        allow_temporal_rerun: Allow re-weave when past-date data is loaded.
    """

    table_ref: TableRef
    keys: tuple[str, ...]
    effective_date: str | ParamExpr
    mode: HistorifyInputMode
    track: tuple[str, ...] | None = None
    delete_policy: DeletePolicy = DeletePolicy.CLOSE
    partition_scope: tuple[str, ...] | None = None
    valid_from: str = "valid_from"
    valid_to: str = "valid_to"
    date_type: HistoryDateType = HistoryDateType.DATE
    schema_mode: SchemaMode = SchemaMode.STRICT
    allow_temporal_rerun: bool = False


# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------


class IntoHistory:
    """Declare a Delta table as the ETL step SCD Type 2 history target.

    Each run compares incoming data against the current open vectors in the target
    table.  Only changed entity states generate new rows — unchanged entities
    produce no writes.

    **Column roles:**

    * ``keys``  — entity identity.  Never change; they are the MERGE join key.
    * ``track`` — change-triggering columns.  A value change inserts a new row
                  and closes the previous open vector.  ``None`` means every
                  non-key column is tracked.
    * *Remaining* — "passive" columns: carried forward into each new row but
                    never trigger history generation on their own.

    **Multiple simultaneous open vectors** are natively supported.  Include the
    distinguishing dimension in ``track``.  For example, a player on loan to two
    clubs simultaneously uses ``track=("team_id", "role")`` — the vectors
    ``(player_id=P1, team_id=RM, role=OWNER)`` and
    ``(player_id=P1, team_id=GET, role=LOAN)`` are independent and coexist
    without conflict.

    Args:
        ref:                  Logical table reference — ``str`` or
                              :class:`~loom.etl.TableRef`.
        keys:                 One or more column names that identify the entity.
                              Must be non-empty and must not overlap with ``track``.
        effective_date:       In ``"log"`` mode: name of the frame column carrying
                              the event date/timestamp.  In ``"snapshot"`` mode:
                              a :class:`~loom.etl.ParamExpr` or column name
                              resolved from run params.
        mode:                 ``"snapshot"`` (default) or ``"log"``.
        track:                Columns whose changes trigger a new history row.
                              ``None`` means all non-key columns are tracked.
        delete_policy:        Action for absent keys in SNAPSHOT mode.
                              Defaults to ``"close"``.
        partition_scope:      Partition columns to constrain Delta reads/writes.
                              Strongly recommended for large tables.
        valid_from:           Name of the period-start column in the Delta table.
                              Defaults to ``"valid_from"``.
        valid_to:             Name of the period-end column in the Delta table.
                              Defaults to ``"valid_to"``.
        date_type:            Precision for boundary columns.  Defaults to ``"date"``.
        schema:               Schema evolution strategy.
                              Defaults to :attr:`~SchemaMode.STRICT`.
        allow_temporal_rerun: Allow re-weave when past-date corrections are loaded.
                              Defaults to ``False``.

    Raises:
        ValueError: If ``keys`` is empty.
        ValueError: If ``track`` overlaps with ``keys``.
        ValueError: If ``valid_from`` and ``valid_to`` share the same name.

    Example::

        # Standard player dimension — full daily snapshot
        target = IntoHistory(
            "warehouse.dim_players",
            keys=("player_id",),
            track=("team_id", "contract_value"),
            effective_date=params.run_date,
            mode="snapshot",
            delete_policy="close",
            partition_scope=("season",),
        )

        # Player loan — multiple simultaneous open vectors per entity
        target = IntoHistory(
            "warehouse.dim_player_contracts",
            keys=("player_id",),
            track=("team_id", "role"),
            effective_date=params.run_date,
            mode="snapshot",
        )

        # Subscription plan — event log with sub-day precision
        target = IntoHistory(
            "warehouse.dim_subscriptions",
            keys=("subscription_id",),
            track=("plan", "price_eur"),
            effective_date="event_ts",
            mode="log",
            date_type="timestamp",
        )
    """

    __slots__ = ("_spec",)

    def __init__(
        self,
        ref: str | TableRef,
        *,
        keys: tuple[str, ...] | list[str],
        effective_date: str | ParamExpr,
        mode: Literal["snapshot", "log"] = "snapshot",
        track: tuple[str, ...] | list[str] | None = None,
        delete_policy: Literal["ignore", "close", "soft_delete"] = "close",
        partition_scope: tuple[str, ...] | list[str] | None = None,
        valid_from: str = "valid_from",
        valid_to: str = "valid_to",
        date_type: Literal["date", "timestamp"] = "date",
        schema: SchemaMode = SchemaMode.STRICT,
        allow_temporal_rerun: bool = False,
    ) -> None:
        keys_t = tuple(keys)
        track_t: tuple[str, ...] | None = tuple(track) if track is not None else None

        _validate_history_args(keys_t, track_t, valid_from, valid_to)

        table_ref = TableRef(ref) if isinstance(ref, str) else ref
        self._spec = HistorifySpec(
            table_ref=table_ref,
            keys=keys_t,
            effective_date=effective_date,
            mode=HistorifyInputMode(mode),
            track=track_t,
            delete_policy=DeletePolicy(delete_policy),
            partition_scope=tuple(partition_scope) if partition_scope is not None else None,
            valid_from=valid_from,
            valid_to=valid_to,
            date_type=HistoryDateType(date_type),
            schema_mode=schema,
            allow_temporal_rerun=allow_temporal_rerun,
        )

    def _to_spec(self) -> HistorifySpec:
        """Return the compiled :class:`HistorifySpec`.

        Returns:
            Frozen :class:`HistorifySpec` with all declared configuration.
        """
        return self._spec

    def __repr__(self) -> str:
        spec = self._spec
        return f"IntoHistory({spec.table_ref.ref!r}, keys={spec.keys!r}, mode={spec.mode!r})"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _validate_history_args(
    keys: tuple[str, ...],
    track: tuple[str, ...] | None,
    valid_from: str,
    valid_to: str,
) -> None:
    """Validate ``IntoHistory`` constructor arguments.

    Args:
        keys:       Entity identity columns.
        track:      Change-triggering columns, or ``None``.
        valid_from: Period-start column name.
        valid_to:   Period-end column name.

    Raises:
        ValueError: On any violated constraint.
    """
    if not keys:
        raise ValueError("IntoHistory: 'keys' must contain at least one column name.")

    if track is not None:
        overlap = set(keys) & set(track)
        if overlap:
            raise ValueError(
                f"IntoHistory: columns cannot appear in both 'keys' and 'track'. "
                f"Overlap: {sorted(overlap)}"
            )

    if valid_from == valid_to:
        raise ValueError(
            f"IntoHistory: 'valid_from' and 'valid_to' must be different column names, "
            f"got {valid_from!r} for both."
        )


__all__ = [
    # enums
    "HistorifyInputMode",
    "DeletePolicy",
    "HistoryDateType",
    # spec
    "HistorifySpec",
    # protocol
    "HistorifyEngine",
    # report
    "HistorifyRepairReport",
    # errors
    "HistorifyKeyConflictError",
    "HistorifyDateCollisionError",
    "HistorifyTemporalConflictError",
    # builder
    "IntoHistory",
]
