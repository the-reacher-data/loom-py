"""SCD Type 2 history target builder."""

from __future__ import annotations

from typing import Literal

from loom.etl.declarative.expr._params import ParamExpr
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target._history._enums import (
    DeletePolicy,
    HistorifyInputMode,
    HistoryDateType,
)
from loom.etl.declarative.target._history._spec import HistorifySpec
from loom.etl.declarative.target._schema_mode import SchemaMode


class IntoHistory:
    """Declare a Delta table as the ETL step SCD Type 2 history target.

    Each run compares incoming data against the current open vectors in the target
    table. Only changed entity states generate new rows — unchanged entities
    produce no writes.

    **Column roles:**

    * ``keys`` — entity identity. Never change; they are the MERGE join key.
    * ``track`` — change-triggering columns. A value change inserts a new row
      and closes the previous open vector. ``None`` means every non-key column
      is tracked.
    * ``overwrite`` — columns updated in-place on the open row when the entity is
      UNCHANGED. No new history row is created; the current open row is silently
      refreshed. Useful for mutable metadata that should not drive history.
    * *Remaining* — "passive" columns: carried forward into each new row but
      never updated nor tracked.

    **Multiple simultaneous open vectors** are natively supported. Include the
    distinguishing dimension in ``track``. For example, a player on loan to two
    clubs simultaneously uses ``track=(\"team_id\", \"role\")`` — the vectors
    ``(player_id=P1, team_id=RM, role=OWNER)`` and
    ``(player_id=P1, team_id=GET, role=LOAN)`` are independent and coexist
    without conflict.

    Args:
        ref: Logical table reference — ``str`` or :class:`~loom.etl.TableRef`.
        keys: One or more column names that identify the entity.
            Must be non-empty and must not overlap with ``track``.
        effective_date: In ``\"log\"`` mode: name of the frame column carrying
            the event date/timestamp. In ``\"snapshot\"`` mode: a
            :class:`~loom.etl.ParamExpr` or column name resolved from run params.
        mode: ``\"snapshot\"`` (default) or ``\"log\"``.
        track: Columns whose changes trigger a new history row.
            ``None`` means all non-key columns are tracked.
        overwrite: Columns to update in-place on the open row when unchanged.
            Must not overlap with ``keys`` or ``track``.
        delete_policy: Action for absent keys in SNAPSHOT mode.
            Defaults to ``\"close\"``.
        partition_scope: Partition columns to constrain Delta reads/writes.
            Strongly recommended for large tables.
        valid_from: Name of the period-start column in the Delta table.
            Defaults to ``\"valid_from\"``.
        valid_to: Name of the period-end column in the Delta table.
            Defaults to ``\"valid_to\"``.
        date_type: Precision for boundary columns. Defaults to ``\"date\"``.
        schema: Schema evolution strategy.
            Defaults to :attr:`~SchemaMode.STRICT`.
        allow_temporal_rerun: Allow re-weave when past-date corrections are loaded.
            Defaults to ``False``.

    Raises:
        ValueError: If ``keys`` is empty.
        ValueError: If ``track`` overlaps with ``keys``.
        ValueError: If ``overwrite`` overlaps with ``keys``, ``track``, or boundary columns.
        ValueError: If ``valid_from`` and ``valid_to`` share the same name.

    Example::

        target = IntoHistory(
            "warehouse.dim_players",
            keys=("player_id",),
            track=("team_id", "contract_value"),
            effective_date=params.run_date,
            mode="snapshot",
            delete_policy="close",
            partition_scope=("season",),
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
        overwrite: tuple[str, ...] | list[str] | None = None,
        delete_policy: Literal["ignore", "close", "soft_delete"] = "close",
        partition_scope: tuple[str, ...] | list[str] | None = None,
        valid_from: str = "valid_from",
        valid_to: str = "valid_to",
        deleted_at: str = "deleted_at",
        date_type: Literal["date", "timestamp"] = "date",
        schema: SchemaMode = SchemaMode.STRICT,
        allow_temporal_rerun: bool = False,
    ) -> None:
        keys_t = tuple(keys)
        track_t: tuple[str, ...] | None = tuple(track) if track is not None else None
        overwrite_t: tuple[str, ...] | None = tuple(overwrite) if overwrite is not None else None

        _validate_history_args(keys_t, track_t, overwrite_t, valid_from, valid_to, deleted_at)
        _validate_log_effective_date(effective_date, valid_from, valid_to, mode)

        table_ref = TableRef(ref) if isinstance(ref, str) else ref
        self._spec = HistorifySpec(
            table_ref=table_ref,
            keys=keys_t,
            effective_date=effective_date,
            mode=HistorifyInputMode(mode),
            track=track_t,
            overwrite=overwrite_t,
            delete_policy=DeletePolicy(delete_policy),
            partition_scope=tuple(partition_scope) if partition_scope is not None else None,
            valid_from=valid_from,
            valid_to=valid_to,
            deleted_at=deleted_at,
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


def _validate_history_args(
    keys: tuple[str, ...],
    track: tuple[str, ...] | None,
    overwrite: tuple[str, ...] | None,
    valid_from: str,
    valid_to: str,
    deleted_at: str,
) -> None:
    """Validate ``IntoHistory`` constructor arguments.

    Args:
        keys: Entity identity columns.
        track: Change-triggering columns, or ``None``.
        overwrite: In-place overwrite columns, or ``None``.
        valid_from: Period-start column name.
        valid_to: Period-end column name.
        deleted_at: Soft-delete audit column name.

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

    if overwrite is not None:
        overlap_keys = set(keys) & set(overwrite)
        if overlap_keys:
            raise ValueError(
                f"IntoHistory: 'overwrite' cannot overlap with 'keys'. "
                f"Overlap: {sorted(overlap_keys)}"
            )
        if track is not None:
            overlap_track = set(track) & set(overwrite)
            if overlap_track:
                raise ValueError(
                    f"IntoHistory: 'overwrite' cannot overlap with 'track'. "
                    f"Overlap: {sorted(overlap_track)}"
                )
        boundary_overlap = {valid_from, valid_to, deleted_at} & set(overwrite)
        if boundary_overlap:
            raise ValueError(
                f"IntoHistory: 'overwrite' cannot contain boundary columns. "
                f"Overlap: {sorted(boundary_overlap)}"
            )

    boundary_names = {valid_from, valid_to, deleted_at}
    if len(boundary_names) < 3:
        raise ValueError(
            f"IntoHistory: 'valid_from', 'valid_to' and 'deleted_at' must be distinct. "
            f"Got valid_from={valid_from!r}, valid_to={valid_to!r}, deleted_at={deleted_at!r}."
        )


def _validate_log_effective_date(
    effective_date: str | object,
    valid_from: str,
    valid_to: str,
    mode: str,
) -> None:
    """Raise if LOG mode effective_date clashes with boundary column names.

    In LOG mode the engine drops the effective_date column after computing
    valid_from / valid_to. If effective_date equals valid_from the drop would
    silently erase the just-created column.

    Args:
        effective_date: Column name or ParamExpr declared as the event date.
        valid_from: Period-start column name.
        valid_to: Period-end column name.
        mode: ``\"log\"`` or ``\"snapshot\"``.

    Raises:
        ValueError: When LOG mode effective_date conflicts with a boundary col.
    """
    if mode != "log":
        return
    eff_col = str(effective_date)
    if eff_col in (valid_from, valid_to):
        raise ValueError(
            f"IntoHistory (LOG mode): effective_date={eff_col!r} must not match "
            f"valid_from={valid_from!r} or valid_to={valid_to!r}. "
            "The engine drops the effective_date column after building history boundaries; "
            "using the same name would silently erase the computed column."
        )


__all__ = ["IntoHistory"]
