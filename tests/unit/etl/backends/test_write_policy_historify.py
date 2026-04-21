"""Unit tests for _WritePolicy dispatch — HistorifySpec routing.

Verifies that:
* HistorifySpec is routed to _do_historify (not any other handler).
* _do_historify validates missing-table policy before delegating.
* _do_historify materializes the frame and delegates to _historify.
* Temp specs still raise TypeError (unchanged).
* Unsupported specs still raise TypeError (unchanged).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from loom.etl.backends._write_policy import _WritePolicy
from loom.etl.declarative.expr._params import params
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target._history import (
    DeletePolicy,
    HistorifyInputMode,
    HistorifyRepairReport,
    HistorifySpec,
)
from loom.etl.declarative.target._schema_mode import SchemaMode
from loom.etl.declarative.target._table import AppendSpec
from loom.etl.declarative.target._temp import TempSpec
from loom.etl.schema._schema import SchemaNotFoundError
from loom.etl.storage._config import MissingTablePolicy
from loom.etl.storage.routing import ResolvedTarget

# ---------------------------------------------------------------------------
# Minimal concrete _WritePolicy for testing dispatch
# ---------------------------------------------------------------------------


class _StubWritePolicy(_WritePolicy[list[int], list[int], dict[str, Any]]):
    """Minimal concrete subclass for testing dispatch logic only."""

    def __init__(
        self,
        *,
        schema_exists: bool = True,
        missing_table_policy: MissingTablePolicy = MissingTablePolicy.SCHEMA_MODE,
    ) -> None:
        resolver = MagicMock()
        resolver.resolve.return_value = MagicMock(spec=ResolvedTarget)
        super().__init__(resolver=resolver, missing_table_policy=missing_table_policy)
        self._schema_exists = schema_exists
        self.historify_calls: list[tuple[Any, ...]] = []
        self.create_calls: list[tuple[Any, ...]] = []

    def _physical_schema(self, target: ResolvedTarget) -> dict[str, Any] | None:
        return {"col": "type"} if self._schema_exists else None

    def _align(
        self, frame: list[int], existing: dict[str, Any] | None, mode: SchemaMode
    ) -> list[int]:
        return frame

    def _materialize_for_write(self, frame: list[int], streaming: bool) -> list[int]:
        return list(frame)  # return a copy so we can track it

    def _predicate_to_sql(self, predicate: Any, params: Any) -> str:
        return ""

    def _create(
        self,
        frame: list[int],
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
        partition_cols: tuple[str, ...] = (),
    ) -> None:
        self.create_calls.append((frame, schema_mode, partition_cols))

    def _append(self, frame: list[int], target: ResolvedTarget, *, schema_mode: SchemaMode) -> None:
        pass  # Stub — not used in historify dispatch tests.

    def _replace(
        self, frame: list[int], target: ResolvedTarget, *, schema_mode: SchemaMode
    ) -> None:
        pass  # Stub — not used in historify dispatch tests.

    def _replace_partitions(
        self,
        frame: list[int],
        target: ResolvedTarget,
        *,
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
    ) -> None:
        pass  # Stub — not used in historify dispatch tests.

    def _replace_where(
        self,
        frame: list[int],
        target: ResolvedTarget,
        *,
        predicate: str,
        schema_mode: SchemaMode,
    ) -> None:
        pass  # Stub — not used in historify dispatch tests.

    def _upsert(
        self,
        frame: list[int],
        target: ResolvedTarget,
        *,
        spec: Any,
        existing_schema: dict[str, Any],
    ) -> None:
        pass  # Stub — not used in historify dispatch tests.

    def _read_existing_data(
        self,
        target: ResolvedTarget,
        frame: list[int],
        spec: HistorifySpec,
    ) -> list[int] | None:
        return [0] if self._schema_exists else None

    def _historify(
        self,
        frame: list[int],
        existing: list[int] | None,
        target: ResolvedTarget,
        *,
        spec: HistorifySpec,
        params_instance: Any,
    ) -> HistorifyRepairReport | None:
        self.historify_calls.append((frame, spec, params_instance))
        return None

    def _write_file(self, frame: list[int], spec: Any, *, streaming: bool) -> None:
        pass  # Stub — not used in historify dispatch tests.


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _spec(
    effective_date: Any = params.run_date,
    schema_mode: SchemaMode = SchemaMode.STRICT,
    partition_scope: tuple[str, ...] | None = None,
) -> HistorifySpec:
    return HistorifySpec(
        table_ref=TableRef("wh.dim_players"),
        keys=("player_id",),
        effective_date=effective_date,
        mode=HistorifyInputMode.SNAPSHOT,
        track=("team_id",),
        delete_policy=DeletePolicy.CLOSE,
        partition_scope=partition_scope,
        schema_mode=schema_mode,
    )


# ---------------------------------------------------------------------------
# Dispatch routing
# ---------------------------------------------------------------------------


class TestHistorifyDispatch:
    def test_historify_spec_routes_to_do_historify(self) -> None:
        writer = _StubWritePolicy(schema_exists=True)
        writer.write([1, 2, 3], _spec(), object())
        assert len(writer.historify_calls) == 1

    def test_historify_receives_materialized_frame(self) -> None:
        writer = _StubWritePolicy(schema_exists=True)
        frame = [10, 20]
        writer.write(frame, _spec(), None)
        received_frame, _, _ = writer.historify_calls[0]
        assert received_frame == [10, 20]

    def test_historify_receives_spec(self) -> None:
        spec = _spec()
        writer = _StubWritePolicy(schema_exists=True)
        writer.write([1], spec, None)
        _, received_spec, _ = writer.historify_calls[0]
        assert received_spec is spec

    def test_historify_receives_params_instance(self) -> None:
        params_obj = object()
        writer = _StubWritePolicy(schema_exists=True)
        writer.write([1], _spec(), params_obj)
        _, _, received_params = writer.historify_calls[0]
        assert received_params is params_obj

    def test_non_historify_spec_not_routed_to_historify(self) -> None:
        writer = _StubWritePolicy(schema_exists=True)
        # AppendSpec hits _do_append, not _do_historify
        append = AppendSpec(table_ref=TableRef("t.out"))
        writer.write([1], append, None)
        assert writer.historify_calls == []


# ---------------------------------------------------------------------------
# Missing-table policy enforcement
# ---------------------------------------------------------------------------


class TestHistorifyMissingTablePolicy:
    def test_missing_table_with_overwrite_schema_allows_creation(self) -> None:
        writer = _StubWritePolicy(
            schema_exists=False,
            missing_table_policy=MissingTablePolicy.SCHEMA_MODE,
        )
        spec = _spec(schema_mode=SchemaMode.OVERWRITE)
        writer.write([1], spec, None)
        # No error raised — _historify is called
        assert len(writer.historify_calls) == 1

    def test_missing_table_with_create_policy_allows_creation(self) -> None:
        writer = _StubWritePolicy(
            schema_exists=False,
            missing_table_policy=MissingTablePolicy.CREATE,
        )
        writer.write([1], _spec(), None)
        assert len(writer.historify_calls) == 1

    def test_missing_table_with_schema_mode_strict_raises(self) -> None:
        writer = _StubWritePolicy(
            schema_exists=False,
            missing_table_policy=MissingTablePolicy.SCHEMA_MODE,
        )
        spec = _spec(schema_mode=SchemaMode.STRICT)
        with pytest.raises(SchemaNotFoundError):
            writer.write([1], spec, None)

    def test_existing_table_always_delegates_to_historify(self) -> None:
        writer = _StubWritePolicy(schema_exists=True)
        writer.write([1], _spec(), None)
        assert len(writer.historify_calls) == 1


# ---------------------------------------------------------------------------
# Temp targets still raise TypeError
# ---------------------------------------------------------------------------


class TestHistorifyDoesNotAffectTempDispatch:
    def test_temp_spec_still_raises_type_error(self) -> None:
        from loom.etl.checkpoint import CheckpointScope

        writer = _StubWritePolicy()
        temp = TempSpec(temp_name="x", temp_scope=CheckpointScope.RUN)
        with pytest.raises(TypeError, match="temp targets"):
            writer.write([1], temp, None)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# _do_historify return value (repair report passthrough)
# ---------------------------------------------------------------------------


class TestHistorifyRepairReportPassthrough:
    def test_none_returned_when_historify_returns_none(self) -> None:
        writer = _StubWritePolicy(schema_exists=True)
        result = writer._do_historify([1], writer._resolver.resolve(None), _spec(), None)
        assert result is None

    def test_repair_report_returned_when_historify_returns_one(self) -> None:
        report = HistorifyRepairReport(
            affected_keys=frozenset({("player_id", 42)}),
            dates_requiring_rerun=("2025-01-01",),
            warnings=("rewove 1 record",),
        )

        class _ReportingWriter(_StubWritePolicy):
            def _historify(
                self, frame: Any, existing: Any, target: Any, *, spec: Any, params_instance: Any
            ) -> HistorifyRepairReport | None:
                return report

        writer = _ReportingWriter(schema_exists=True)
        result = writer._do_historify([1], writer._resolver.resolve(None), _spec(), None)
        assert result is report
