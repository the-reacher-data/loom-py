"""TDD Red — tests for FromClickHouse builder and ClickHouseSourceSpec.

All tests in this file MUST FAIL with ImportError / ModuleNotFoundError until
loom/etl/io/sources/_clickhouse.py is implemented.
"""

from __future__ import annotations

import pytest

from loom.etl.compiler._errors import ETLCompilationError
from loom.etl.declarative.source._specs import SourceKind
from loom.etl.io.sources._clickhouse import ClickHouseSourceSpec, FromClickHouse  # noqa: F401

# A minimal predicate stand-in — just a truthy object that can be stored in a
# tuple, equivalent to how the existing FromTable tests pass col() expressions.
_PRED = object()


class TestToSpecRequiresPredicateOrUnbounded:
    def test_to_spec_requires_predicate_or_unbounded(self) -> None:
        """FromClickHouse without .where() or .unbounded() must raise ETLCompilationError."""
        builder = FromClickHouse("cdc_events")
        with pytest.raises(ETLCompilationError):
            builder._to_spec("cdc")


class TestToSpecWithWhere:
    def test_to_spec_with_where_produces_spec(self) -> None:
        """Calling .where(pred) yields a ClickHouseSourceSpec with the right fields."""
        builder = FromClickHouse("cdc_events").where(_PRED)
        spec = builder._to_spec("cdc")

        assert isinstance(spec, ClickHouseSourceSpec)
        assert spec.alias == "cdc"
        assert spec.table == "cdc_events"
        assert len(spec.predicates) == 1
        assert spec.predicates[0] is _PRED


class TestToSpecUnbounded:
    def test_to_spec_unbounded_allows_no_predicate(self) -> None:
        """.unbounded() with no .where() must NOT raise ETLCompilationError."""
        builder = FromClickHouse("cdc_events").unbounded()
        # Should not raise
        spec = builder._to_spec("cdc")
        assert isinstance(spec, ClickHouseSourceSpec)


class TestSelectColumns:
    def test_select_columns_stored_in_spec(self) -> None:
        """Selected columns are stored as a tuple on the spec."""
        builder = FromClickHouse("cdc_events").where(_PRED).select(["a", "b"])
        spec = builder._to_spec("cdc")

        assert spec.columns == ("a", "b")


class TestDistinctFlag:
    def test_distinct_flag_stored_in_spec(self) -> None:
        """Calling .distinct() sets spec.distinct to True."""
        builder = FromClickHouse("cdc_events").where(_PRED).distinct()
        spec = builder._to_spec("cdc")

        assert spec.distinct is True

    def test_distinct_defaults_to_false(self) -> None:
        """Without .distinct(), spec.distinct is False."""
        spec = FromClickHouse("cdc_events").where(_PRED)._to_spec("cdc")
        assert spec.distinct is False


class TestSpecKind:
    def test_spec_kind_is_clickhouse(self) -> None:
        """spec.kind must equal SourceKind.CLICKHOUSE."""
        spec = FromClickHouse("cdc_events").where(_PRED)._to_spec("cdc")
        assert spec.kind == SourceKind.CLICKHOUSE


class TestRepr:
    def test_repr_includes_table(self) -> None:
        """repr(FromClickHouse('cdc_events')) must contain the table name."""
        assert "cdc_events" in repr(FromClickHouse("cdc_events"))
