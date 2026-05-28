"""TDD Red — tests for IntoClickHouse builder and ClickHouseTableSpec.

All tests in this file MUST FAIL with ImportError / ModuleNotFoundError until
loom/etl/io/targets/_clickhouse.py is implemented.
"""

from __future__ import annotations

from loom.etl.io.targets._clickhouse import ClickHouseTableSpec, IntoClickHouse  # noqa: F401


class TestReplaceModeStoredInSpec:
    def test_replace_mode_stored_in_spec(self) -> None:
        """.replace() sets spec.write_mode to 'replace'."""
        spec = IntoClickHouse("motos").replace()._to_spec()
        assert isinstance(spec, ClickHouseTableSpec)
        assert spec.write_mode == "replace"


class TestAppendModeStoredInSpec:
    def test_append_mode_stored_in_spec(self) -> None:
        """.append() sets spec.write_mode to 'append'."""
        spec = IntoClickHouse("motos").append()._to_spec()
        assert spec.write_mode == "append"


class TestDefaultIsAppend:
    def test_default_is_append(self) -> None:
        """IntoClickHouse('t') without an explicit mode defaults to 'append'."""
        spec = IntoClickHouse("t")._to_spec()
        assert spec.write_mode == "append"


class TestTableStoredInSpec:
    def test_table_stored_in_spec(self) -> None:
        """spec.table matches the table name passed to IntoClickHouse."""
        spec = IntoClickHouse("motos")._to_spec()
        assert spec.table == "motos"


class TestSpecKind:
    def test_spec_kind_is_clickhouse_target(self) -> None:
        """spec.kind must equal 'clickhouse'."""
        spec = IntoClickHouse("motos")._to_spec()
        assert spec.kind == "clickhouse"


class TestDatabaseOverride:
    def test_database_override(self) -> None:
        """IntoClickHouse('t', database='mydb') stores spec.database == 'mydb'."""
        spec = IntoClickHouse("t", database="mydb")._to_spec()
        assert spec.database == "mydb"

    def test_database_defaults_to_none(self) -> None:
        """Without database= kwarg, spec.database is None."""
        spec = IntoClickHouse("t")._to_spec()
        assert spec.database is None


class TestRepr:
    def test_repr_includes_table_and_mode(self) -> None:
        """repr(IntoClickHouse('motos').replace()) contains table and mode info."""
        builder = IntoClickHouse("motos").replace()
        text = repr(builder)
        assert "motos" in text
        assert "replace" in text
