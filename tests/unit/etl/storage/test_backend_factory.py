"""Unit tests for backend factories and observability observer wiring."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.etl.observability.config import ExecutionRecordStoreConfig, ObservabilityConfig
from loom.etl.observability.factory import make_observers
from loom.etl.observability.observers.structlog import StructlogRunObserver
from loom.etl.storage._config import StorageConfig, StorageDefaults, TablePathConfig
from loom.etl.storage._factory import make_backends, make_temp_store


def _path_defaults(root: str) -> StorageDefaults:
    return StorageDefaults(table_path=TablePathConfig(uri=root))


# ---------------------------------------------------------------------------
# make_backends
# ---------------------------------------------------------------------------


def test_make_backends_polars_path_defaults_returns_polars_types(tmp_path: Path) -> None:
    from loom.etl.backends.polars import DeltaCatalog, PolarsSourceReader, PolarsTargetWriter

    config = StorageConfig(defaults=_path_defaults(str(tmp_path / "test-lake")))
    reader, writer, catalog = make_backends(config)

    assert isinstance(reader, PolarsSourceReader)
    assert isinstance(writer, PolarsTargetWriter)
    assert isinstance(catalog, DeltaCatalog)


def test_make_backends_spark_without_session_raises() -> None:
    config = StorageConfig(engine="spark")
    with pytest.raises(ValueError, match="SparkSession"):
        make_backends(config, spark=None)


# ---------------------------------------------------------------------------
# make_observers
# ---------------------------------------------------------------------------


def test_make_observers_log_true_includes_structlog() -> None:
    config = ObservabilityConfig(log=True)
    observers = make_observers(config)

    assert len(observers) == 1
    assert isinstance(observers[0], StructlogRunObserver)


def test_make_observers_log_false_returns_empty() -> None:
    config = ObservabilityConfig(log=False)
    observers = make_observers(config)

    assert observers == []


def test_make_observers_otel_true_includes_otel_observer() -> None:
    from loom.etl.observability.observers.otel import OtelRunObserver

    config = ObservabilityConfig(log=False, otel=True)
    observers = make_observers(config)

    assert len(observers) == 1
    assert isinstance(observers[0], OtelRunObserver)


def test_make_observers_log_and_otel_true_includes_both() -> None:
    from loom.etl.observability.observers.otel import OtelRunObserver

    config = ObservabilityConfig(log=True, otel=True)
    observers = make_observers(config)

    assert len(observers) == 2
    assert isinstance(observers[0], StructlogRunObserver)
    assert isinstance(observers[1], OtelRunObserver)


def test_make_observers_with_record_store_root_adds_observer() -> None:
    config = ObservabilityConfig(
        log=False,
        record_store=ExecutionRecordStoreConfig(root="/var/lib/loom/runs"),
    )
    observers = make_observers(config, StorageConfig())

    assert len(observers) == 1
    assert type(observers[0]).__name__ == "ExecutionRecordsObserver"


def test_make_observers_rejects_database_destination_for_polars_engine() -> None:
    config = ObservabilityConfig(
        log=False,
        record_store=ExecutionRecordStoreConfig(database="ops"),
    )
    with pytest.raises(ValueError, match="storage.engine='spark'"):
        make_observers(config, StorageConfig(engine="polars"))


def test_make_observers_record_store_requires_storage_config() -> None:
    config = ObservabilityConfig(
        log=False, record_store=ExecutionRecordStoreConfig(root="/var/lib/loom/runs")
    )
    with pytest.raises(ValueError, match="storage config is required"):
        make_observers(config)


# ---------------------------------------------------------------------------
# make_temp_store
# ---------------------------------------------------------------------------


def test_make_temp_store_no_root_returns_none() -> None:
    config = StorageConfig()
    result = make_temp_store(config)

    assert result is None


def test_make_temp_store_with_root_returns_store(tmp_path: Path) -> None:
    from loom.etl.temp._store import IntermediateStore

    root = tmp_path / "lake"
    config = StorageConfig(tmp_root=str(root / "tmp"))
    result = make_temp_store(config)

    assert isinstance(result, IntermediateStore)
