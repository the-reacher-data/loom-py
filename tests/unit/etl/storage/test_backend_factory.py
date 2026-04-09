"""Unit tests for backend factories and observability observer wiring."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.etl.observability.config import ExecutionRecordStoreConfig, ObservabilityConfig
from loom.etl.observability.factory import make_observers
from loom.etl.observability.observers.structlog import StructlogRunObserver
from loom.etl.storage._config import (
    CatalogConnection,
    StorageConfig,
    StorageDefaults,
    TablePathConfig,
    TableRoute,
)
from loom.etl.storage._factory import make_backends, make_temp_store


def _path_defaults(root: str) -> StorageDefaults:
    return StorageDefaults(table_path=TablePathConfig(uri=root))


# ---------------------------------------------------------------------------
# make_backends
# ---------------------------------------------------------------------------


def test_make_backends_polars_path_defaults_returns_polars_types(tmp_path: Path) -> None:
    from loom.etl.backends.polars import PolarsSourceReader, PolarsTargetWriter

    config = StorageConfig(defaults=_path_defaults(str(tmp_path / "test-lake")))
    reader, writer = make_backends(config)

    assert isinstance(reader, PolarsSourceReader)
    assert isinstance(writer, PolarsTargetWriter)


def test_make_backends_spark_without_session_raises() -> None:
    config = StorageConfig(engine="spark")
    with pytest.raises(ValueError, match="SparkSession"):
        make_backends(config, spark=None)


def test_make_backends_prefers_spark_when_session_is_provided() -> None:
    from unittest.mock import MagicMock

    from loom.etl.backends.spark._io_compat import SparkSourceReader, SparkTargetWriter

    spark = MagicMock()
    config = StorageConfig(engine="polars")
    reader, writer = make_backends(config, spark=spark)

    assert isinstance(reader, SparkSourceReader)
    assert isinstance(writer, SparkTargetWriter)


def test_make_backends_polars_catalog_route_builds_backends() -> None:
    from loom.etl.backends.polars import PolarsSourceReader, PolarsTargetWriter

    config = StorageConfig(
        catalogs={
            "unity": CatalogConnection(workspace="https://dbc.example", token="token-123"),
        },
        tables=(TableRoute(name="raw.orders", ref="raw.orders", catalog="unity"),),
    )
    reader, writer = make_backends(config)

    assert isinstance(reader, PolarsSourceReader)
    assert isinstance(writer, PolarsTargetWriter)


def test_make_backends_polars_mixed_routes_builds_backends(tmp_path: Path) -> None:
    from loom.etl.backends.polars import PolarsSourceReader, PolarsTargetWriter

    config = StorageConfig(
        defaults=_path_defaults(str(tmp_path / "lake")),
        catalogs={
            "unity": CatalogConnection(workspace="https://dbc.example", token="token-123"),
        },
        tables=(
            TableRoute(
                name="raw.path_orders",
                path=TablePathConfig(uri=str(tmp_path / "explicit" / "orders")),
            ),
            TableRoute(name="raw.uc_orders", ref="raw.orders", catalog="unity"),
        ),
    )

    reader, writer = make_backends(config)
    assert isinstance(reader, PolarsSourceReader)
    assert isinstance(writer, PolarsTargetWriter)


def test_make_backends_polars_two_part_uc_ref_requires_catalog_key() -> None:
    config = StorageConfig(
        tables=(TableRoute(name="raw.orders", ref="raw.orders"),),
    )
    with pytest.raises(ValueError, match="2-part refs require route.catalog"):
        make_backends(config)


def test_make_backends_spark_catalog_route_builds_backends() -> None:
    from unittest.mock import MagicMock

    from loom.etl.backends.spark._io_compat import SparkSourceReader, SparkTargetWriter

    spark = MagicMock()
    config = StorageConfig(
        engine="spark",
        catalogs={
            "unity": CatalogConnection(workspace="https://dbc.example", token="token-123"),
        },
        tables=(TableRoute(name="raw.orders", ref="raw.orders", catalog="unity"),),
    )
    reader, writer = make_backends(config, spark=spark)
    assert isinstance(reader, SparkSourceReader)
    assert isinstance(writer, SparkTargetWriter)


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
