"""Unit tests for backend factories and observability observer wiring."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.etl.observability.config import (
    ExecutionRecordStoreConfig,
    ObservabilityConfig,
    OtelConfig,
)
from loom.etl.observability.factory import make_observers
from loom.etl.observability.observers.structlog import StructlogRunObserver
from loom.etl.runner._providers import load_backend_provider
from loom.etl.runner._wiring import (
    make_backends,
    make_checkpoint_store,
    make_execution_record_writer,
)
from loom.etl.storage._config import (
    CatalogConnection,
    StorageConfig,
    StorageDefaults,
    TablePathConfig,
    TableRoute,
)


def _path_defaults(root: str) -> StorageDefaults:
    return StorageDefaults(table_path=TablePathConfig(uri=root))


class _DummyExecutionRecordWriter:
    def write_record(self, record: object, table_ref: object, /) -> None:
        _ = (record, table_ref)


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

    from loom.etl.backends.spark import SparkSourceReader, SparkTargetWriter

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

    from loom.etl.backends.spark import SparkSourceReader, SparkTargetWriter

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


def test_load_backend_provider_resolves_registered_providers() -> None:
    assert type(load_backend_provider("polars")).__name__ == "PolarsProvider"
    assert type(load_backend_provider("spark")).__name__ == "SparkProvider"


def test_load_backend_provider_rejects_unknown_engine() -> None:
    with pytest.raises(ValueError, match="No backend provider registered"):
        load_backend_provider("duckdb")


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


def test_make_observers_with_otel_config_enables_otel_observer() -> None:
    from loom.etl.observability.observers.otel import OtelRunObserver

    config = ObservabilityConfig(
        log=False,
        otel=False,
        otel_config=OtelConfig(service_name="loom-tests"),
    )
    observers = make_observers(config)

    assert len(observers) == 1
    assert isinstance(observers[0], OtelRunObserver)


def test_make_observers_with_record_store_root_adds_observer() -> None:
    config = ObservabilityConfig(
        log=False,
        record_store=ExecutionRecordStoreConfig(root="/var/lib/loom/runs"),
    )
    observers = make_observers(config, record_writer=_DummyExecutionRecordWriter())

    assert len(observers) == 1
    assert type(observers[0]).__name__ == "ExecutionRecordsObserver"


def test_make_execution_record_writer_rejects_database_destination_for_polars_engine() -> None:
    obs_config = ObservabilityConfig(
        log=False,
        record_store=ExecutionRecordStoreConfig(database="ops"),
    )
    with pytest.raises(ValueError, match="storage.engine='spark'"):
        make_execution_record_writer(StorageConfig(engine="polars"), obs_config)


def test_make_execution_record_writer_polars_root_returns_writer() -> None:
    obs_config = ObservabilityConfig(
        log=False,
        record_store=ExecutionRecordStoreConfig(root="s3://bucket/runs"),
    )
    writer = make_execution_record_writer(StorageConfig(engine="polars"), obs_config)
    assert writer is not None
    assert type(writer).__name__ == "TargetExecutionRecordWriter"


def test_make_execution_record_writer_spark_database_requires_session() -> None:
    obs_config = ObservabilityConfig(
        log=False,
        record_store=ExecutionRecordStoreConfig(database="ops"),
    )
    with pytest.raises(ValueError, match="SparkSession"):
        make_execution_record_writer(StorageConfig(engine="spark"), obs_config, spark=None)


def test_make_observers_record_store_requires_record_writer() -> None:
    config = ObservabilityConfig(
        log=False, record_store=ExecutionRecordStoreConfig(root="/var/lib/loom/runs")
    )
    with pytest.raises(ValueError, match="record_writer is required"):
        make_observers(config)


# ---------------------------------------------------------------------------
# make_checkpoint_store
# ---------------------------------------------------------------------------


def test_make_checkpoint_store_no_root_returns_none() -> None:
    config = StorageConfig()
    result = make_checkpoint_store(config)

    assert result is None


def test_make_checkpoint_store_with_root_returns_store() -> None:
    from loom.etl.checkpoint import CheckpointStore

    config = StorageConfig(tmp_root="s3://bucket/checkpoints")
    result = make_checkpoint_store(config)

    assert isinstance(result, CheckpointStore)
