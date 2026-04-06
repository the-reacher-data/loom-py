"""Unit tests for _backend_factory — make_backends dispatch and make_observers/make_temp_store."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.etl.executor.observer._structlog import StructlogRunObserver
from loom.etl.storage._config import DeltaConfig, UnityCatalogConfig
from loom.etl.storage._factory import make_backends, make_observers, make_temp_store
from loom.etl.storage._observability import ObservabilityConfig, RunSinkConfig

# ---------------------------------------------------------------------------
# make_backends — DeltaConfig → Polars backends
# ---------------------------------------------------------------------------


def test_make_backends_delta_config_returns_polars_types(tmp_path: Path) -> None:
    from loom.etl.backends.polars import DeltaCatalog, PolarsDeltaReader, PolarsDeltaWriter

    config = DeltaConfig(root=str(tmp_path / "test-lake"))
    reader, writer, catalog = make_backends(config)

    assert isinstance(reader, PolarsDeltaReader)
    assert isinstance(writer, PolarsDeltaWriter)
    assert isinstance(catalog, DeltaCatalog)


# ---------------------------------------------------------------------------
# make_backends — UnityCatalogConfig without spark → ValueError
# ---------------------------------------------------------------------------


def test_make_backends_unity_catalog_without_spark_raises() -> None:
    config = UnityCatalogConfig(type="unity_catalog")
    with pytest.raises(ValueError, match="SparkSession"):
        make_backends(config, spark=None)


# ---------------------------------------------------------------------------
# make_observers — log=True produces StructlogRunObserver
# ---------------------------------------------------------------------------


def test_make_observers_log_true_includes_structlog() -> None:
    config = ObservabilityConfig(log=True)
    observers = make_observers(config, DeltaConfig(root="/tmp/lake"))

    assert len(observers) == 1
    assert isinstance(observers[0], StructlogRunObserver)


def test_make_observers_log_false_returns_empty() -> None:
    config = ObservabilityConfig(log=False)
    observers = make_observers(config, DeltaConfig(root="/tmp/lake"))

    assert observers == []


def test_make_observers_no_sink_when_run_sink_none() -> None:
    config = ObservabilityConfig(log=True, run_sink=None)
    observers = make_observers(config, DeltaConfig(root="/tmp/lake"))

    assert len(observers) == 1  # only structlog


def test_make_observers_with_run_sink_root_adds_sink_observer() -> None:
    config = ObservabilityConfig(
        log=False,
        run_sink=RunSinkConfig(root="/tmp/runs"),
    )
    observers = make_observers(config, DeltaConfig(root="/tmp/lake"))

    assert len(observers) == 1
    assert type(observers[0]).__name__ == "RunSinkObserver"


def test_make_observers_rejects_database_destination_for_polars_backend() -> None:
    config = ObservabilityConfig(
        log=False,
        run_sink=RunSinkConfig(database="ops"),
    )
    with pytest.raises(ValueError, match="only supported with Spark/Unity Catalog"):
        make_observers(config, DeltaConfig(root="/tmp/lake"))


# ---------------------------------------------------------------------------
# make_temp_store — no tmp_root returns None
# ---------------------------------------------------------------------------


def test_make_temp_store_no_root_returns_none(tmp_path: Path) -> None:
    config = DeltaConfig(root=str(tmp_path / "lake"))  # tmp_root defaults to ""
    result = make_temp_store(config)

    assert result is None


def test_make_temp_store_with_root_returns_store(tmp_path: Path) -> None:
    from loom.etl.temp._store import IntermediateStore

    root = tmp_path / "lake"
    config = DeltaConfig(root=str(root), tmp_root=str(root / "tmp"))
    result = make_temp_store(config)

    assert isinstance(result, IntermediateStore)
