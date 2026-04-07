"""Storage backend factory for reader/writer/catalog and temp store wiring.

Internal module — not part of the public API.
"""

from __future__ import annotations

from typing import Any, Protocol

from loom.etl.storage._config import StorageConfig
from loom.etl.storage._io import SourceReader, TableDiscovery, TargetWriter
from loom.etl.temp._cleaners import TempCleaner
from loom.etl.temp._store import IntermediateStore


class _TempStoreAware(Protocol):
    """Structural protocol satisfied by every StorageConfig variant."""

    @property
    def tmp_root(self) -> str: ...

    @property
    def tmp_storage_options(self) -> dict[str, str]: ...


def make_backends(
    config: StorageConfig,
    spark: Any = None,
) -> tuple[SourceReader, TargetWriter, TableDiscovery]:
    """Instantiate reader, writer, and catalog from *config*.

    Args:
        config: Resolved storage config.
        spark: Active SparkSession. Required for Unity Catalog.

    Returns:
        Triple of ``(reader, writer, catalog)``.

    Raises:
        ValueError: If Spark engine is requested but no SparkSession is provided.
    """
    if config.engine == "spark":
        if spark is None:
            raise ValueError(
                "A SparkSession is required when storage.engine='spark'. "
                "Pass spark=<session> to ETLRunner.from_yaml() or ETLRunner.from_config()."
            )
        return _make_spark_backends(config, spark)
    return _make_polars_backends(config)


def make_temp_store(
    config: _TempStoreAware,
    spark: Any = None,
    cleaner: TempCleaner | None = None,
) -> IntermediateStore | None:
    """Build an intermediate store from config or return ``None`` when disabled."""
    if not config.tmp_root:
        return None
    backend = _make_temp_backend(spark, config.tmp_storage_options or {})
    return IntermediateStore(
        tmp_root=config.tmp_root,
        backend=backend,
        cleaner=cleaner,
    )


def _make_temp_backend(spark: Any, storage_options: dict[str, str]) -> Any:
    if spark is not None:
        from loom.etl.backends.spark._temp import _SparkTempBackend

        return _SparkTempBackend(spark)
    from loom.etl.backends.polars._temp import _PolarsTempBackend

    return _PolarsTempBackend(storage_options)


def _make_spark_backends(
    config: StorageConfig,
    spark: Any,
) -> tuple[SourceReader, TargetWriter, TableDiscovery]:
    from loom.etl.backends.polars import DeltaCatalog
    from loom.etl.backends.spark import SparkCatalog, SparkSourceReader, SparkTargetWriter

    has_catalog_routes = config.has_catalog_routes()
    has_path_routes = config.has_path_routes()
    if has_catalog_routes and has_path_routes:
        raise ValueError(
            "Mixed catalog+path table routing for Spark is not wired yet in factory. "
            "Use catalog-only or path-only routes for now."
        )

    if has_catalog_routes or not has_path_routes:
        spark_catalog = SparkCatalog(spark)
        return SparkSourceReader(spark), SparkTargetWriter(spark, None), spark_catalog

    locator = config.to_path_locator()
    delta_catalog = DeltaCatalog(locator)
    return SparkSourceReader(spark, locator), SparkTargetWriter(spark, locator), delta_catalog


def _make_polars_backends(
    config: StorageConfig,
) -> tuple[SourceReader, TargetWriter, TableDiscovery]:
    from loom.etl.backends.polars import DeltaCatalog, PolarsSourceReader, PolarsTargetWriter

    if config.has_catalog_routes():
        raise ValueError(
            "Catalog table routes are not wired for Polars yet. "
            "Use path routes/defaults until we complete the writer-side design."
        )
    locator = config.to_path_locator()
    catalog = DeltaCatalog(locator)
    return PolarsSourceReader(locator), PolarsTargetWriter(locator), catalog
