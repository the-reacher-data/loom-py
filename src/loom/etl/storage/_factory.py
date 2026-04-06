"""Storage backend factory for reader/writer/catalog and temp store wiring.

Internal module — not part of the public API.
"""

from __future__ import annotations

from typing import Any, Protocol

from loom.etl.storage._config import DeltaConfig, StorageConfig, UnityCatalogConfig
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
        ValueError: If *config* is UnityCatalogConfig and *spark* is ``None``.
    """
    match config:
        case DeltaConfig():
            return _make_polars_backends(config)
        case UnityCatalogConfig():
            if spark is None:
                raise ValueError(
                    "A SparkSession is required for UnityCatalogConfig. "
                    "Pass spark=<session> to ETLRunner.from_yaml() or ETLRunner.from_config()."
                )
            return _make_spark_backends(spark)
        case _:
            raise TypeError(f"Unsupported storage config type: {type(config).__name__!r}")


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
    spark: Any,
) -> tuple[SourceReader, TargetWriter, TableDiscovery]:
    from loom.etl.backends.spark import SparkCatalog, SparkSourceReader, SparkTargetWriter

    catalog = SparkCatalog(spark)
    return SparkSourceReader(spark), SparkTargetWriter(spark, None), catalog


def _make_polars_backends(
    config: DeltaConfig,
) -> tuple[SourceReader, TargetWriter, TableDiscovery]:
    from loom.etl.backends.polars import DeltaCatalog, PolarsSourceReader, PolarsTargetWriter

    locator = config.to_locator()
    catalog = DeltaCatalog(locator)
    return PolarsSourceReader(locator), PolarsTargetWriter(locator), catalog
