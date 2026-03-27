"""Backend factory — instantiate reader, writer, catalog, observers and temp store
from a resolved :data:`~loom.etl.StorageConfig`.

This module is the single extension point for new storage backends.
Adding a third backend requires:
1. A new ``case NewConfig():`` branch in :func:`make_backends`.
2. A corresponding ``_make_<name>_backends()`` helper.

Internal module — not part of the public API.
"""

from __future__ import annotations

import logging
from typing import Any, Protocol

from loom.etl.executor import ETLRunObserver
from loom.etl.storage._config import DeltaConfig, StorageConfig, UnityCatalogConfig
from loom.etl.storage._io import SourceReader, TableDiscovery, TargetWriter
from loom.etl.storage._observability import ObservabilityConfig
from loom.etl.temp._cleaners import TempCleaner
from loom.etl.temp._store import IntermediateStore

_log = logging.getLogger(__name__)


class _TempStoreAware(Protocol):
    """Structural protocol satisfied by every StorageConfig variant.

    All current variants (:class:`~loom.etl.DeltaConfig`,
    :class:`~loom.etl.UnityCatalogConfig`) carry these two fields.  Any future
    backend added to :data:`~loom.etl.StorageConfig` must also declare them so
    that :func:`make_temp_store` works without modification.
    """

    tmp_root: str
    tmp_storage_options: dict[str, str]


def make_backends(
    config: StorageConfig,
    spark: Any = None,
) -> tuple[SourceReader, TargetWriter, TableDiscovery]:
    """Instantiate reader, writer, and catalog from *config*.

    Args:
        config: Resolved storage config.
        spark:  Active SparkSession.  Required when *config* is
                :class:`~loom.etl.UnityCatalogConfig`.

    Returns:
        Triple of (reader, writer, catalog).

    Raises:
        ValueError: When *config* is UnityCatalogConfig and *spark* is None.
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


def make_observers(config: ObservabilityConfig) -> list[ETLRunObserver]:
    """Build the observer list from *config*.

    Args:
        config: Observability configuration.

    Returns:
        Ordered list of :class:`~loom.etl.executor.ETLRunObserver` instances.
    """
    from loom.etl.executor.observer._sink_observer import RunSinkObserver
    from loom.etl.executor.observer._structlog import StructlogRunObserver
    from loom.etl.executor.observer.sinks import DeltaRunSink

    observers: list[ETLRunObserver] = []
    if config.log:
        observers.append(StructlogRunObserver(slow_step_threshold_ms=config.slow_step_threshold_ms))
    if config.run_sink is not None and config.run_sink.root:
        from loom.etl.storage._locator import TableLocation

        location = TableLocation(
            uri=config.run_sink.root,
            storage_options=config.run_sink.storage_options,
        )
        observers.append(RunSinkObserver(DeltaRunSink(location)))
    return observers


def make_temp_store(
    config: StorageConfig,
    spark: Any = None,
    cleaner: TempCleaner | None = None,
) -> IntermediateStore | None:
    """Build an IntermediateStore from config, or None when unconfigured.

    Args:
        config:  Resolved storage config.
        spark:   Active SparkSession (Spark backend only).
        cleaner: Cloud-aware temp cleaner.

    Returns:
        Configured :class:`~loom.etl._temp_store.IntermediateStore` or None.
    """
    from typing import cast

    temp_cfg = cast(_TempStoreAware, config)
    if not temp_cfg.tmp_root:
        return None
    return IntermediateStore(
        tmp_root=temp_cfg.tmp_root,
        storage_options=temp_cfg.tmp_storage_options or {},
        spark=spark,
        cleaner=cleaner,
    )


def _make_spark_backends(
    spark: Any,
) -> tuple[SourceReader, TargetWriter, TableDiscovery]:
    from loom.etl.backends.spark import SparkCatalog, SparkDeltaReader, SparkDeltaWriter

    catalog = SparkCatalog(spark)
    return SparkDeltaReader(spark), SparkDeltaWriter(spark, None, catalog), catalog


def _make_polars_backends(
    config: DeltaConfig,
) -> tuple[SourceReader, TargetWriter, TableDiscovery]:
    from loom.etl.backends.polars import DeltaCatalog, PolarsDeltaReader, PolarsDeltaWriter

    locator = config.to_locator()
    catalog = DeltaCatalog(locator)
    return PolarsDeltaReader(locator), PolarsDeltaWriter(locator, catalog), catalog
