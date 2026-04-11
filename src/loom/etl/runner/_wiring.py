"""Runtime wiring helpers for ETL runner dependencies.

Internal module — not part of the public API.
"""

from __future__ import annotations

from typing import Any, Protocol

from loom.etl.checkpoint import CheckpointStore, FsspecTempCleaner, TempCleaner
from loom.etl.checkpoint._backends._polars import _PolarsCheckpointBackend
from loom.etl.checkpoint._backends._spark import _SparkCheckpointBackend
from loom.etl.checkpoint._cleaners import _is_cloud_path
from loom.etl.observability.config import ObservabilityConfig
from loom.etl.observability.sinks import ExecutionRecordWriter
from loom.etl.runner._providers import load_backend_provider
from loom.etl.runtime.contracts import SourceReader, TargetWriter
from loom.etl.storage._config import StorageConfig


class _CheckpointConfig(Protocol):
    """Structural protocol satisfied by every StorageConfig variant."""

    @property
    def checkpoint_root(self) -> str: ...

    @property
    def checkpoint_storage_options(self) -> dict[str, str]: ...


def make_backends(
    config: StorageConfig,
    spark: Any = None,
) -> tuple[SourceReader, TargetWriter]:
    """Instantiate reader and writer from *config*.

    Args:
        config: Resolved storage config.
        spark: Active SparkSession. Required for Unity Catalog.

    Returns:
        Pair ``(reader, writer)``.

    Selection rule:
        * ``spark is not None`` -> Spark backends.
        * ``spark is None`` -> Polars backends.

    Raises:
        ValueError: If ``storage.engine='spark'`` but ``spark`` is not provided.
    """
    if config.engine == "spark" and spark is None:
        raise ValueError(
            "A SparkSession is required when storage.engine='spark'. "
            "Pass spark=<session> to ETLRunner.from_yaml() or ETLRunner.from_config()."
        )
    engine = _resolve_engine(config, spark)
    provider = load_backend_provider(engine)
    return provider.create_backends(config, spark)


def make_checkpoint_store(
    config: _CheckpointConfig,
    spark: Any = None,
    cleaner: TempCleaner | None = None,
) -> CheckpointStore | None:
    """Build a checkpoint store from config or return ``None`` when disabled."""
    if not config.checkpoint_root:
        return None
    if not _is_cloud_path(config.checkpoint_root):
        raise ValueError(
            "checkpoint_root must be a cloud URI (s3://, gs://, abfss://, ...). "
            "Local checkpoint paths are not supported."
        )
    resolved_cleaner = cleaner or FsspecTempCleaner(
        storage_options=config.checkpoint_storage_options or {}
    )
    backend = _make_checkpoint_backend(spark, config.checkpoint_storage_options or {})
    return CheckpointStore(
        root=config.checkpoint_root,
        backend=backend,
        cleaner=resolved_cleaner,
    )


def make_execution_record_writer(
    storage: StorageConfig,
    observability: ObservabilityConfig,
    spark: Any = None,
) -> ExecutionRecordWriter | None:
    """Build execution-record writer from storage/observability configs."""
    store_cfg = observability.record_store
    if store_cfg is None:
        return None
    store_cfg.validate()
    engine = _resolve_engine(storage, spark)
    provider = load_backend_provider(engine)
    return provider.create_execution_record_writer(storage, store_cfg, spark)


def _make_checkpoint_backend(spark: Any, storage_options: dict[str, str]) -> Any:
    if spark is not None:
        return _SparkCheckpointBackend(spark)
    return _PolarsCheckpointBackend(storage_options)


def _resolve_engine(config: StorageConfig, spark: Any) -> str:
    if spark is not None:
        return "spark"
    return config.engine


__all__ = ["make_backends", "make_checkpoint_store", "make_execution_record_writer"]
