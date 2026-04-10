"""Runtime wiring helpers for ETL runner dependencies.

Internal module — not part of the public API.
"""

from __future__ import annotations

from typing import Any, Protocol

from loom.etl.checkpoint import CheckpointStore, FsspecTempCleaner, TempCleaner
from loom.etl.checkpoint._backends._polars import _PolarsCheckpointBackend
from loom.etl.checkpoint._backends._spark import _SparkCheckpointBackend
from loom.etl.checkpoint._cleaners import _is_cloud_path
from loom.etl.runtime.contracts import SourceReader, TargetWriter
from loom.etl.storage import CatalogConnection, StorageConfig
from loom.etl.storage._locator import MappingLocator, PrefixLocator, TableLocation, TableLocator
from loom.etl.storage.routing import build_table_resolver


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
    return _build_backend_pair(config, spark)


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


def _make_checkpoint_backend(spark: Any, storage_options: dict[str, str]) -> Any:
    if spark is not None:
        return _SparkCheckpointBackend(spark)
    return _PolarsCheckpointBackend(storage_options)


def _build_backend_pair(config: StorageConfig, spark: Any) -> tuple[SourceReader, TargetWriter]:
    if spark is not None:
        return _build_spark_pair(config, spark)
    return _build_polars_pair(config)


def _build_spark_pair(config: StorageConfig, spark: Any) -> tuple[SourceReader, TargetWriter]:
    from loom.etl.backends.spark import SparkSourceReader, SparkTargetWriter

    route_resolver = build_table_resolver(config)
    return (
        SparkSourceReader(spark, route_resolver=route_resolver),
        SparkTargetWriter(
            spark,
            None,
            route_resolver=route_resolver,
            missing_table_policy=config.missing_table_policy,
        ),
    )


def _build_polars_pair(config: StorageConfig) -> tuple[SourceReader, TargetWriter]:
    from loom.etl.backends.polars import PolarsSourceReader, PolarsTargetWriter

    locator = _build_polars_locator(config)
    return (
        PolarsSourceReader(locator),
        PolarsTargetWriter(locator, missing_table_policy=config.missing_table_policy),
    )


def _build_polars_locator(config: StorageConfig) -> TableLocator:
    mapping: dict[str, TableLocation] = {}
    for route in config.tables:
        if route.path is not None:
            mapping[route.name] = route.path.to_location()
            continue
        if route.ref.strip():
            qualified_ref, catalog_key = _qualify_polars_catalog_ref(
                config, route.ref, route.catalog
            )
            mapping[route.name] = TableLocation(
                uri=f"uc://{qualified_ref}",
                storage_options=_unity_storage_options(config, catalog_key),
            )

    default_path = config.defaults.table_path
    if mapping:
        default_location = default_path.to_location() if default_path is not None else None
        return MappingLocator(mapping=mapping, default=default_location)
    if default_path is not None:
        return PrefixLocator(
            root=default_path.uri,
            storage_options=default_path.storage_options or None,
            writer=default_path.writer or None,
            delta_config=default_path.delta_config or None,
            commit=default_path.commit or None,
        )
    raise ValueError(
        "storage.to_path_locator: no path routes configured. "
        "Define storage.defaults.table_path or add explicit storage.tables entries."
    )


def _qualify_polars_catalog_ref(
    config: StorageConfig, ref: str, catalog_key: str
) -> tuple[str, str]:
    parts = tuple(part for part in ref.split(".") if part)
    if len(parts) == 3:
        if catalog_key:
            return ref, catalog_key
        catalog_name = parts[0]
        return ref, catalog_name if catalog_name in config.catalogs else ""
    if len(parts) == 2:
        key = catalog_key or ("default" if "default" in config.catalogs else "")
        if not key:
            raise ValueError(
                "Polars UC routes with 2-part refs require route.catalog or "
                "storage.catalogs.default to build uc://catalog.schema.table."
            )
        return f"{key}.{ref}", key
    raise ValueError(
        f"Invalid catalog ref {ref!r}: expected 'schema.table' or 'catalog.schema.table'."
    )


def _unity_storage_options(config: StorageConfig, catalog_key: str) -> dict[str, str]:
    if not catalog_key:
        return {}
    connection = config.catalogs.get(catalog_key)
    if connection is None:
        return {}
    return _unity_storage_options_from_connection(connection)


def _unity_storage_options_from_connection(connection: CatalogConnection) -> dict[str, str]:
    options: dict[str, str] = {}
    if connection.workspace:
        options["databricks_workspace_url"] = connection.workspace
    if connection.token:
        options["databricks_access_token"] = connection.token
    return options


__all__ = ["make_backends", "make_checkpoint_store"]
