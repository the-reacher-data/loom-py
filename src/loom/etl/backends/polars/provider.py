"""Polars backend provider for ETL runner wiring."""

from __future__ import annotations

from typing import Any

from loom.etl.backends.polars._reader import PolarsSourceReader
from loom.etl.backends.polars._writer import PolarsTargetWriter
from loom.etl.observability.config import ExecutionRecordStoreConfig
from loom.etl.observability.sinks import ExecutionRecordWriter, TargetExecutionRecordWriter
from loom.etl.runner._providers import BackendProvider
from loom.etl.runtime.contracts import SourceReader, TargetWriter
from loom.etl.storage._config import CatalogConnection, StorageConfig
from loom.etl.storage._locator import MappingLocator, PrefixLocator, TableLocation, TableLocator


class PolarsProvider(BackendProvider):
    """Create Polars reader/writer pair from storage config."""

    def create_backends(
        self,
        config: StorageConfig,
        spark: Any = None,
    ) -> tuple[SourceReader, TargetWriter]:
        _ = spark
        locator = _build_polars_locator(config)
        return (
            PolarsSourceReader(locator),
            PolarsTargetWriter(locator, missing_table_policy=config.missing_table_policy),
        )

    def create_execution_record_writer(
        self,
        config: StorageConfig,
        record_store: ExecutionRecordStoreConfig,
        spark: Any = None,
    ) -> ExecutionRecordWriter:
        _ = (config, spark)
        if record_store.database:
            raise ValueError(
                "observability.record_store.database is only supported "
                "with storage.engine='spark'. "
                "For storage.engine='polars', configure observability.record_store.root."
            )

        locator = PrefixLocator(
            root=record_store.root,
            storage_options=record_store.storage_options or None,
            writer=record_store.writer or None,
            delta_config=record_store.delta_config or None,
            commit=record_store.commit or None,
        )
        target_writer = PolarsTargetWriter(locator)
        return TargetExecutionRecordWriter(target_writer)


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


__all__ = ["PolarsProvider"]
