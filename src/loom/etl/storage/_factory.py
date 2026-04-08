"""Storage backend factory for reader/writer/catalog and temp store wiring.

Internal module — not part of the public API.
"""

from __future__ import annotations

from typing import Any, Protocol

from loom.etl.schema._schema import ColumnSchema
from loom.etl.schema._table import TableRef
from loom.etl.storage._config import CatalogConnection, StorageConfig
from loom.etl.storage._io import SourceReader, TableDiscovery, TargetWriter
from loom.etl.storage._locator import MappingLocator, PrefixLocator, TableLocation, TableLocator
from loom.etl.storage.route import RoutedCatalog, build_table_resolver
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

    route_resolver = build_table_resolver(config)
    path_catalog = DeltaCatalog(config.to_path_locator()) if config.has_path_routes() else None
    spark_catalog = SparkCatalog(spark)
    catalog_backend: TableDiscovery = spark_catalog
    if config.catalogs:
        catalog_backend = _SparkCatalogDiscovery(
            spark_catalog=spark_catalog,
            unity_catalog=DeltaCatalog(_UnityCatalogLocator(config)),
            unity_catalog_keys=frozenset(config.catalogs),
        )
    catalog = RoutedCatalog(route_resolver, catalog=catalog_backend, path=path_catalog)
    return (
        SparkSourceReader(spark, route_resolver=route_resolver),
        SparkTargetWriter(spark, None, route_resolver=route_resolver),
        catalog,
    )


def _make_polars_backends(
    config: StorageConfig,
) -> tuple[SourceReader, TargetWriter, TableDiscovery]:
    from loom.etl.backends.polars import DeltaCatalog, PolarsSourceReader, PolarsTargetWriter

    locator = _build_polars_locator(config)
    catalog = DeltaCatalog(locator)
    return PolarsSourceReader(locator), PolarsTargetWriter(locator), catalog


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


class _UnityCatalogLocator:
    def __init__(self, config: StorageConfig) -> None:
        self._catalogs = config.catalogs

    def locate(self, ref: TableRef) -> TableLocation:
        parts = tuple(part for part in ref.ref.split(".") if part)
        if len(parts) != 3:
            raise KeyError(
                f"Unity catalog locator requires 'catalog.schema.table', got {ref.ref!r}."
            )
        catalog_key = parts[0]
        connection = self._catalogs.get(catalog_key)
        if connection is None:
            raise KeyError(f"No unity catalog connection configured for catalog {catalog_key!r}.")
        return TableLocation(
            uri=f"uc://{ref.ref}",
            storage_options=_unity_storage_options_from_connection(connection),
        )


class _SparkCatalogDiscovery:
    def __init__(
        self,
        *,
        spark_catalog: TableDiscovery,
        unity_catalog: TableDiscovery,
        unity_catalog_keys: frozenset[str],
    ) -> None:
        self._spark_catalog = spark_catalog
        self._unity_catalog = unity_catalog
        self._unity_catalog_keys = unity_catalog_keys

    def exists(self, ref: TableRef) -> bool:
        return self._delegate(ref).exists(ref)

    def columns(self, ref: TableRef) -> tuple[str, ...]:
        return self._delegate(ref).columns(ref)

    def schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        return self._delegate(ref).schema(ref)

    def update_schema(self, ref: TableRef, schema: tuple[ColumnSchema, ...]) -> None:
        self._delegate(ref).update_schema(ref, schema)

    def _delegate(self, ref: TableRef) -> TableDiscovery:
        parts = tuple(part for part in ref.ref.split(".") if part)
        if len(parts) == 3 and parts[0] in self._unity_catalog_keys:
            return self._unity_catalog
        return self._spark_catalog
