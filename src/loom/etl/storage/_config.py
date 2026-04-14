"""Unified ETL storage configuration.

Defines one canonical ``storage:`` model for both Spark and Polars runtimes.
Logical table names remain stable in pipelines while this config resolves each
name to either a catalog reference or a physical Delta path.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlparse

if TYPE_CHECKING:
    from loom.etl.storage._file_locator import MappingFileLocator

import msgspec
from deltalake import CommitProperties, WriterProperties

from loom.etl.storage._locator import MappingLocator, PrefixLocator, TableLocation, TableLocator


class StorageEngine(StrEnum):
    """Execution engine guardrail declared in the storage config."""

    POLARS = "polars"
    SPARK = "spark"


class MissingTablePolicy(StrEnum):
    """Policy used when a TABLE target does not exist at write time."""

    SCHEMA_MODE = "schema_mode"
    CREATE = "create"


class CatalogConnection(msgspec.Struct, frozen=True):
    """Catalog connection settings.

    Args:
        provider: Catalog provider. Currently ``"unity"``.
        workspace: Databricks workspace URL for Unity Catalog.
        token: Databricks access token.
    """

    provider: Literal["unity"] = "unity"
    workspace: str = ""
    token: str = ""


class TablePathConfig(msgspec.Struct, frozen=True):
    """Physical Delta path settings for table storage.

    Args:
        uri: Root Delta path/URI.
        storage_options: Object-store credentials/options.
        writer: Delta writer properties.
        delta_config: Delta table properties.
        commit: Delta commit metadata.
    """

    uri: str = ""
    storage_options: dict[str, str] = {}
    writer: dict[str, Any] = {}
    delta_config: dict[str, str | None] = {}
    commit: dict[str, Any] = {}

    def validate(self, *, context: str) -> None:
        """Validate path config and delta-rs option dictionaries."""
        if not self.uri.strip():
            raise ValueError(f"storage.{context}.uri must be a non-empty string")
        _validate_writer(self.writer, context=f"{context}.writer")
        _validate_commit(self.commit, context=f"{context}.commit")

    def to_location(self) -> TableLocation:
        """Convert to :class:`TableLocation`."""
        return TableLocation(
            uri=self.uri,
            storage_options=self.storage_options,
            writer=self.writer,
            delta_config=self.delta_config,
            commit=self.commit,
        )


class FilePathConfig(msgspec.Struct, frozen=True):
    """Physical path settings for FILE sources/targets.

    Args:
        uri: Output/input file URI template.
        storage_options: Object-store credentials/options.
    """

    uri: str = ""
    storage_options: dict[str, str] = {}

    def validate(self, *, context: str) -> None:
        """Validate file path config."""
        if not self.uri.strip():
            raise ValueError(f"storage.{context}.uri must be a non-empty string")


class StorageDefaults(msgspec.Struct, frozen=True):
    """Default resolution settings used when no per-name override is declared."""

    table_path: TablePathConfig | None = None


class TableRoute(msgspec.Struct, frozen=True):
    """Route one logical table name to catalog ref or physical path.

    Args:
        name: Logical table name used by the pipeline (e.g. ``"sys.customers"``).
        ref: Catalog reference. Supported forms:
            * ``"catalog.schema.table"``
            * ``"schema.table"`` (uses ``catalog`` or ``default``)
        catalog: Catalog connection key used with 2-part refs.
        path: Physical Delta path route.
    """

    name: str
    ref: str = ""
    catalog: str = ""
    path: TablePathConfig | None = None

    def validate(self, *, catalogs: dict[str, CatalogConnection], context: str) -> None:
        """Validate route syntax and exclusivity constraints."""
        if not self.name.strip():
            raise ValueError(f"storage.{context}.name must be a non-empty string")

        has_ref = bool(self.ref.strip())
        has_path = self.path is not None
        if has_ref == has_path:
            raise ValueError(
                f"storage.{context} must define exactly one destination: 'ref' or 'path'"
            )

        if has_ref:
            _validate_ref(self.ref, context=f"storage.{context}.ref")
            if self.catalog and self.catalog not in catalogs:
                raise ValueError(
                    f"storage.{context}.catalog={self.catalog!r} is not defined in storage.catalogs"
                )

        if self.path is not None:
            self.path.validate(context=f"{context}.path")


class FileRoute(msgspec.Struct, frozen=True):
    """Route one logical file name to a physical file URI.

    Args:
        name: Logical file name used by the pipeline.
        path: Physical path configuration.
    """

    name: str
    path: FilePathConfig

    def validate(self, *, context: str) -> None:
        """Validate file route fields."""
        if not self.name.strip():
            raise ValueError(f"storage.{context}.name must be a non-empty string")
        self.path.validate(context=f"{context}.path")


class StorageConfig(msgspec.Struct, frozen=True):
    """Canonical storage configuration used by ETL runner/factory.

    Args:
        engine: Engine guardrail.
        missing_table_policy: First-run policy for missing destination tables.
        catalogs: Catalog connection map.
        defaults: Default path settings.
        tables: Per-logical-table routes.
        files: Per-logical-file routes.
        tmp_root: Root URI for intermediate storage.
        tmp_storage_options: Credentials/options for intermediate storage.
    """

    engine: Literal["polars", "spark"] = "polars"
    missing_table_policy: MissingTablePolicy = MissingTablePolicy.SCHEMA_MODE
    catalogs: dict[str, CatalogConnection] = {}
    defaults: StorageDefaults = StorageDefaults()
    tables: tuple[TableRoute, ...] = ()
    files: tuple[FileRoute, ...] = ()
    tmp_root: str = ""
    tmp_storage_options: dict[str, str] = {}

    def validate(self) -> None:
        """Validate structural constraints and option dictionaries."""
        _ensure_unique_names(
            (route.name for route in self.tables),
            context="storage.tables",
        )
        _ensure_unique_names(
            (route.name for route in self.files),
            context="storage.files",
        )

        if self.defaults.table_path is not None:
            self.defaults.table_path.validate(context="defaults.table_path")

        for idx, table_route in enumerate(self.tables):
            table_route.validate(catalogs=self.catalogs, context=f"tables[{idx}]")
        for idx, file_route in enumerate(self.files):
            file_route.validate(context=f"files[{idx}]")
        if self.tmp_root and not _is_cloud_uri(self.tmp_root):
            raise ValueError(
                "storage.tmp_root must be a cloud URI (s3://, gs://, abfss://, ...). "
                "Local checkpoint roots are not supported."
            )
        self._validate_polars_uc_credentials()

    @property
    def checkpoint_root(self) -> str:
        """Checkpoint root URI (canonical alias of ``tmp_root``)."""
        return self.tmp_root

    @property
    def checkpoint_storage_options(self) -> dict[str, str]:
        """Checkpoint storage options (canonical alias of ``tmp_storage_options``)."""
        return self.tmp_storage_options

    def has_catalog_routes(self) -> bool:
        """Return ``True`` when at least one table route uses ``ref``."""
        return any(bool(route.ref.strip()) for route in self.tables)

    def to_file_locator(self) -> MappingFileLocator | None:
        """Build a :class:`~loom.etl.storage._file_locator.MappingFileLocator`
        from the declared ``storage.files`` routes, or ``None`` when no file
        routes are configured.

        Returns:
            Locator mapping each ``files[].name`` to its physical
            :class:`~loom.etl.storage._file_locator.FileLocation`, or
            ``None`` when ``storage.files`` is empty.
        """
        from loom.etl.storage._file_locator import FileLocation, MappingFileLocator

        if not self.files:
            return None
        mapping = {
            route.name: FileLocation(
                uri_template=route.path.uri,
                storage_options=route.path.storage_options,
            )
            for route in self.files
        }
        return MappingFileLocator(mapping)

    def has_path_routes(self) -> bool:
        """Return ``True`` when defaults or per-table routes use path mode."""
        if self.defaults.table_path is not None:
            return True
        return any(route.path is not None for route in self.tables)

    def to_path_locator(self) -> TableLocator:
        """Build a :class:`TableLocator` from path defaults and per-table path routes.

        Raises:
            ValueError: If no path defaults/routes are configured.
        """
        mapping = {
            route.name: route.path.to_location() for route in self.tables if route.path is not None
        }
        default_location = (
            self.defaults.table_path.to_location() if self.defaults.table_path is not None else None
        )
        if not mapping and default_location is None:
            raise ValueError(
                "storage.to_path_locator: no path routes configured. "
                "Define storage.defaults.table_path or storage.tables[].path."
            )
        if not mapping and default_location is not None:
            return PrefixLocator(
                root=default_location.uri,
                storage_options=default_location.storage_options or None,
                writer=default_location.writer or None,
                delta_config=default_location.delta_config or None,
                commit=default_location.commit or None,
            )
        return MappingLocator(mapping=mapping, default=default_location)

    def _validate_polars_uc_credentials(self) -> None:
        if self.engine != StorageEngine.POLARS.value:
            return
        for idx, table_route in enumerate(self.tables):
            if not table_route.ref.strip():
                continue
            catalog_key = _resolve_polars_catalog_key(table_route, self.catalogs)
            if not catalog_key:
                raise ValueError(
                    "Polars UC routes require credentials in storage.catalogs. "
                    f"Missing mapping for storage.tables[{idx}] (ref={table_route.ref!r}). "
                    "Set tables[].catalog or define matching storage.catalogs entry."
                )
            connection = self.catalogs[catalog_key]
            if not connection.workspace.strip() or not connection.token.strip():
                raise ValueError(
                    f"storage.catalogs[{catalog_key!r}] must define non-empty workspace and token "
                    "for Polars UC routes."
                )


def convert_storage_config(raw: dict[str, Any]) -> StorageConfig:
    """Convert a resolved plain dict into :class:`StorageConfig`."""
    return msgspec.convert(raw, StorageConfig)


def _validate_ref(ref: str, *, context: str) -> None:
    parts = tuple(part for part in ref.split(".") if part)
    if len(parts) not in (2, 3):
        raise ValueError(f"{context} must be 'schema.table' or 'catalog.schema.table', got {ref!r}")


def _ensure_unique_names(names: Any, *, context: str) -> None:
    seen: set[str] = set()
    for name in names:
        if name in seen:
            raise ValueError(f"{context} contains duplicate name {name!r}")
        seen.add(name)


def _resolve_polars_catalog_key(
    route: TableRoute,
    catalogs: Mapping[str, CatalogConnection],
) -> str:
    explicit_key = route.catalog.strip()
    if explicit_key:
        return explicit_key

    parts = tuple(part for part in route.ref.split(".") if part)
    if len(parts) == 2:
        return "default" if "default" in catalogs else ""
    if len(parts) == 3:
        catalog_from_ref = parts[0]
        return catalog_from_ref if catalog_from_ref in catalogs else ""
    return ""


def _is_cloud_uri(path: str) -> bool:
    cloud_schemes = frozenset({"s3", "gs", "gcs", "abfss", "abfs", "az", "r2"})
    parsed = urlparse(path.strip())
    if parsed.scheme.lower() not in cloud_schemes:
        return False
    # Cloud roots must include authority/bucket/container to avoid accepting
    # incomplete URIs like "s3://".
    return bool(parsed.netloc)


# ---------------------------------------------------------------------------
# Internal validators — construct delta-rs objects to fail fast at startup
# ---------------------------------------------------------------------------


def _validate_writer(writer: dict[str, Any], *, context: str) -> None:
    if not writer:
        return
    try:
        WriterProperties(**writer)
    except TypeError as exc:
        raise TypeError(f"Invalid key in storage.{context}: {exc}") from exc


def _validate_commit(commit: dict[str, Any], *, context: str) -> None:
    if not commit:
        return
    try:
        CommitProperties(**commit)
    except TypeError as exc:
        raise TypeError(f"Invalid key in storage.{context}: {exc}") from exc
