"""Unified ETL storage configuration.

Defines one canonical ``storage:`` model for both Spark and Polars runtimes.
Logical table names remain stable in pipelines while this config resolves each
name to either a catalog reference or a physical Delta path.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

import msgspec

from loom.etl.storage._locator import MappingLocator, PrefixLocator, TableLocation, TableLocator


class StorageEngine(StrEnum):
    """Execution engine guardrail declared in the storage config."""

    POLARS = "polars"
    SPARK = "spark"


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
        catalogs: Catalog connection map.
        defaults: Default path settings.
        tables: Per-logical-table routes.
        files: Per-logical-file routes.
        tmp_root: Root URI for intermediate storage.
        tmp_storage_options: Credentials/options for intermediate storage.
    """

    engine: Literal["polars", "spark"] = "polars"
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

    def has_catalog_routes(self) -> bool:
        """Return ``True`` when at least one table route uses ``ref``."""
        return any(bool(route.ref.strip()) for route in self.tables)

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


# ---------------------------------------------------------------------------
# Internal validators — construct delta-rs objects to fail fast at startup
# ---------------------------------------------------------------------------


def _validate_writer(writer: dict[str, Any], *, context: str) -> None:
    if not writer:
        return
    try:
        from deltalake.table import WriterProperties

        WriterProperties(**writer)
    except TypeError as exc:
        raise TypeError(f"Invalid key in storage.{context}: {exc}") from exc


def _validate_commit(commit: dict[str, Any], *, context: str) -> None:
    if not commit:
        return
    try:
        from deltalake import CommitProperties

        CommitProperties(**commit)
    except TypeError as exc:
        raise TypeError(f"Invalid key in storage.{context}: {exc}") from exc
