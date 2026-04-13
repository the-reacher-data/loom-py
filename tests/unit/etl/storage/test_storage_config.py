"""Unit tests for StorageConfig and convert_storage_config."""

from __future__ import annotations

from pathlib import Path

import msgspec
import pytest

from loom.etl.storage._config import (
    CatalogConnection,
    FilePathConfig,
    FileRoute,
    MissingTablePolicy,
    StorageConfig,
    StorageDefaults,
    StorageEngine,
    TablePathConfig,
    TableRoute,
    convert_storage_config,
)
from loom.etl.storage._file_locator import FileLocation, MappingFileLocator
from loom.etl.storage._locator import MappingLocator, PrefixLocator


def _default_path(uri: str) -> StorageDefaults:
    return StorageDefaults(table_path=TablePathConfig(uri=uri))


class TestStorageConfig:
    def test_defaults(self) -> None:
        config = StorageConfig()
        assert config.engine == "polars"
        assert config.missing_table_policy == MissingTablePolicy.SCHEMA_MODE
        assert config.catalogs == {}
        assert config.defaults == StorageDefaults()
        assert config.tables == ()
        assert config.files == ()
        assert config.tmp_root == ""
        assert config.tmp_storage_options == {}

    def test_engine_enum_values(self) -> None:
        assert StorageEngine.POLARS.value == "polars"
        assert StorageEngine.SPARK.value == "spark"

    def test_validate_accepts_minimal_path_defaults(self) -> None:
        config = StorageConfig(defaults=_default_path("s3://my-lake"))
        config.validate()

    def test_validate_rejects_local_checkpoint_root(self, tmp_path: Path) -> None:
        local_path = tmp_path / "loom-checkpoints"
        config = StorageConfig(tmp_root=str(local_path))
        with pytest.raises(ValueError, match="cloud URI"):
            config.validate()

    @pytest.mark.parametrize("tmp_root", ["s3://", "gs://", "abfss://", "file:///tmp/loom"])
    def test_validate_rejects_invalid_checkpoint_cloud_uri(self, tmp_root: str) -> None:
        config = StorageConfig(tmp_root=tmp_root)
        with pytest.raises(ValueError, match="cloud URI"):
            config.validate()

    def test_validate_accepts_r2_checkpoint_root(self) -> None:
        config = StorageConfig(tmp_root="r2://my-bucket/checkpoints")
        config.validate()

    def test_validate_rejects_duplicate_table_names(self) -> None:
        config = StorageConfig(
            tables=(
                TableRoute(name="raw.orders", path=TablePathConfig(uri="s3://raw/orders")),
                TableRoute(name="raw.orders", path=TablePathConfig(uri="s3://raw/orders-v2")),
            )
        )
        with pytest.raises(ValueError, match="duplicate name"):
            config.validate()

    def test_validate_rejects_duplicate_file_names(self) -> None:
        config = StorageConfig(
            files=(
                FileRoute(name="daily.csv", path=FilePathConfig(uri="s3://a/daily.csv")),
                FileRoute(name="daily.csv", path=FilePathConfig(uri="s3://b/daily.csv")),
            )
        )
        with pytest.raises(ValueError, match="duplicate name"):
            config.validate()

    def test_validate_rejects_missing_route_destination(self) -> None:
        config = StorageConfig(tables=(TableRoute(name="raw.orders"),))
        with pytest.raises(ValueError, match="exactly one destination"):
            config.validate()

    def test_validate_rejects_route_with_both_ref_and_path(self) -> None:
        config = StorageConfig(
            tables=(
                TableRoute(
                    name="raw.orders",
                    ref="main.raw.orders",
                    path=TablePathConfig(uri="s3://raw/orders"),
                ),
            )
        )
        with pytest.raises(ValueError, match="exactly one destination"):
            config.validate()

    def test_validate_rejects_invalid_ref_shape(self) -> None:
        config = StorageConfig(tables=(TableRoute(name="raw.orders", ref="orders"),))
        with pytest.raises(ValueError, match="schema.table"):
            config.validate()

    def test_validate_rejects_unknown_catalog_key(self) -> None:
        config = StorageConfig(
            tables=(TableRoute(name="raw.orders", ref="raw.orders", catalog="finance"),)
        )
        with pytest.raises(ValueError, match="not defined in storage.catalogs"):
            config.validate()

    def test_validate_accepts_two_part_ref_with_catalog_key(self) -> None:
        config = StorageConfig(
            catalogs={
                "default": CatalogConnection(
                    workspace="https://dbc.example",
                    token="token-123",
                )
            },
            tables=(TableRoute(name="raw.orders", ref="raw.orders", catalog="default"),),
        )
        config.validate()

    def test_validate_accepts_three_part_ref_without_catalog_key(self) -> None:
        config = StorageConfig(
            engine="spark",
            tables=(TableRoute(name="raw.orders", ref="main.raw.orders"),),
        )
        config.validate()

    def test_validate_rejects_polars_three_part_ref_without_catalog_credentials(self) -> None:
        config = StorageConfig(tables=(TableRoute(name="raw.orders", ref="main.raw.orders"),))
        with pytest.raises(ValueError, match="Polars UC routes require credentials"):
            config.validate()

    def test_validate_rejects_polars_uc_catalog_with_empty_credentials(self) -> None:
        config = StorageConfig(
            catalogs={"main": CatalogConnection()},
            tables=(TableRoute(name="raw.orders", ref="main.raw.orders"),),
        )
        with pytest.raises(ValueError, match="non-empty workspace and token"):
            config.validate()

    def test_validate_accepts_polars_three_part_ref_with_catalog_alias(self) -> None:
        config = StorageConfig(
            catalogs={
                "unity": CatalogConnection(
                    workspace="https://dbc.example",
                    token="token-123",
                )
            },
            tables=(TableRoute(name="raw.orders", ref="main.raw.orders", catalog="unity"),),
        )
        config.validate()

    @pytest.mark.parametrize(
        "config,error",
        [
            (
                StorageConfig(defaults=_default_path("s3://lake"), tables=()),
                None,
            ),
            (
                StorageConfig(
                    defaults=StorageDefaults(
                        table_path=TablePathConfig(uri="s3://lake", writer={"bad": 1})
                    )
                ),
                "writer",
            ),
            (
                StorageConfig(
                    defaults=StorageDefaults(
                        table_path=TablePathConfig(uri="s3://lake", commit={"bad": 1})
                    )
                ),
                "commit",
            ),
        ],
    )
    def test_validate_writer_and_commit_options(
        self,
        config: StorageConfig,
        error: str | None,
    ) -> None:
        if error is None:
            config.validate()
            return
        with pytest.raises(TypeError, match=error):
            config.validate()

    def test_has_catalog_routes(self) -> None:
        config = StorageConfig(tables=(TableRoute(name="raw.orders", ref="main.raw.orders"),))
        assert config.has_catalog_routes() is True
        assert config.has_path_routes() is False

    def test_has_path_routes(self) -> None:
        config = StorageConfig(defaults=_default_path("s3://my-lake"))
        assert config.has_catalog_routes() is False
        assert config.has_path_routes() is True

    @pytest.mark.parametrize(
        "config,expected_type",
        [
            (StorageConfig(defaults=_default_path("s3://lake")), PrefixLocator),
            (
                StorageConfig(
                    defaults=_default_path("s3://default"),
                    tables=(
                        TableRoute(
                            name="raw.orders",
                            path=TablePathConfig(uri="s3://finance/raw/orders"),
                        ),
                    ),
                ),
                MappingLocator,
            ),
        ],
    )
    def test_to_path_locator(self, config: StorageConfig, expected_type: type[object]) -> None:
        assert isinstance(config.to_path_locator(), expected_type)

    def test_to_path_locator_without_path_routes_raises(self) -> None:
        config = StorageConfig()
        with pytest.raises(ValueError, match="no path routes configured"):
            config.to_path_locator()


class TestConvertStorageConfig:
    def test_convert_valid_shape(self) -> None:
        raw = {
            "engine": "polars",
            "defaults": {
                "table_path": {
                    "uri": "s3://my-lake",
                    "storage_options": {"AWS_REGION": "eu-west-1"},
                }
            },
            "tables": [
                {"name": "raw.orders", "path": {"uri": "s3://raw/orders"}},
                {"name": "sys.customers", "ref": "main.crm.customers"},
            ],
        }
        config = convert_storage_config(raw)
        assert isinstance(config, StorageConfig)
        assert config.engine == StorageEngine.POLARS.value
        assert len(config.tables) == 2

    def test_convert_invalid_engine_raises(self) -> None:
        with pytest.raises(msgspec.ValidationError):
            convert_storage_config({"engine": "unknown"})

    def test_convert_invalid_field_type_raises(self) -> None:
        with pytest.raises(msgspec.ValidationError):
            convert_storage_config({"tmp_storage_options": ["bad"]})


class TestToFileLocator:
    def test_returns_none_when_no_files(self) -> None:
        config = StorageConfig()
        assert config.to_file_locator() is None

    def test_returns_mapping_locator_with_files(self) -> None:
        config = StorageConfig(
            files=(
                FileRoute(
                    name="events_raw",
                    path=FilePathConfig(uri="s3://raw/events/"),
                ),
            )
        )
        locator = config.to_file_locator()
        assert isinstance(locator, MappingFileLocator)

    def test_locator_resolves_registered_alias(self) -> None:
        config = StorageConfig(
            files=(
                FileRoute(
                    name="events_raw",
                    path=FilePathConfig(
                        uri="s3://raw/events/",
                        storage_options={"AWS_REGION": "eu-west-1"},
                    ),
                ),
            )
        )
        locator = config.to_file_locator()
        assert locator is not None
        location = locator.locate("events_raw")
        assert location.uri_template == "s3://raw/events/"
        assert location.storage_options == {"AWS_REGION": "eu-west-1"}

    def test_locator_raises_on_unknown_alias(self) -> None:
        config = StorageConfig(
            files=(FileRoute(name="events_raw", path=FilePathConfig(uri="s3://raw/events/")),)
        )
        locator = config.to_file_locator()
        assert locator is not None
        with pytest.raises(KeyError, match="events_daily"):
            locator.locate("events_daily")


class TestMappingFileLocator:
    def test_locate_returns_correct_location(self) -> None:
        locator = MappingFileLocator(
            mapping={"events": FileLocation(uri_template="s3://raw/events/")}
        )
        loc = locator.locate("events")
        assert loc.uri_template == "s3://raw/events/"

    def test_locate_missing_alias_raises_key_error(self) -> None:
        locator = MappingFileLocator(
            mapping={"events": FileLocation(uri_template="s3://raw/events/")}
        )
        with pytest.raises(KeyError, match="reports"):
            locator.locate("reports")

    def test_error_message_lists_available_aliases(self) -> None:
        locator = MappingFileLocator(
            mapping={
                "events": FileLocation(uri_template="s3://raw/events/"),
                "exports": FileLocation(uri_template="s3://out/exports/"),
            }
        )
        with pytest.raises(KeyError, match="events"):
            locator.locate("missing")

    def test_storage_options_empty_by_default(self) -> None:
        loc = FileLocation(uri_template="s3://bucket/path/")
        assert loc.storage_options == {}

    def test_storage_options_preserved(self) -> None:
        loc = FileLocation(
            uri_template="s3://bucket/path/",
            storage_options={"AWS_REGION": "us-east-1"},
        )
        assert loc.storage_options == {"AWS_REGION": "us-east-1"}
