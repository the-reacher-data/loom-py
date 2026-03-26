"""Unit tests for DeltaConfig, UnityCatalogConfig, and convert_storage_config.

Covers the programmatic construction path — no YAML, no dict — as the
canonical typed entry point for storage configuration.
"""

from __future__ import annotations

import msgspec
import pytest

from loom.etl._storage_config import (
    DeltaConfig,
    StorageBackend,
    TableOverride,
    UnityCatalogConfig,
    convert_storage_config,
)

# ---------------------------------------------------------------------------
# DeltaConfig — direct construction
# ---------------------------------------------------------------------------


def test_delta_config_defaults() -> None:
    config = DeltaConfig()
    assert config.type == "delta"
    assert config.root == ""
    assert config.storage_options == {}
    assert config.writer == {}
    assert config.delta_config == {}
    assert config.commit == {}
    assert config.tables == {}
    assert config.tmp_root == ""
    assert config.tmp_storage_options == {}


def test_delta_config_with_root() -> None:
    config = DeltaConfig(root="s3://my-lake/")
    assert config.root == "s3://my-lake/"


def test_delta_config_with_storage_options() -> None:
    config = DeltaConfig(
        root="s3://my-lake/",
        storage_options={"AWS_REGION": "eu-west-1"},
    )
    assert config.storage_options["AWS_REGION"] == "eu-west-1"


def test_delta_config_with_writer_options() -> None:
    config = DeltaConfig(writer={"compression": "SNAPPY"})
    assert config.writer["compression"] == "SNAPPY"


def test_delta_config_with_tmp_root() -> None:
    config = DeltaConfig(root="s3://lake/", tmp_root="s3://lake/tmp/")
    assert config.tmp_root == "s3://lake/tmp/"


def test_delta_config_frozen() -> None:
    config = DeltaConfig(root="s3://lake/")
    with pytest.raises((TypeError, AttributeError)):
        config.root = "s3://other/"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# DeltaConfig — config evolution via msgspec.structs.replace
# ---------------------------------------------------------------------------


def test_delta_config_replace_root() -> None:
    base = DeltaConfig(root="s3://prod/", writer={"compression": "SNAPPY"})
    staging = msgspec.structs.replace(base, root="s3://staging/")

    assert staging.root == "s3://staging/"
    assert staging.writer == {"compression": "SNAPPY"}  # preserved


def test_delta_config_replace_writer() -> None:
    base = DeltaConfig(root="s3://lake/", writer={"compression": "SNAPPY"})
    prod = msgspec.structs.replace(base, writer={"compression": "ZSTD", "compression_level": 3})

    assert prod.root == "s3://lake/"
    assert prod.writer["compression"] == "ZSTD"
    assert base.writer["compression"] == "SNAPPY"  # original unchanged


def test_delta_config_replace_storage_options() -> None:
    base = DeltaConfig(root="s3://lake/", storage_options={"AWS_REGION": "eu-west-1"})
    other_region = msgspec.structs.replace(base, storage_options={"AWS_REGION": "us-east-1"})

    assert other_region.storage_options["AWS_REGION"] == "us-east-1"
    assert base.storage_options["AWS_REGION"] == "eu-west-1"  # original unchanged


# ---------------------------------------------------------------------------
# DeltaConfig — validate()
# ---------------------------------------------------------------------------


def test_delta_config_validate_empty_writer_passes() -> None:
    DeltaConfig().validate()  # must not raise


def test_delta_config_validate_bad_writer_key_raises() -> None:
    config = DeltaConfig(writer={"nonexistent_key": "value"})
    with pytest.raises(TypeError, match="writer"):
        config.validate()


def test_delta_config_validate_bad_commit_key_raises() -> None:
    config = DeltaConfig(commit={"nonexistent_commit_key": "value"})
    with pytest.raises(TypeError, match="commit"):
        config.validate()


def test_delta_config_validate_bad_table_override_writer_raises() -> None:
    config = DeltaConfig(
        root="s3://lake/",
        tables={"raw.orders": TableOverride(writer={"bad_key": "x"})},
    )
    with pytest.raises(TypeError, match="tables.raw.orders.writer"):
        config.validate()


# ---------------------------------------------------------------------------
# UnityCatalogConfig — direct construction
# ---------------------------------------------------------------------------


def test_unity_catalog_config_minimal() -> None:
    config = UnityCatalogConfig(type="unity_catalog")
    assert config.type == "unity_catalog"
    assert config.workspace_url == ""
    assert config.catalog == ""
    assert config.token == ""
    assert config.tables == {}


def test_unity_catalog_config_explicit_fields() -> None:
    config = UnityCatalogConfig(
        type="unity_catalog",
        workspace_url="https://my-workspace.azuredatabricks.net",
        catalog="my_catalog",
        token="dapi-xxx",
    )
    assert config.workspace_url == "https://my-workspace.azuredatabricks.net"
    assert config.catalog == "my_catalog"
    assert config.token == "dapi-xxx"


def test_unity_catalog_config_replace_catalog() -> None:
    base = UnityCatalogConfig(type="unity_catalog", catalog="prod")
    dev = msgspec.structs.replace(base, catalog="dev")

    assert dev.catalog == "dev"
    assert base.catalog == "prod"  # original unchanged


def test_unity_catalog_config_frozen() -> None:
    config = UnityCatalogConfig(type="unity_catalog")
    with pytest.raises((TypeError, AttributeError)):
        config.catalog = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# convert_storage_config — dispatch
# ---------------------------------------------------------------------------


def test_convert_defaults_to_delta_when_no_type() -> None:
    config = convert_storage_config({"root": "s3://my-lake/"})
    assert isinstance(config, DeltaConfig)
    assert config.root == "s3://my-lake/"


def test_convert_explicit_delta_type() -> None:
    config = convert_storage_config({"type": "delta", "root": "s3://lake/"})
    assert isinstance(config, DeltaConfig)


def test_convert_unity_catalog_type() -> None:
    config = convert_storage_config({"type": "unity_catalog"})
    assert isinstance(config, UnityCatalogConfig)


def test_convert_unknown_type_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Unknown storage backend"):
        convert_storage_config({"type": "unknown_backend"})


def test_convert_invalid_delta_fields_raises_validation_error() -> None:
    with pytest.raises(msgspec.ValidationError):
        convert_storage_config({"root": 123})  # root must be str


def test_convert_empty_dict_produces_default_delta_config() -> None:
    config = convert_storage_config({})
    assert isinstance(config, DeltaConfig)
    assert config.root == ""


# ---------------------------------------------------------------------------
# StorageBackend — enum values
# ---------------------------------------------------------------------------


def test_storage_backend_values() -> None:
    assert StorageBackend.DELTA == "delta"
    assert StorageBackend.UNITY_CATALOG == "unity_catalog"


def test_storage_backend_comparison_with_config_type() -> None:
    delta = DeltaConfig()
    uc = UnityCatalogConfig(type="unity_catalog")

    assert delta.type == StorageBackend.DELTA
    assert uc.type == StorageBackend.UNITY_CATALOG


# ---------------------------------------------------------------------------
# DeltaConfig — to_locator()
# ---------------------------------------------------------------------------


def test_delta_config_to_locator_no_tables_returns_prefix_locator() -> None:
    from loom.etl._locator import PrefixLocator

    config = DeltaConfig(root="s3://lake/")
    locator = config.to_locator()
    assert isinstance(locator, PrefixLocator)


def test_delta_config_to_locator_with_tables_returns_mapping_locator() -> None:
    from loom.etl._locator import MappingLocator

    config = DeltaConfig(
        root="s3://lake/",
        tables={"raw.orders": TableOverride(root="s3://raw-account/raw/")},
    )
    locator = config.to_locator()
    assert isinstance(locator, MappingLocator)
