"""Unit tests for DeltaConfig, UnityCatalogConfig and convert_storage_config."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import msgspec
import pytest

from loom.etl._locator import MappingLocator, PrefixLocator
from loom.etl._storage_config import (
    DeltaConfig,
    StorageBackend,
    TableOverride,
    UnityCatalogConfig,
    convert_storage_config,
)


class TestDeltaConfig:
    def test_defaults(self) -> None:
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

    @pytest.mark.parametrize(
        "kwargs,check",
        [
            (
                {"root": "s3://my-lake/"},
                lambda c: c.root == "s3://my-lake/",
            ),
            (
                {"root": "s3://my-lake/", "storage_options": {"AWS_REGION": "eu-west-1"}},
                lambda c: c.storage_options["AWS_REGION"] == "eu-west-1",
            ),
            (
                {"writer": {"compression": "SNAPPY"}},
                lambda c: c.writer["compression"] == "SNAPPY",
            ),
            (
                {"root": "s3://lake/", "tmp_root": "s3://lake/tmp/"},
                lambda c: c.tmp_root == "s3://lake/tmp/",
            ),
        ],
    )
    def test_direct_construction(
        self,
        kwargs: dict[str, Any],
        check: Callable[[DeltaConfig], bool],
    ) -> None:
        assert check(DeltaConfig(**kwargs))

    def test_is_frozen(self) -> None:
        config = DeltaConfig(root="s3://lake/")
        with pytest.raises((TypeError, AttributeError)):
            config.root = "s3://other/"  # type: ignore[misc]

    @pytest.mark.parametrize(
        "base,replace,check",
        [
            (
                DeltaConfig(root="s3://prod/", writer={"compression": "SNAPPY"}),
                {"root": "s3://staging/"},
                lambda new, old: new.root == "s3://staging/" and new.writer == old.writer,
            ),
            (
                DeltaConfig(root="s3://lake/", writer={"compression": "SNAPPY"}),
                {"writer": {"compression": "ZSTD", "compression_level": 3}},
                lambda new, old: (
                    new.writer["compression"] == "ZSTD" and old.writer["compression"] == "SNAPPY"
                ),
            ),
            (
                DeltaConfig(root="s3://lake/", storage_options={"AWS_REGION": "eu-west-1"}),
                {"storage_options": {"AWS_REGION": "us-east-1"}},
                lambda new, old: (
                    new.storage_options["AWS_REGION"] == "us-east-1"
                    and old.storage_options["AWS_REGION"] == "eu-west-1"
                ),
            ),
        ],
    )
    def test_replace(
        self,
        base: DeltaConfig,
        replace: dict[str, Any],
        check: Callable[[DeltaConfig, DeltaConfig], bool],
    ) -> None:
        updated = msgspec.structs.replace(base, **replace)
        assert check(updated, base)

    @pytest.mark.parametrize(
        "config,error",
        [
            (DeltaConfig(writer={"nonexistent_key": "value"}), "writer"),
            (DeltaConfig(commit={"nonexistent_commit_key": "value"}), "commit"),
            (
                DeltaConfig(
                    root="s3://lake/",
                    tables={"raw.orders": TableOverride(writer={"bad_key": "x"})},
                ),
                "tables.raw.orders.writer",
            ),
        ],
    )
    def test_validate_invalid_options_raise(self, config: DeltaConfig, error: str) -> None:
        with pytest.raises(TypeError, match=error):
            config.validate()

    def test_validate_empty_writer_passes(self) -> None:
        DeltaConfig().validate()

    @pytest.mark.parametrize(
        "config,expected_type",
        [
            (DeltaConfig(root="s3://lake/"), PrefixLocator),
            (
                DeltaConfig(
                    root="s3://lake/",
                    tables={"raw.orders": TableOverride(root="s3://raw-account/raw/")},
                ),
                MappingLocator,
            ),
        ],
    )
    def test_to_locator(self, config: DeltaConfig, expected_type: type[object]) -> None:
        assert isinstance(config.to_locator(), expected_type)


class TestUnityCatalogConfig:
    def test_minimal_defaults(self) -> None:
        config = UnityCatalogConfig(type="unity_catalog")
        assert config.type == "unity_catalog"
        assert config.workspace_url == ""
        assert config.catalog == ""
        assert config.token == ""
        assert config.tables == {}

    def test_explicit_fields(self) -> None:
        config = UnityCatalogConfig(
            type="unity_catalog",
            workspace_url="https://my-workspace.azuredatabricks.net",
            catalog="my_catalog",
            token="dapi-xxx",
        )
        assert config.workspace_url == "https://my-workspace.azuredatabricks.net"
        assert config.catalog == "my_catalog"
        assert config.token == "dapi-xxx"

    def test_replace(self) -> None:
        base = UnityCatalogConfig(type="unity_catalog", catalog="prod")
        dev = msgspec.structs.replace(base, catalog="dev")
        assert dev.catalog == "dev"
        assert base.catalog == "prod"

    def test_is_frozen(self) -> None:
        config = UnityCatalogConfig(type="unity_catalog")
        with pytest.raises((TypeError, AttributeError)):
            config.catalog = "other"  # type: ignore[misc]


class TestConvertStorageConfig:
    @pytest.mark.parametrize(
        "raw,expected_type,extra_check",
        [
            (
                {"root": "s3://my-lake/"},
                DeltaConfig,
                lambda c: c.root == "s3://my-lake/",
            ),
            (
                {"type": "delta", "root": "s3://lake/"},
                DeltaConfig,
                lambda c: c.type == "delta",
            ),
            (
                {"type": "unity_catalog"},
                UnityCatalogConfig,
                lambda c: c.type == "unity_catalog",
            ),
            (
                {},
                DeltaConfig,
                lambda c: c.root == "",
            ),
        ],
    )
    def test_convert_valid_dispatch(
        self,
        raw: dict[str, Any],
        expected_type: type[DeltaConfig | UnityCatalogConfig],
        extra_check: Callable[[DeltaConfig | UnityCatalogConfig], bool],
    ) -> None:
        config = convert_storage_config(raw)
        assert isinstance(config, expected_type)
        assert extra_check(config)

    @pytest.mark.parametrize(
        "raw,exc,error",
        [
            ({"type": "unknown_backend"}, ValueError, "Unknown storage backend"),
            ({"root": 123}, msgspec.ValidationError, ""),
        ],
    )
    def test_convert_errors(
        self,
        raw: dict[str, Any],
        exc: type[Exception],
        error: str,
    ) -> None:
        if error:
            with pytest.raises(exc, match=error):
                convert_storage_config(raw)
            return
        with pytest.raises(exc):
            convert_storage_config(raw)


class TestStorageBackendEnum:
    def test_values_and_comparison(self) -> None:
        delta = DeltaConfig()
        uc = UnityCatalogConfig(type="unity_catalog")
        assert StorageBackend.DELTA.value == "delta"
        assert StorageBackend.UNITY_CATALOG.value == "unity_catalog"
        assert delta.type == StorageBackend.DELTA.value
        assert uc.type == StorageBackend.UNITY_CATALOG.value
