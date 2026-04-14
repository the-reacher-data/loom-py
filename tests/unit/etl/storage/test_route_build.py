"""Unit tests for building table route resolvers from StorageConfig."""

from __future__ import annotations

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.storage._config import CatalogConnection, StorageConfig, TableRoute
from loom.etl.storage.routing import CatalogTarget, build_table_resolver


def test_build_table_resolver_qualifies_two_part_override_with_route_catalog() -> None:
    resolver = build_table_resolver(
        StorageConfig(
            engine="spark",
            catalogs={"unity": CatalogConnection()},
            tables=(TableRoute(name="raw.orders", ref="raw.orders", catalog="unity"),),
        )
    )
    target = resolver.resolve(TableRef("raw.orders"))

    assert isinstance(target, CatalogTarget)
    assert target.catalog_ref.ref == "unity.raw.orders"


def test_build_table_resolver_uses_default_catalog_for_two_part_refs() -> None:
    resolver = build_table_resolver(
        StorageConfig(engine="spark", catalogs={"default": CatalogConnection()})
    )
    target = resolver.resolve(TableRef("staging.out"))

    assert isinstance(target, CatalogTarget)
    assert target.catalog_ref.ref == "default.staging.out"
