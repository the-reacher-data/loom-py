"""Unit tests for route resolver and write planner."""

from __future__ import annotations

from loom.etl.io.target import SchemaMode
from loom.etl.io.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpsertSpec,
)
from loom.etl.schema._schema import ColumnSchema, LoomDtype
from loom.etl.schema._table import TableRef, col
from loom.etl.storage._locator import PrefixLocator, TableLocation
from loom.etl.storage.route import (
    CatalogRouteResolver,
    CatalogTarget,
    CompositeRouteResolver,
    FixedCatalogRouteResolver,
    FixedPathRouteResolver,
    PathRouteResolver,
    PathTarget,
)
from loom.etl.storage.schema import PhysicalSchema
from loom.etl.storage.write import (
    AppendOp,
    ReplaceOp,
    ReplacePartitionsOp,
    ReplaceWhereOp,
    UpsertOp,
    WritePlanner,
)


class _StubSchemaReader:
    def __init__(self, schema: PhysicalSchema | None) -> None:
        self._schema = schema
        self.calls = 0

    def read_schema(self, _target: object) -> PhysicalSchema | None:
        self.calls += 1
        return self._schema


def test_catalog_route_resolver_qualifies_two_part_ref_with_default_catalog() -> None:
    resolver = CatalogRouteResolver(default_catalog="main")
    resolved = resolver.resolve(TableRef("raw.orders"))
    assert resolved.catalog_ref.ref == "main.raw.orders"


def test_path_route_resolver_resolves_locator() -> None:
    resolver = PathRouteResolver(PrefixLocator("s3://lake"))
    resolved = resolver.resolve(TableRef("raw.orders"))
    assert resolved.location.uri == "s3://lake/raw/orders"


def test_composite_route_resolver_applies_override_then_default() -> None:
    resolver = CompositeRouteResolver(
        default=CatalogRouteResolver(default_catalog="main"),
        overrides={
            "raw.orders": FixedPathRouteResolver(
                TableLocation(uri="s3://raw/orders"),
            ),
            "sys.customers": FixedCatalogRouteResolver(TableRef("finance.crm.customers")),
        },
    )
    path_target = resolver.resolve(TableRef("raw.orders"))
    catalog_target = resolver.resolve(TableRef("sys.customers"))
    default_target = resolver.resolve(TableRef("staging.out"))

    assert isinstance(path_target, PathTarget)
    assert isinstance(catalog_target, CatalogTarget)
    assert isinstance(default_target, CatalogTarget)
    assert path_target.location.uri == "s3://raw/orders"
    assert catalog_target.catalog_ref.ref == "finance.crm.customers"
    assert default_target.catalog_ref.ref == "main.staging.out"


def test_write_planner_reads_schema_once_and_builds_append_op() -> None:
    schema = PhysicalSchema(columns=(ColumnSchema("id", LoomDtype.INT64),))
    schema_reader = _StubSchemaReader(schema)
    planner = WritePlanner(
        resolver=CatalogRouteResolver(),
        schema_reader=schema_reader,
    )
    op = planner.plan(AppendSpec(table_ref=TableRef("raw.orders")), streaming=True)
    assert isinstance(op, AppendOp)
    assert op.streaming is True
    assert op.existing_schema == schema
    assert schema_reader.calls == 1


def test_write_planner_builds_replace_variants() -> None:
    planner = WritePlanner(
        resolver=CatalogRouteResolver(),
        schema_reader=_StubSchemaReader(None),
    )
    replace_op = planner.plan(
        ReplaceSpec(table_ref=TableRef("staging.out"), schema_mode=SchemaMode.OVERWRITE)
    )
    parts_op = planner.plan(
        ReplacePartitionsSpec(
            table_ref=TableRef("staging.out"),
            partition_cols=("year", "month"),
            schema_mode=SchemaMode.EVOLVE,
        )
    )
    where_op = planner.plan(
        ReplaceWhereSpec(
            table_ref=TableRef("staging.out"),
            replace_predicate=col("year") == 2026,
            schema_mode=SchemaMode.STRICT,
        ),
        streaming=True,
    )

    assert isinstance(replace_op, ReplaceOp)
    assert replace_op.schema_mode is SchemaMode.OVERWRITE
    assert isinstance(parts_op, ReplacePartitionsOp)
    assert parts_op.partition_cols == ("year", "month")
    assert isinstance(where_op, ReplaceWhereOp)
    assert where_op.streaming is True


def test_write_planner_builds_upsert_op() -> None:
    planner = WritePlanner(
        resolver=CatalogRouteResolver(),
        schema_reader=_StubSchemaReader(None),
    )
    op = planner.plan(
        UpsertSpec(
            table_ref=TableRef("staging.out"),
            upsert_keys=("id",),
            partition_cols=("year",),
            upsert_exclude=("created_at",),
            schema_mode=SchemaMode.EVOLVE,
        )
    )
    assert isinstance(op, UpsertOp)
    assert op.upsert_keys == ("id",)
    assert op.partition_cols == ("year",)
    assert op.upsert_exclude == ("created_at",)
    assert op.schema_mode is SchemaMode.EVOLVE
