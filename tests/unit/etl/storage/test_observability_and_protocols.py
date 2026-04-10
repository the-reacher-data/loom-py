"""Tests for observability config structs and storage I/O protocols."""

from __future__ import annotations

from typing import Any

import msgspec

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.source import SourceSpec, TableSourceSpec
from loom.etl.declarative.target._table import ReplaceSpec
from loom.etl.observability.config import ExecutionRecordStoreConfig, ObservabilityConfig
from loom.etl.runtime.contracts import SourceReader, TableDiscovery, TargetWriter
from loom.etl.schema._schema import ColumnSchema, LoomDtype


def test_observability_config_defaults_and_conversion() -> None:
    default = ObservabilityConfig()
    assert default.log is True
    assert default.record_store is None
    assert default.slow_step_threshold_ms is None

    cfg = msgspec.convert(
        {
            "log": False,
            "record_store": {
                "root": "s3://lake/runs",
                "storage_options": {"AWS_REGION": "eu-west-1"},
                "writer": {"compression": "SNAPPY"},
                "delta_config": {"delta.appendOnly": "true"},
                "commit": {"userName": "loom"},
            },
            "slow_step_threshold_ms": 2500,
        },
        ObservabilityConfig,
    )
    assert cfg.log is False
    assert cfg.slow_step_threshold_ms == 2500
    assert cfg.record_store == ExecutionRecordStoreConfig(
        root="s3://lake/runs",
        storage_options={"AWS_REGION": "eu-west-1"},
        writer={"compression": "SNAPPY"},
        delta_config={"delta.appendOnly": "true"},
        commit={"userName": "loom"},
    )


def test_record_store_config_validate_requires_exactly_one_destination() -> None:
    ExecutionRecordStoreConfig(root="s3://lake/runs").validate()
    ExecutionRecordStoreConfig(database="ops").validate()

    try:
        ExecutionRecordStoreConfig().validate()
    except ValueError as exc:
        assert "exactly one destination" in str(exc)
    else:  # pragma: no cover - defensive branch
        raise AssertionError("Expected ValueError for empty destination")

    try:
        ExecutionRecordStoreConfig(root="s3://lake/runs", database="ops").validate()
    except ValueError as exc:
        assert "exactly one destination" in str(exc)
    else:  # pragma: no cover - defensive branch
        raise AssertionError("Expected ValueError for conflicting destinations")


def test_protocol_method_bodies_are_callable() -> None:
    src_spec = TableSourceSpec(alias="orders", table_ref=TableRef("raw.orders"))
    target_spec = ReplaceSpec(table_ref=TableRef("staging.out"))
    schema = (ColumnSchema("id", LoomDtype.INT64),)

    assert TableDiscovery.exists(object(), TableRef("raw.orders")) is None
    assert TableDiscovery.columns(object(), TableRef("raw.orders")) is None
    assert TableDiscovery.schema(object(), TableRef("raw.orders")) is None
    assert TableDiscovery.update_schema(object(), TableRef("raw.orders"), schema) is None
    assert SourceReader.read(object(), src_spec, None) is None
    assert SourceReader.execute_sql(object(), {}, "SELECT 1") is None
    assert TargetWriter.write(object(), object(), target_spec, None) is None


class _CatalogImpl:
    def exists(self, ref: TableRef) -> bool:
        return ref.ref == "raw.orders"

    def columns(self, ref: TableRef) -> tuple[str, ...]:
        return ("id",)

    def schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        return (ColumnSchema("id", LoomDtype.INT64),)

    def update_schema(self, ref: TableRef, schema: tuple[ColumnSchema, ...]) -> None:
        return None


class _ReaderImpl:
    def read(self, spec: SourceSpec, params_instance: Any, /) -> Any:
        return {"alias": spec.alias, "params": params_instance}

    def execute_sql(self, frames: dict[str, Any], query: str, /) -> Any:
        return {"frames": frames, "query": query}


class _WriterImpl:
    def write(
        self, frame: Any, spec: object, params_instance: Any, /, *, streaming: bool = False
    ) -> None:
        return None


def test_runtime_protocol_checks_for_concrete_impls() -> None:
    assert isinstance(_CatalogImpl(), TableDiscovery)
    assert isinstance(_ReaderImpl(), SourceReader)
    assert isinstance(_WriterImpl(), TargetWriter)
