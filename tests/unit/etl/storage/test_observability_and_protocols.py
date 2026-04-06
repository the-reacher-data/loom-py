"""Tests for observability config structs and storage I/O protocols."""

from __future__ import annotations

from typing import Any

import msgspec

from loom.etl.io.source import SourceSpec, TableSourceSpec
from loom.etl.io.target._table import ReplaceSpec
from loom.etl.schema._schema import ColumnSchema, LoomDtype
from loom.etl.schema._table import TableRef
from loom.etl.storage._io import SourceReader, TableDiscovery, TargetWriter
from loom.etl.storage._observability import ObservabilityConfig, RunSinkConfig


def test_observability_config_defaults_and_conversion() -> None:
    default = ObservabilityConfig()
    assert default.log is True
    assert default.run_sink is None
    assert default.slow_step_threshold_ms is None

    cfg = msgspec.convert(
        {
            "log": False,
            "run_sink": {"root": "s3://lake/runs", "storage_options": {"AWS_REGION": "eu-west-1"}},
            "slow_step_threshold_ms": 2500,
        },
        ObservabilityConfig,
    )
    assert cfg.log is False
    assert cfg.slow_step_threshold_ms == 2500
    assert cfg.run_sink == RunSinkConfig(
        root="s3://lake/runs", storage_options={"AWS_REGION": "eu-west-1"}
    )


def test_protocol_method_bodies_are_callable() -> None:
    src_spec = TableSourceSpec(alias="orders", table_ref=TableRef("raw.orders"))
    target_spec = ReplaceSpec(table_ref=TableRef("staging.out"))
    schema = (ColumnSchema("id", LoomDtype.INT64),)

    assert TableDiscovery.exists(object(), TableRef("raw.orders")) is None
    assert TableDiscovery.columns(object(), TableRef("raw.orders")) is None
    assert TableDiscovery.schema(object(), TableRef("raw.orders")) is None
    assert TableDiscovery.update_schema(object(), TableRef("raw.orders"), schema) is None
    assert SourceReader.read(object(), src_spec, None) is None
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


class _WriterImpl:
    def write(
        self, frame: Any, spec: object, params_instance: Any, /, *, streaming: bool = False
    ) -> None:
        return None


def test_runtime_protocol_checks_for_concrete_impls() -> None:
    assert isinstance(_CatalogImpl(), TableDiscovery)
    assert isinstance(_ReaderImpl(), SourceReader)
    assert isinstance(_WriterImpl(), TargetWriter)
