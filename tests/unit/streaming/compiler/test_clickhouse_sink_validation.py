"""A ClickHouse sink is validated at compile time, like every other backend."""

from __future__ import annotations

from typing import Any, cast

import pytest
from omegaconf import DictConfig, OmegaConf

from loom.core.config import ConfigContext
from loom.streaming import (
    Backend,
    FromTopic,
    IntoTable,
    Process,
    StreamFlow,
)
from loom.streaming.compiler import CompilationError, compile_flow

from .cases import Order, Result


def _flow() -> StreamFlow[Order, Result]:
    """A flow whose only destination is a ClickHouse table."""
    sink = IntoTable(
        payload=Result,
        table="results",
        backend=Backend.CLICKHOUSE,
        name="results_sink",
    )
    return StreamFlow(
        name="clickhouse_sink_flow",
        source=FromTopic("in", payload=Order),
        process=Process(cast(Any, sink)),
    )


def test_compila_cuando_la_base_de_datos_del_sink_esta_declarada(
    streaming_kafka_config: DictConfig,
) -> None:
    """With the sink and its database declared, compilation succeeds."""
    config = OmegaConf.merge(
        streaming_kafka_config,
        OmegaConf.create(
            {
                "database": {"clickhouse": {"url": "clickhouse://localhost:8123/default"}},
                "streaming": {
                    "sinks": {"results_sink": {"database": "clickhouse", "table": "results"}}
                },
            }
        ),
    )

    plan = compile_flow(_flow(), config=ConfigContext(cast(DictConfig, config)))

    assert plan.name == "clickhouse_sink_flow"


def test_falla_en_compilacion_cuando_falta_la_base_de_datos_del_sink(
    streaming_kafka_config: DictConfig,
) -> None:
    """A missing ``database`` section is a compilation issue, not a runtime crash."""
    config = OmegaConf.merge(
        streaming_kafka_config,
        OmegaConf.create(
            {"streaming": {"sinks": {"results_sink": {"database": "clickhouse", "table": "r"}}}}
        ),
    )

    with pytest.raises(CompilationError) as failure:
        compile_flow(_flow(), config=ConfigContext(cast(DictConfig, config)))

    assert any("results_sink" in issue.message for issue in failure.value.issues)
