"""Compiler validation for With / WithAsync resource scoping."""

from __future__ import annotations

import pytest
from omegaconf import DictConfig, OmegaConf

from loom.streaming import (
    ContextFactory,
    FromTopic,
    IntoTopic,
    Process,
    ResourceScope,
    StreamFlow,
    With,
)
from loom.streaming.compiler import CompilationError, compile_flow
from tests.unit.streaming.compiler.cases import FakeStep, Order, Result


class TestWithCompiler:
    def test_compile_fails_on_batch_scope_with_direct_context_manager(
        self,
    ) -> None:
        """BATCH scope with a direct CM instance must be rejected at compile time."""

        class _FakeSyncCM:
            def __enter__(self) -> _FakeSyncCM:
                return self

            def __exit__(self, *args: object) -> None:
                return None

        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                With(
                    process=Process(FakeStep(), IntoTopic("out", payload=Result)),
                    scope=ResourceScope.BATCH,
                    db=_FakeSyncCM(),
                )
            ),
        )

        with pytest.raises(CompilationError, match="ContextFactory"):
            compile_flow(flow, runtime_config=OmegaConf.create({}))

    def test_compile_succeeds_on_batch_scope_with_context_factory(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        """BATCH scope with a ContextFactory is valid."""

        class _FakeSyncCM:
            def __enter__(self) -> _FakeSyncCM:
                return self

            def __exit__(self, *args: object) -> None:
                return None

        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                With(
                    process=Process(FakeStep(), IntoTopic("out", payload=Result)),
                    scope=ResourceScope.BATCH,
                    db=ContextFactory(lambda: _FakeSyncCM()),
                )
            ),
        )

        plan = compile_flow(flow, runtime_config=streaming_kafka_config)

        assert plan.name == "test"
