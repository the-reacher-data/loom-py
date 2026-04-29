"""Tests for resolving declarative config bindings in streaming flows."""

from __future__ import annotations

from contextlib import AbstractContextManager, nullcontext

from omegaconf import DictConfig, OmegaConf

from loom.streaming import (
    ContextFactory,
    FromTopic,
    IntoTopic,
    Message,
    Process,
    RecordStep,
    StreamFlow,
    With,
)
from loom.streaming.compiler import compile_flow
from tests.unit.streaming.compiler.cases import Order, Result


class _ConfiguredStep(RecordStep[Order, Result]):
    """Step whose constructor is resolved from YAML bindings."""

    def __init__(self, *, prefix: str, suffix: str = "") -> None:
        self.prefix = prefix
        self.suffix = suffix

    def execute(self, message: Message[Order], **kwargs: object) -> Result:
        del kwargs
        return Result(value=f"{self.prefix}:{message.payload.order_id}{self.suffix}")


class _DeclaredStep(RecordStep[Order, Result]):
    """Step declared as a class in the flow and instantiated by the compiler."""

    def execute(self, message: Message[Order], **kwargs: object) -> Result:
        del kwargs
        return Result(value=message.payload.order_id)


class _TokenFactory(ContextFactory):
    """Configured context factory resolved from YAML bindings."""

    __slots__ = ("token",)

    def __init__(self, *, token: str) -> None:
        self.token = token
        super().__init__(self._create)

    def _create(self) -> AbstractContextManager[str]:
        return nullcontext(self.token)


class TestConfigBindings:
    """Binding resolution for declarative streaming flows."""

    def test_compile_flow_instantiates_declared_step_classes(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        """Bare step classes should be materialized by the compiler."""
        runtime_config = streaming_kafka_config

        flow: StreamFlow[Order, Result] = StreamFlow(
            name="declared_step_flow",
            source=FromTopic("orders.raw", payload=Order),
            process=Process(
                _DeclaredStep,
                IntoTopic("orders.validated", payload=Result),
            ),
        )

        plan = compile_flow(flow, runtime_config=runtime_config)

        node = plan.nodes[0].node
        assert isinstance(node, _DeclaredStep)

    def test_compile_flow_resolves_step_and_context_factory_bindings(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        """Bindings should be materialized before the plan is compiled."""
        merged_config = OmegaConf.merge(
            streaming_kafka_config,
            OmegaConf.create(
                {
                    "streaming": {
                        "bindings": {
                            "step": {
                                "prefix": "cfg-prefix",
                                "suffix": "cfg-suffix",
                            },
                            "factory": {
                                "token": "cfg-token",
                            },
                        }
                    }
                }
            ),
        )
        runtime_config = OmegaConf.create(OmegaConf.to_container(merged_config, resolve=False))
        assert isinstance(runtime_config, DictConfig)

        flow: StreamFlow[Order, Result] = StreamFlow(
            name="binding_flow",
            source=FromTopic("orders.raw", payload=Order),
            process=Process(
                With(
                    process=Process(
                        _ConfiguredStep.from_config(
                            "streaming.bindings.step",
                            prefix="override-prefix",
                        ),
                        IntoTopic("orders.validated", payload=Result),
                    ),
                    token_factory=_TokenFactory.from_config("streaming.bindings.factory"),
                ),
            ),
        )

        plan = compile_flow(flow, runtime_config=runtime_config)

        scoped = plan.nodes[0].node
        assert isinstance(scoped, With)

        step = scoped.process.nodes[0]
        assert isinstance(step, _ConfiguredStep)
        assert step.prefix == "override-prefix"
        assert step.suffix == "cfg-suffix"

        factory = scoped.context_factories["token_factory"]
        assert isinstance(factory, _TokenFactory)
        assert factory.token == "cfg-token"
