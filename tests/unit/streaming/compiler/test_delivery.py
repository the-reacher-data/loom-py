"""Delivery-semantics validation: DELIVERY_CONFLICT detection and compilation."""

from __future__ import annotations

from typing import Any

import pytest
from omegaconf import DictConfig, OmegaConf

from loom.core.config import ConfigContext
from loom.streaming import Drain, Fork, ForkRoute, FromTopic, IntoTopic, Process, StreamFlow, msg
from loom.streaming.compiler import CompilationError, StreamingErrorCode, compile_flow
from loom.streaming.compiler.phases.validate import validate_delivery
from tests.unit.streaming.compiler.cases import FakeStep, Order, Result

_BROKER = "localhost:9092"


def _flow() -> StreamFlow[Order, Result]:
    return StreamFlow(
        name="test",
        source=FromTopic("in", payload=Order),
        process=Process(FakeStep(), IntoTopic("out", payload=Result)),
    )


def _kafka_config(consumer_overrides: dict[str, object]) -> dict[str, object]:
    return {
        "kafka": {
            "consumer": {
                "brokers": [_BROKER],
                "group_id": "test",
                "topics": ["orders.raw"],
                **consumer_overrides,
            },
            "producer": {"brokers": [_BROKER], "topic": "orders.out"},
        }
    }


class TestValidateDelivery:
    @pytest.mark.parametrize(
        ("delivery", "enable_auto_commit"),
        [
            ("at_least_once", True),
            ("at_most_once", False),
        ],
    )
    def test_conflicting_fields_emit_delivery_conflict(
        self,
        delivery: str,
        enable_auto_commit: bool,
    ) -> None:
        ctx = ConfigContext.from_dict(
            _kafka_config({"delivery": delivery, "enable_auto_commit": enable_auto_commit})
        )

        issues = validate_delivery(_flow(), ctx)

        assert len(issues) == 1
        issue = issues[0]
        assert issue.code is StreamingErrorCode.DELIVERY_CONFLICT
        assert issue.field == "kafka.consumer.enable_auto_commit"
        assert delivery in issue.message
        assert "enable_auto_commit" in issue.message

    @pytest.mark.parametrize(
        "consumer_overrides",
        [
            {},
            {"delivery": "at_least_once"},
            {"delivery": "at_most_once"},
            {"enable_auto_commit": True},
            {"enable_auto_commit": False},
            {"delivery": "at_least_once", "enable_auto_commit": False},
            {"delivery": "at_most_once", "enable_auto_commit": True},
        ],
    )
    def test_consistent_or_partial_fields_pass(
        self,
        consumer_overrides: dict[str, object],
    ) -> None:
        ctx = ConfigContext.from_dict(_kafka_config(consumer_overrides))

        assert validate_delivery(_flow(), ctx) == []

    def test_missing_kafka_section_is_not_reported_here(self) -> None:
        assert validate_delivery(_flow(), ConfigContext.from_dict({})) == []


class TestCompileFlowDeliveryConflict:
    def test_compile_flow_raises_with_delivery_conflict_code(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        config = OmegaConf.merge(
            streaming_kafka_config,
            {"kafka": {"consumer": {"delivery": "at_least_once", "enable_auto_commit": True}}},
        )

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(_flow(), config=config)

        codes = {issue.code for issue in exc_info.value.issues}
        assert StreamingErrorCode.DELIVERY_CONFLICT in codes


class TestUnroutedForkUnderAtLeastOnce:
    def _flow_with_fork(self, default: Process[Any, Any] | None) -> StreamFlow[Order, Result]:
        return StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                Fork.when(
                    routes=[
                        ForkRoute(
                            when=msg.payload.order_id == "vip",
                            process=Process(IntoTopic("out", payload=Order)),
                        )
                    ],
                    default=default,
                )
            ),
        )

    def test_fork_without_default_is_rejected_under_at_least_once(
        self, streaming_kafka_config: DictConfig
    ) -> None:
        config = OmegaConf.merge(
            streaming_kafka_config,
            {"kafka": {"consumer": {"delivery": "at_least_once"}}},
        )

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(self._flow_with_fork(default=None), config=config)

        codes = {issue.code for issue in exc_info.value.issues}
        assert StreamingErrorCode.FORK_UNMATCHED_UNROUTED in codes

    def test_fork_with_default_passes_under_at_least_once(
        self, streaming_kafka_config: DictConfig
    ) -> None:
        config = OmegaConf.merge(
            streaming_kafka_config,
            {"kafka": {"consumer": {"delivery": "at_least_once"}}},
        )

        plan = compile_flow(self._flow_with_fork(default=Process(Drain())), config=config)

        assert plan.name == "test"

    def test_fork_without_default_is_allowed_under_at_most_once(
        self, streaming_kafka_config: DictConfig
    ) -> None:
        plan = compile_flow(self._flow_with_fork(default=None), config=streaming_kafka_config)

        assert plan.name == "test"
