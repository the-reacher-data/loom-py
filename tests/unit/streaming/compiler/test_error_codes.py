"""Structured compilation issues: codes, components, and aggregation contract."""

from __future__ import annotations

import pytest
from omegaconf import DictConfig, OmegaConf

from loom.streaming import (
    Fork,
    ForkRoute,
    FromTopic,
    IntoTopic,
    Process,
    StreamFlow,
    StreamShape,
    msg,
)
from loom.streaming.compiler import (
    CompilationError,
    CompilationIssue,
    StreamingErrorCode,
    compile_flow,
)
from loom.streaming.compiler.phases.build_plan import (
    _build_dispatch_table,
    _build_storage_sink,
)
from tests.unit.streaming.compiler.cases import FakeStep, Order, Result


class TestCompilationErrorContract:
    def test_issues_carry_code_component_and_message(
        self, streaming_kafka_config: DictConfig
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order, shape=StreamShape.BATCH),
            process=Process(FakeStep(), IntoTopic("out", payload=Result)),
        )

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(flow, config=streaming_kafka_config)

        codes = [issue.code for issue in exc_info.value.issues]
        assert StreamingErrorCode.SHAPE_MISMATCH in codes
        mismatch = next(
            issue
            for issue in exc_info.value.issues
            if issue.code is StreamingErrorCode.SHAPE_MISMATCH
        )
        assert mismatch.component == "FakeStep"
        assert "shape mismatch" in mismatch.message

    def test_errors_property_mirrors_issue_messages(
        self, streaming_kafka_config: DictConfig
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(FakeStep()),
        )

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(flow, config=streaming_kafka_config)

        exc = exc_info.value
        assert exc.errors == [issue.message for issue in exc.issues]
        assert str(exc).startswith(f"Compilation failed with {len(exc.issues)} error(s):")

    def test_missing_terminal_output_code_and_field(
        self, streaming_kafka_config: DictConfig
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(FakeStep()),
        )

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(flow, config=streaming_kafka_config)

        issue = next(
            issue
            for issue in exc_info.value.issues
            if issue.code is StreamingErrorCode.MISSING_TERMINAL_OUTPUT
        )
        assert issue.field == "output"

    def test_kafka_config_invalid_code(self) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(FakeStep(), IntoTopic("out", payload=Result)),
        )

        empty_config = OmegaConf.create({})

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(flow, config=empty_config)

        codes = {issue.code for issue in exc_info.value.issues}
        assert StreamingErrorCode.KAFKA_CONFIG_INVALID in codes

    def test_constructor_normalizes_bare_strings(self) -> None:
        exc = CompilationError(["legacy message"])

        assert exc.errors == ["legacy message"]
        assert exc.issues[0].code is StreamingErrorCode.UNSPECIFIED
        assert exc.issues[0].message == "legacy message"


class TestBranchPrefixing:
    def test_fork_branch_issue_keeps_code_and_prefixes_message(
        self, streaming_kafka_config: DictConfig
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                Fork.when(
                    routes=[
                        ForkRoute(
                            when=msg.payload.amount >= 0,
                            process=Process(FakeStep()),
                        )
                    ],
                )
            ),
        )

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(flow, config=streaming_kafka_config)

        branch_issue = next(
            issue
            for issue in exc_info.value.issues
            if issue.code is StreamingErrorCode.FORK_BRANCH_NO_TERMINAL
        )
        assert branch_issue.message.startswith("fork branch ")

    def test_prefixed_scopes_message_and_component(self) -> None:
        issue = CompilationIssue(
            code=StreamingErrorCode.SHAPE_MISMATCH,
            message="shape mismatch: expected record but got batch before FakeStep",
            component="FakeStep",
        )

        scoped = issue.prefixed("router branch 'vip'")

        assert scoped.code is StreamingErrorCode.SHAPE_MISMATCH
        assert scoped.message == (
            "router branch 'vip': shape mismatch: expected record but got batch before FakeStep"
        )
        assert scoped.component == "router branch 'vip' > FakeStep"


class TestBuildPhaseCodes:
    def test_unsupported_storage_sink_raises_coded_error(self) -> None:
        class _NotATable:
            name = "fake"

        not_a_table = _NotATable()
        empty_config = OmegaConf.create({})

        with pytest.raises(CompilationError) as exc_info:
            _build_storage_sink(not_a_table, empty_config)  # type: ignore[arg-type]

        issue = exc_info.value.issues[0]
        assert issue.code is StreamingErrorCode.STORAGE_SINK_UNSUPPORTED
        assert "Unsupported storage sink: _NotATable" in issue.message

    def test_payload_without_message_type_raises_coded_error(self) -> None:
        class _PlainPayload:
            pass

        with pytest.raises(CompilationError) as exc_info:
            _build_dispatch_table((_PlainPayload,))

        issue = exc_info.value.issues[0]
        assert issue.code is StreamingErrorCode.PAYLOAD_TYPE_INVALID
        assert issue.field == "payloads"
