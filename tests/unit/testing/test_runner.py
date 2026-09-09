from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import msgspec
import pytest

from loom.ai.abc import AgentHandle
from loom.core.command import Command
from loom.core.engine.plan import ExecutionPlan
from loom.core.errors import NotFound
from loom.core.identity import Identity
from loom.core.model import LoomStruct
from loom.core.use_case.markers import Agent, Caller, Input, LoadById
from loom.core.use_case.rule import RuleViolation, RuleViolations
from loom.core.use_case.use_case import UseCase
from loom.testing.runner import AgentHandleDouble, UseCaseTest

# ---------------------------------------------------------------------------
# Domain fixtures
# ---------------------------------------------------------------------------


class Cmd(Command, frozen=True):
    email: str
    name: str


class Entity:
    def __init__(self, name: str) -> None:
        self.name = name


class _SimpleUseCase(UseCase[Any, str]):
    async def execute(self, cmd: Cmd = Input()) -> str:
        return cmd.email


class _ParamOnlyUseCase(UseCase[Any, str]):
    async def execute(self, user_id: int) -> str:
        return f"id={user_id}"


class _ParamAndInputUseCase(UseCase[Any, str]):
    async def execute(
        self,
        tenant_id: str,
        cmd: Cmd = Input(),
    ) -> str:
        return f"{tenant_id}:{cmd.email}"


class _LoadUseCase(UseCase[Any, str]):
    async def execute(
        self,
        eid: int,
        entity: Entity = LoadById(Entity, by="eid"),
    ) -> str:
        return entity.name


def _always_fail_rule(
    command: Command,
    fields_set: frozenset[str],
    context: dict[str, object] | None = None,
) -> None:
    raise RuleViolation("email", "bad")


class _RuleFailUseCase(UseCase[Any, str]):
    rules = [_always_fail_rule]

    async def execute(self, cmd: Cmd = Input()) -> str:
        return cmd.email


class _Product(LoomStruct):
    id: int
    name: str


class _MainRepoUseCase(UseCase[_Product, str]):
    async def execute(self, product_id: int) -> str:
        product: _Product | None = await self.main_repo.get_by_id(product_id)
        if product is None:
            return "missing"
        return product.name


class _SeverityAssessment(msgspec.Struct, frozen=True):
    severity: int


class _TriageUseCase(UseCase[Any, dict[str, Any]]):
    """Mirrors the markers.md example: query, tool call, model run, branch."""

    async def execute(
        self,
        incident_id: str,
        caller: Identity = Caller(),
        triage: AgentHandle[_SeverityAssessment] = Agent("incident-triage"),
    ) -> dict[str, Any]:
        obs = triage.sql("observability_readonly")
        deploys = await obs.query("select * from deploys where incident = :id")

        runbooks = triage.mcp("runbooks")
        runbook = await runbooks.call("search_incident", {"incident_id": incident_id}, expect=dict)

        assessment = await triage.run(f"Assess {incident_id}.")

        escalate = assessment.output.severity >= 4
        return {
            "caller": caller.subject,
            "deploys": deploys,
            "runbook": runbook,
            "severity": assessment.output.severity,
            "escalate": escalate,
        }


class _NoDoubleUseCase(UseCase[Any, object]):
    async def execute(
        self, triage: AgentHandle[_SeverityAssessment] = Agent("incident-triage")
    ) -> object:
        return triage


# ---------------------------------------------------------------------------
# Basic execution
# ---------------------------------------------------------------------------


class TestUseCaseTestRun:
    async def test_run_simple_input_use_case(self) -> None:
        result = await (
            UseCaseTest(_SimpleUseCase()).with_input(email="alice@corp.com", name="Alice").run()
        )
        assert result == "alice@corp.com"

    async def test_run_param_only_use_case(self) -> None:
        result = await UseCaseTest(_ParamOnlyUseCase()).with_params(user_id=42).run()
        assert result == "id=42"

    async def test_run_param_and_input(self) -> None:
        result = await (
            UseCaseTest(_ParamAndInputUseCase())
            .with_params(tenant_id="t-1")
            .with_input(email="b@corp.com", name="Bob")
            .run()
        )
        assert result == "t-1:b@corp.com"

    async def test_run_with_command(self) -> None:
        cmd = Cmd(email="pre@corp.com", name="Pre")
        result = await UseCaseTest(_SimpleUseCase()).with_command(cmd).run()
        assert result == "pre@corp.com"

    async def test_run_with_loaded_entity(self) -> None:
        entity = Entity(name="preloaded")
        result = await (
            UseCaseTest(_LoadUseCase()).with_params(eid=1).with_loaded(Entity, entity).run()
        )
        assert result == "preloaded"

    async def test_run_with_deps(self) -> None:
        entity = Entity(name="from_repo")
        repo = AsyncMock()
        repo.get_by_id = AsyncMock(return_value=entity)
        result = await UseCaseTest(_LoadUseCase()).with_params(eid=5).with_deps(Entity, repo).run()
        assert result == "from_repo"
        repo.get_by_id.assert_awaited_once_with(5, profile="default")


# ---------------------------------------------------------------------------
# Error propagation
# ---------------------------------------------------------------------------


class TestUseCaseTestErrors:
    async def test_rule_violation_propagated(self) -> None:
        test_case = UseCaseTest(_RuleFailUseCase()).with_input(email="bad@corp.com", name="X")
        with pytest.raises(RuleViolations):
            await test_case.run()

    async def test_not_found_propagated(self) -> None:
        repo = AsyncMock()
        repo.get_by_id = AsyncMock(return_value=None)
        test_case = UseCaseTest(_LoadUseCase()).with_params(eid=99).with_deps(Entity, repo)
        with pytest.raises(NotFound):
            await test_case.run()


# ---------------------------------------------------------------------------
# Plan property
# ---------------------------------------------------------------------------


class TestUseCaseTestPlan:
    def test_plan_returns_execution_plan(self) -> None:
        runner = UseCaseTest(_SimpleUseCase())
        assert isinstance(runner.plan, ExecutionPlan)

    def test_plan_has_correct_use_case_type(self) -> None:
        runner = UseCaseTest(_SimpleUseCase())
        assert runner.plan.use_case_type is _SimpleUseCase

    def test_plan_has_input_binding(self) -> None:
        runner = UseCaseTest(_SimpleUseCase())
        assert runner.plan.input_binding is not None

    async def test_plan_after_run_is_same_instance(self) -> None:
        runner = UseCaseTest(_SimpleUseCase())
        plan_before = runner.plan
        await runner.with_input(email="x@corp.com", name="X").run()
        assert runner.plan is plan_before


# ---------------------------------------------------------------------------
# Builder fluent API
# ---------------------------------------------------------------------------


class TestUseCaseTestBuilder:
    def test_with_params_returns_self(self) -> None:
        runner = UseCaseTest(_SimpleUseCase())
        assert runner.with_params() is runner

    def test_with_input_returns_self(self) -> None:
        runner = UseCaseTest(_SimpleUseCase())
        assert runner.with_input(email="x@c.com", name="Y") is runner

    def test_with_command_returns_self(self) -> None:
        runner = UseCaseTest(_SimpleUseCase())
        assert runner.with_command(Cmd(email="x@c.com", name="Y")) is runner

    def test_with_loaded_returns_self(self) -> None:
        runner = UseCaseTest(_LoadUseCase())
        assert runner.with_loaded(Entity, Entity("e")) is runner

    def test_with_deps_returns_self(self) -> None:
        runner = UseCaseTest(_LoadUseCase())
        assert runner.with_deps(Entity, AsyncMock()) is runner

    def test_with_main_repo_returns_self(self) -> None:
        runner = UseCaseTest(_MainRepoUseCase())
        repo = AsyncMock()
        assert runner.with_main_repo(repo) is runner

    async def test_with_input_merges_multiple_calls(self) -> None:
        result = await (
            UseCaseTest(_SimpleUseCase())
            .with_input(email="a@corp.com")
            .with_input(name="Alice")
            .run()
        )
        assert result == "a@corp.com"

    async def test_with_params_merges_multiple_calls(self) -> None:
        result = await (
            UseCaseTest(_ParamAndInputUseCase())
            .with_params(tenant_id="t-1")
            .with_input(email="c@corp.com", name="C")
            .run()
        )
        assert result == "t-1:c@corp.com"

    async def test_with_command_overrides_previous_input(self) -> None:
        cmd = Cmd(email="cmd@corp.com", name="Cmd")
        result = await (
            UseCaseTest(_SimpleUseCase())
            .with_input(email="original@corp.com", name="Original")
            .with_command(cmd)
            .run()
        )
        assert result == "cmd@corp.com"

    async def test_with_main_repo_injects_repo_for_use_case_logic(self) -> None:
        repo = AsyncMock()
        repo.get_by_id = AsyncMock(return_value=_Product(id=1, name="keyboard"))

        result = await (
            UseCaseTest(_MainRepoUseCase()).with_main_repo(repo).with_params(product_id=1).run()
        )

        assert result == "keyboard"
        repo.get_by_id.assert_awaited_once_with(1)


# ---------------------------------------------------------------------------
# Agent() marker doubles (spec 014, T401)
# ---------------------------------------------------------------------------


class TestAgentMarkerFailsClosedWithoutADouble:
    async def test_declaring_agent_with_no_double_raises(self) -> None:
        with pytest.raises(RuntimeError, match="incident-triage"):
            await UseCaseTest(_NoDoubleUseCase()).with_caller(Identity(subject="ada")).run()

    async def test_error_names_the_use_case_and_the_agent(self) -> None:
        with pytest.raises(RuntimeError) as excinfo:
            await UseCaseTest(_NoDoubleUseCase()).with_caller(Identity(subject="ada")).run()
        message = str(excinfo.value)
        assert "_NoDoubleUseCase" in message
        assert "incident-triage" in message

    async def test_a_use_case_with_no_marker_at_all_never_needs_a_double(self) -> None:
        result = await UseCaseTest(_SimpleUseCase()).with_input(email="x@corp.com", name="X").run()
        assert result == "x@corp.com"


class TestAgentHandleDoubleRequiresNoNetworkModelOrDatabase:
    """The closing criterion of T401: the whole example use case, doubled fully."""

    async def test_the_full_example_use_case_runs_with_no_network_no_model_no_database(
        self,
    ) -> None:
        triage = AgentHandleDouble("incident-triage").on_run(_SeverityAssessment(severity=5))
        triage.sql("observability_readonly").on_query([{"service": "checkout", "status": "bad"}])
        triage.mcp("runbooks").on_call("search_incident", {"title": "checkout runbook"})

        result = await (
            UseCaseTest(_TriageUseCase())
            .with_caller(Identity(subject="ada"))
            .with_agent("incident-triage", triage)
            .with_params(incident_id="INC-1")
            .run()
        )

        assert result == {
            "caller": "ada",
            "deploys": [{"service": "checkout", "status": "bad"}],
            "runbook": {"title": "checkout runbook"},
            "severity": 5,
            "escalate": True,
        }

    async def test_the_double_records_the_prompt_it_was_asked(self) -> None:
        triage = AgentHandleDouble("incident-triage").on_run(_SeverityAssessment(severity=1))
        triage.sql("observability_readonly").on_query([])
        triage.mcp("runbooks").on_call("search_incident", {})

        await (
            UseCaseTest(_TriageUseCase())
            .with_caller(Identity(subject="ada"))
            .with_agent("incident-triage", triage)
            .with_params(incident_id="INC-2")
            .run()
        )

        assert triage.run_calls[0].prompt == "Assess INC-2."
        assert triage.sql("observability_readonly").calls[0].statement.startswith("select")
        assert triage.mcp("runbooks").calls[0].tool == "search_incident"

    async def test_a_below_threshold_severity_does_not_escalate(self) -> None:
        triage = AgentHandleDouble("incident-triage").on_run(_SeverityAssessment(severity=1))
        triage.sql("observability_readonly").on_query([])
        triage.mcp("runbooks").on_call("search_incident", {})

        result = await (
            UseCaseTest(_TriageUseCase())
            .with_caller(Identity(subject="ada"))
            .with_agent("incident-triage", triage)
            .with_params(incident_id="INC-3")
            .run()
        )

        assert result["escalate"] is False

    async def test_with_agent_returns_self_for_chaining(self) -> None:
        runner = UseCaseTest(_NoDoubleUseCase())
        double = AgentHandleDouble("incident-triage").on_run(_SeverityAssessment(severity=1))
        assert runner.with_agent("incident-triage", double) is runner

    async def test_run_text_double_returns_the_scripted_prose(self) -> None:
        double = AgentHandleDouble("writer").on_run_text("the write-up")
        answer = await double.run_text("summarise")
        assert answer.output == "the write-up"
        assert double.run_text_calls[0].prompt == "summarise"

    async def test_a_single_scheduled_answer_serves_every_call(self) -> None:
        double = AgentHandleDouble("writer").on_run(_SeverityAssessment(severity=2))
        first = await double.run("first")
        second = await double.run("second")
        assert first.output.severity == 2
        assert second.output.severity == 2
        assert len(double.run_calls) == 2

    async def test_an_unscripted_run_raises(self) -> None:
        double = AgentHandleDouble("incident-triage")
        with pytest.raises(AssertionError, match="incident-triage"):
            await double.run("Assess INC-4.")

    async def test_an_unscripted_tool_call_raises(self) -> None:
        double = AgentHandleDouble("incident-triage")
        with pytest.raises(AssertionError, match="search_incident"):
            await double.mcp("runbooks").call("search_incident", {}, expect=dict)

    async def test_an_unscripted_query_raises(self) -> None:
        double = AgentHandleDouble("incident-triage")
        with pytest.raises(AssertionError, match="observability_readonly"):
            await double.sql("observability_readonly").query("select 1")

    async def test_grants_lists_every_mcp_and_sql_name_reached(self) -> None:
        double = AgentHandleDouble("incident-triage")
        double.mcp("runbooks")
        double.sql("observability_readonly")
        assert double.grants() == ("runbooks", "observability_readonly")
