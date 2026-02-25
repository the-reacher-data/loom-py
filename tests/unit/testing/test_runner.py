from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from loom.core.command import Command
from loom.core.engine.plan import ExecutionPlan
from loom.core.errors import NotFound
from loom.core.use_case.markers import Input, Load
from loom.core.use_case.rule import RuleViolation, RuleViolations
from loom.core.use_case.use_case import UseCase
from loom.testing.runner import UseCaseTest


# ---------------------------------------------------------------------------
# Domain fixtures
# ---------------------------------------------------------------------------


class Cmd(Command, frozen=True):
    email: str
    name: str


class Entity:
    def __init__(self, name: str) -> None:
        self.name = name


class _SimpleUseCase(UseCase[str]):
    async def execute(self, cmd: Cmd = Input()) -> str:  # type: ignore[override]
        return cmd.email


class _ParamOnlyUseCase(UseCase[str]):
    async def execute(self, user_id: int) -> str:  # type: ignore[override]
        return f"id={user_id}"


class _ParamAndInputUseCase(UseCase[str]):
    async def execute(  # type: ignore[override]
        self,
        tenant_id: str,
        cmd: Cmd = Input(),
    ) -> str:
        return f"{tenant_id}:{cmd.email}"


class _LoadUseCase(UseCase[str]):
    async def execute(  # type: ignore[override]
        self,
        eid: int,
        entity: Entity = Load(Entity, by="eid"),
    ) -> str:
        return entity.name


class _RuleFailUseCase(UseCase[str]):
    rules = [lambda cmd, fs: (_ for _ in ()).throw(RuleViolation("email", "bad"))]

    async def execute(self, cmd: Cmd = Input()) -> str:  # type: ignore[override]
        return cmd.email


# ---------------------------------------------------------------------------
# Basic execution
# ---------------------------------------------------------------------------


class TestUseCaseTestRun:
    async def test_run_simple_input_use_case(self) -> None:
        result = await (
            UseCaseTest(_SimpleUseCase())
            .with_input(email="alice@corp.com", name="Alice")
            .run()
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
            UseCaseTest(_LoadUseCase())
            .with_params(eid=1)
            .with_loaded(Entity, entity)
            .run()
        )
        assert result == "preloaded"

    async def test_run_with_deps(self) -> None:
        entity = Entity(name="from_repo")
        repo = AsyncMock()
        repo.get_by_id = AsyncMock(return_value=entity)
        result = await (
            UseCaseTest(_LoadUseCase())
            .with_params(eid=5)
            .with_deps(Entity, repo)
            .run()
        )
        assert result == "from_repo"
        repo.get_by_id.assert_awaited_once_with(5)


# ---------------------------------------------------------------------------
# Error propagation
# ---------------------------------------------------------------------------


class TestUseCaseTestErrors:
    async def test_rule_violation_propagated(self) -> None:
        with pytest.raises(RuleViolations):
            await (
                UseCaseTest(_RuleFailUseCase())
                .with_input(email="bad@corp.com", name="X")
                .run()
            )

    async def test_not_found_propagated(self) -> None:
        repo = AsyncMock()
        repo.get_by_id = AsyncMock(return_value=None)
        with pytest.raises(NotFound):
            await (
                UseCaseTest(_LoadUseCase())
                .with_params(eid=99)
                .with_deps(Entity, repo)
                .run()
            )


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
