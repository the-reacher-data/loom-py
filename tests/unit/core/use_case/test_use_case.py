from __future__ import annotations

from typing import Any

import pytest

from loom.core.use_case.compute import ComputeFn
from loom.core.use_case.rule import RuleFn
from loom.core.use_case.use_case import UseCase


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _noop_compute(cmd: object, fields_set: frozenset[str]) -> object:
    return cmd


def _noop_rule(cmd: object, fields_set: frozenset[str]) -> None:
    pass


# ---------------------------------------------------------------------------
# Concrete subclasses for testing
# ---------------------------------------------------------------------------


class _MinimalUseCase(UseCase[str]):
    """UseCase with no computes or rules."""

    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class _WithPipelineUseCase(UseCase[None]):
    """UseCase that declares computes and rules at class level."""

    computes = [_noop_compute]
    rules = [_noop_rule]

    async def execute(self, **kwargs: Any) -> None:
        pass


# ---------------------------------------------------------------------------
# Abstract contract
# ---------------------------------------------------------------------------


class TestUseCaseIsAbstract:
    def test_cannot_instantiate_base_directly(self) -> None:
        with pytest.raises(TypeError):
            UseCase()  # type: ignore[abstract]

    def test_subclass_without_execute_is_abstract(self) -> None:
        class _Incomplete(UseCase[str]):
            pass

        with pytest.raises(TypeError):
            _Incomplete()  # type: ignore[abstract]

    def test_concrete_subclass_can_be_instantiated(self) -> None:
        uc = _MinimalUseCase()
        assert uc is not None


# ---------------------------------------------------------------------------
# ClassVar defaults
# ---------------------------------------------------------------------------


class TestUseCaseClassVars:
    def test_default_computes_is_empty(self) -> None:
        assert _MinimalUseCase.computes == ()

    def test_default_rules_is_empty(self) -> None:
        assert _MinimalUseCase.rules == ()

    def test_declared_computes_accessible(self) -> None:
        assert len(_WithPipelineUseCase.computes) == 1
        assert _WithPipelineUseCase.computes[0] is _noop_compute

    def test_declared_rules_accessible(self) -> None:
        assert len(_WithPipelineUseCase.rules) == 1
        assert _WithPipelineUseCase.rules[0] is _noop_rule

    def test_class_vars_do_not_leak_across_subclasses(self) -> None:
        assert _MinimalUseCase.computes is not _WithPipelineUseCase.computes
        assert len(_MinimalUseCase.computes) == 0


# ---------------------------------------------------------------------------
# execute is async and callable
# ---------------------------------------------------------------------------


class TestUseCaseExecute:
    async def test_execute_is_awaitable(self) -> None:
        uc = _MinimalUseCase()
        result = await uc.execute()
        assert result == "ok"

    async def test_subclass_can_declare_typed_execute(self) -> None:
        class _TypedUseCase(UseCase[int]):
            async def execute(self, x: int = 0) -> int:  # type: ignore[override]
                return x * 2

        uc = _TypedUseCase()
        result = await uc.execute(x=5)
        assert result == 10
