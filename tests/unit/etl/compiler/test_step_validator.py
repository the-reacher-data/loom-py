"""Tests for compiler.validators._step."""

from __future__ import annotations

import importlib
from dataclasses import FrozenInstanceError
from typing import Any

import pytest

from loom.etl.compiler._plan import SourceBinding, TargetBinding
from loom.etl.compiler.validators import _step as step_validator
from loom.etl.io._format import Format
from loom.etl.io._source import SourceKind, SourceSpec
from loom.etl.io.target._table import ReplaceSpec
from loom.etl.schema._table import TableRef

step_validator = importlib.reload(step_validator)
StepCompilationContext = step_validator.StepCompilationContext
validate_step = step_validator.validate_step


def _build_context() -> StepCompilationContext:
    source_spec = SourceSpec(
        alias="orders",
        kind=SourceKind.TABLE,
        format=Format.DELTA,
        table_ref=TableRef("raw.orders"),
    )
    return StepCompilationContext(
        step_type=type("DummyStep", (), {}),
        params_type=type("DummyParams", (), {}),
        source_bindings=(SourceBinding(alias="orders", spec=source_spec),),
        target_binding=TargetBinding(spec=ReplaceSpec(table_ref=TableRef("staging.orders"))),
    )


def test_validate_step_calls_all_validators_in_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _build_context()
    calls: list[str] = []

    def _validate_signature(
        step_type: type[Any],
        params_type: type[Any],
        source_bindings: tuple[SourceBinding, ...],
    ) -> None:
        assert step_type is ctx.step_type
        assert params_type is ctx.params_type
        assert source_bindings == ctx.source_bindings
        calls.append("signature")

    def _validate_upsert(step_type: type[Any], spec: Any) -> None:
        assert step_type is ctx.step_type
        assert spec is ctx.target_binding.spec
        calls.append("upsert")

    def _validate_params(
        step_type: type[Any],
        params_type: type[Any],
        source_bindings: tuple[SourceBinding, ...],
        target_binding: TargetBinding,
    ) -> None:
        assert step_type is ctx.step_type
        assert params_type is ctx.params_type
        assert source_bindings == ctx.source_bindings
        assert target_binding == ctx.target_binding
        calls.append("params")

    monkeypatch.setattr(step_validator, "validate_execute_signature", _validate_signature)
    monkeypatch.setattr(step_validator, "validate_upsert_spec", _validate_upsert)
    monkeypatch.setattr(step_validator, "validate_param_exprs", _validate_params)

    validate_step(ctx)

    assert calls == ["signature", "upsert", "params"]


def test_step_compilation_context_is_frozen() -> None:
    ctx = _build_context()

    with pytest.raises((FrozenInstanceError, AttributeError)):
        ctx.params_type = int  # type: ignore[misc]
