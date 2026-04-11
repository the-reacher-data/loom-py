"""Tests for ETLProcess / ETLPipeline class-level validation and generics."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from loom.etl import ETLParams, ETLPipeline, ETLProcess, ETLStep, IntoTable
from loom.etl.pipeline._generics import _extract_generic_arg


class RunParams(ETLParams):
    run_date: date


class _NoSourceStep(ETLStep[RunParams]):
    target = IntoTable("staging.out").replace()

    def execute(self, params: RunParams) -> Any:
        return {"ok": True}


def test_process_extracts_generic_params_type() -> None:
    class _Process(ETLProcess[RunParams]):
        steps = [_NoSourceStep]

    assert _Process._params_type is RunParams
    assert _extract_generic_arg(_Process, ETLProcess) is RunParams
    assert ETLProcess._params_type is None


def test_pipeline_extracts_generic_params_type() -> None:
    class _Process(ETLProcess[RunParams]):
        steps = [_NoSourceStep]

    class _Pipeline(ETLPipeline[RunParams]):
        processes = [_Process]

    assert _Pipeline._params_type is RunParams
    assert _extract_generic_arg(_Pipeline, ETLPipeline) is RunParams
    assert ETLPipeline._params_type is None


def test_process_steps_must_be_a_list() -> None:
    with pytest.raises(TypeError, match="'steps' must be a list"):

        class _BadProcess(ETLProcess[RunParams]):  # NOSONAR
            steps = (_NoSourceStep,)


def test_pipeline_processes_must_be_a_list() -> None:
    class _Process(ETLProcess[RunParams]):
        steps = [_NoSourceStep]

    with pytest.raises(TypeError, match="'processes' must be a list"):

        class _BadPipeline(ETLPipeline[RunParams]):  # NOSONAR
            processes = (_Process,)


def test_extract_generic_arg_returns_none_without_matching_origin() -> None:
    class _Plain:
        pass

    assert _extract_generic_arg(_Plain, ETLProcess) is None
