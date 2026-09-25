"""A FromConfig value never leaves the step: no log, repr, plan, event or error carries it."""

from __future__ import annotations

import logging
import traceback
from datetime import date
from typing import Any, Literal

import pytest

from loom.core.config import ConfigContext
from loom.core.observability.runtime import ObservabilityRuntime
from loom.etl import ETLParams, ETLPipeline, ETLProcess, ETLStep, FromConfig, IntoTable
from loom.etl.compiler import ETLCompilationError, ETLCompiler
from loom.etl.executor import ETLExecutor
from loom.etl.runner import ETLRunner
from loom.etl.runtime import ConfigValueError
from loom.etl.testing import StubRunObserver, StubSourceReader, StubTargetWriter

SECRET = "s3cr3t-respondio-token-value"


class RunParams(ETLParams):  # type: ignore[misc]
    run_date: date


class TokenStep(ETLStep[RunParams]):
    api_token = FromConfig("respondio.api_token")
    target = IntoTable("raw.token").replace()

    def execute(self, params: RunParams, *, api_token: str) -> Any:  # type: ignore[override]
        return {"token_length": len(api_token)}


class ModeStep(ETLStep[RunParams]):
    # msgspec names the rejected value for a Literal: the case most likely to leak.
    mode = FromConfig("respondio.api_token", Literal["read", "write"])
    target = IntoTable("raw.mode").replace()

    def execute(self, params: RunParams, *, mode: str) -> Any:  # type: ignore[override]
        return mode


class _Proc(ETLProcess[RunParams]):
    steps = [TokenStep]


class _Pipeline(ETLPipeline[RunParams]):
    processes = [_Proc]


_PARAMS = RunParams(run_date=date(2026, 9, 25))


def _context() -> ConfigContext:
    return ConfigContext.from_dict({"respondio": {"api_token": SECRET}})


def _rendered(exc: BaseException) -> str:
    return "".join(traceback.format_exception(exc))


def test_successful_run_leaks_nothing(caplog: pytest.LogCaptureFixture) -> None:
    observer = StubRunObserver()
    writer = StubTargetWriter()
    runner = ETLRunner(
        StubSourceReader({}),
        writer,
        ObservabilityRuntime([observer]),
        config_context=_context(),
    )

    with caplog.at_level(logging.DEBUG):
        runner.run(_Pipeline, _PARAMS)

    assert writer.written[0][0] == {"token_length": len(SECRET)}
    assert SECRET not in caplog.text
    assert all(SECRET not in repr(data) for _, data in observer.events)
    assert SECRET not in repr(ETLCompiler().compile(_Pipeline))
    assert SECRET not in repr(writer.written[0][1])
    assert SECRET not in repr(TokenStep.api_token)
    assert SECRET not in repr(runner)


def test_compile_error_does_not_carry_the_value() -> None:
    with pytest.raises(ETLCompilationError) as exc_info:
        ETLCompiler(config_context=_context()).compile_step(ModeStep)

    assert "does not validate" in str(exc_info.value)
    assert SECRET not in _rendered(exc_info.value)


def test_runtime_error_and_error_event_do_not_carry_the_value(
    caplog: pytest.LogCaptureFixture,
) -> None:
    observer = StubRunObserver()
    executor = ETLExecutor(
        StubSourceReader({}),
        StubTargetWriter(),
        ObservabilityRuntime([observer]),
        config_context=_context(),
    )

    with caplog.at_level(logging.DEBUG), pytest.raises(ConfigValueError) as exc_info:
        executor.run_step(ETLCompiler().compile_step(ModeStep), _PARAMS)

    assert SECRET not in _rendered(exc_info.value)
    assert SECRET not in caplog.text
    assert all(SECRET not in repr(data) for _, data in observer.events)
