"""The hook's tool-call summary end to end (011 T405: AC17-AC20).

Every run goes through ``AgentRuntime`` over a ``ScriptedEngine`` that plays
two tools; the hook use cases are real use cases executed by a real
``RuntimeExecutor``.  No network, no database, no model.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator
from dataclasses import dataclass, field
from types import AsyncGeneratorType
from typing import Any

import msgspec
import pytest

from loom.ai.abc import (
    AgentEvent,
    DepsFactory,
    FinalEvent,
    TextDeltaEvent,
    ToolCallEvent,
    ToolCallRecord,
    ToolResultEvent,
)
from loom.ai.compiler._plan import AgentPlan, CompiledOutputHook
from loom.ai.runtime import AgentRuntime
from loom.ai.runtime._bounded import RunContext
from loom.ai.runtime._tool_calls import ToolCallAccumulator
from loom.core.command import Command
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from loom.core.use_case import Input, UseCase
from loom.core.use_case.keys import use_case_key
from tests.integration.ai.conftest import (
    DEFAULT_USAGE,
    CountingEngineProvider,
    RecordingDepsFactory,
    ScriptedEngine,
    make_ai_config,
    make_plan,
)

_AGENT = "incident-triage"
_MANDATORY = ("get_incident", "query_metrics", "list_alerts")


class AuditCommand(Command, frozen=True, kw_only=True):
    """Hook Command opting into the run's tool summary by declaring the name."""

    output: dict[str, Any]
    interaction_id: str
    tool_calls: tuple[ToolCallRecord, ...]


class BlindCommand(Command, frozen=True, kw_only=True):
    """Hook Command that never names ``tool_calls``, so nothing is accumulated."""

    output: dict[str, Any]
    interaction_id: str


@dataclass
class Recorder:
    """Shared observer of every hook execution, resolved from the container."""

    commands: list[AuditCommand] = field(default_factory=list)


@use_case_key("incidents.audit_run")
class AuditRun(UseCase[Any, str]):
    """Composes a completeness report out of what the run actually consulted."""

    def __init__(self, recorder: Recorder) -> None:
        self._recorder = recorder

    async def execute(self, cmd: AuditCommand = Input()) -> str:
        self._recorder.commands.append(cmd)
        answered = {
            record.tool
            for record in cmd.tool_calls
            if record.result is not None and record.result.ok
        }
        missing = [tool for tool in _MANDATORY if tool not in answered]
        return f"{len(answered)} of {len(_MANDATORY)} mandatory queries; missing {missing}"


@use_case_key("incidents.record_blind")
class RecordBlind(UseCase[Any, str]):
    """Hook that asks for nothing beyond the answer."""

    def __init__(self, recorder: Recorder) -> None:
        self._recorder = recorder

    async def execute(self, cmd: BlindCommand = Input()) -> str:
        return cmd.interaction_id


_COMMANDS: dict[type[UseCase[Any, Any]], type[Command]] = {
    AuditRun: AuditCommand,
    RecordBlind: BlindCommand,
}


def _two_tool_script() -> tuple[AgentEvent, ...]:
    """A run that answers one mandatory query, is refused another and abandons a third."""
    return (
        TextDeltaEvent(text="thinking"),
        ToolCallEvent(tool="get_incident", call_id="c1", arguments={"ref": "INC-1"}),
        ToolResultEvent(call_id="c1", ok=True, summary="3 rows"),
        ToolCallEvent(tool="query_metrics", call_id="c2", arguments={"window_min": 15}),
        ToolResultEvent(call_id="c2", ok=False, summary="refused"),
        ToolCallEvent(tool="list_alerts", call_id="c3", arguments={}),
        FinalEvent(output={"verdict": "inconclusive"}, usage=DEFAULT_USAGE),
    )


def _hooked_plan(use_case: type[UseCase[Any, Any]]) -> AgentPlan:
    """Build a plan whose ``on_output`` names ``use_case``, as the compiler would."""
    command = _COMMANDS[use_case]
    hook = CompiledOutputHook(
        usecase=str(getattr(use_case, "__use_case_key__", use_case.__name__)),
        use_case=use_case,
        accepted=frozenset(info.name for info in msgspec.structs.fields(command)),
    )
    return msgspec.structs.replace(make_plan(_AGENT), on_output=hook)


def _runtime(plan: AgentPlan, deps: RecordingDepsFactory, container: LoomContainer) -> AgentRuntime:
    """Build a runtime serving one scripted agent playing the two-tool script."""
    return AgentRuntime(
        plans=[plan],
        config=make_ai_config(),
        engine_provider=CountingEngineProvider(  # type: ignore[arg-type]
            engines={_AGENT: ScriptedEngine(script=_two_tool_script())}
        ),
        deps=deps,  # type: ignore[arg-type]
        container=container,
    )


@pytest.fixture
def recorder(container: LoomContainer) -> Recorder:
    """Recorder the hook use cases resolve from the container."""
    recorder = Recorder()
    container.register_instance(Recorder, recorder)
    return recorder


@pytest.fixture
def hook_deps() -> RecordingDepsFactory:
    """Deps factory serving both hook use cases through a real executor."""
    return RecordingDepsFactory((AuditRun, RecordBlind))


class TestResumenDeLlamadas:
    async def test_entrega_los_registros_al_command_del_caso_de_uso(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """AC18: the record survives the payload path and arrives as the declared type."""
        runtime = _runtime(_hooked_plan(AuditRun), hook_deps, container)

        async with runtime:
            await runtime.run(_AGENT, "prompt", identity=identity)

        (command,) = recorder.commands
        assert isinstance(command, AuditCommand)
        assert all(isinstance(record, ToolCallRecord) for record in command.tool_calls)
        assert [record.tool for record in command.tool_calls] == [
            "get_incident",
            "query_metrics",
            "list_alerts",
        ]
        assert command.tool_calls[0].arguments == {"ref": "INC-1"}
        assert command.tool_calls[1].arguments == {"window_min": 15}

    async def test_distingue_lo_refusado_de_lo_no_respondido_en_el_command(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """AC17: a refusal carries loom's outcome; an unanswered call carries none."""
        runtime = _runtime(_hooked_plan(AuditRun), hook_deps, container)

        async with runtime:
            await runtime.run(_AGENT, "prompt", identity=identity)

        answered, refused, unanswered = recorder.commands[0].tool_calls
        assert answered.result is not None
        assert (answered.result.ok, answered.result.summary) == (True, "3 rows")
        assert refused.result is not None
        assert (refused.result.ok, refused.result.summary) == (False, "refused")
        assert unanswered.result is None

    async def test_el_hook_compone_un_informe_con_lo_consultado(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The reported symptom: the hook can tell how many mandatory queries really ran."""
        runtime = _runtime(_hooked_plan(AuditRun), hook_deps, container)

        async with runtime:
            result = await runtime.run(_AGENT, "prompt", identity=identity)

        assert result.hook_result == (
            "1 of 3 mandatory queries; missing ['query_metrics', 'list_alerts']"
        )


class TestComposicionDelAcumulador:
    async def test_no_envuelve_el_camino_de_eventos_cuando_el_hook_no_lo_declara(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """AC19: with no declaration there is no accumulator in the path, wrapper included."""
        streams = _capture_hooked_streams(monkeypatch)
        runtime = _runtime(_hooked_plan(RecordBlind), hook_deps, container)

        async with runtime:
            await runtime.run(_AGENT, "prompt", identity=identity)

        (stream,) = streams
        assert ToolCallAccumulator.track.__qualname__ not in stream.__qualname__
        assert stream.__qualname__.endswith("supervised_events")

    async def test_envuelve_el_camino_de_eventos_cuando_el_hook_lo_declara(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The opt-in is what composes the accumulator, and nothing else does."""
        streams = _capture_hooked_streams(monkeypatch)
        runtime = _runtime(_hooked_plan(AuditRun), hook_deps, container)

        async with runtime:
            await runtime.run(_AGENT, "prompt", identity=identity)

        (stream,) = streams
        assert stream.__qualname__ == ToolCallAccumulator.track.__qualname__

    async def test_cierra_el_acumulador_cuando_el_consumidor_abandona_el_stream(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """AC20: the accumulator is closed where the run's other generators are."""
        streams = _capture_hooked_streams(monkeypatch)
        runtime = _runtime(_hooked_plan(AuditRun), hook_deps, container)

        async with (
            runtime,
            runtime.run_stream(_AGENT, "prompt", identity=identity) as stream,
        ):
            await anext(aiter(stream))

        (tracked,) = streams
        assert tracked.ag_frame is None


def _capture_hooked_streams(
    monkeypatch: pytest.MonkeyPatch,
) -> list[AsyncGeneratorType[AgentEvent, None]]:
    """Record the event stream the lifecycle composes for the hook stage.

    Asserting on this object is what proves the composition itself, rather
    than proving an empty summary a missing accumulator and a silent tool
    would produce alike.
    """
    from loom.ai.runtime import _lifecycle

    original = _lifecycle.hooked_events
    captured: list[AsyncGeneratorType[AgentEvent, None]] = []

    def spy(
        events: AsyncIterator[AgentEvent],
        run: RunContext,
        deps: DepsFactory,
        container: LoomContainer,
        *,
        tool_calls: ToolCallAccumulator | None = None,
    ) -> AsyncGenerator[AgentEvent, None]:
        assert isinstance(events, AsyncGeneratorType)
        captured.append(events)
        return original(events, run, deps, container, tool_calls=tool_calls)

    monkeypatch.setattr(_lifecycle, "hooked_events", spy)
    return captured
