"""The ``on_output`` hook at run time (002 T5: AC5-AC12, AC14).

Every run goes through ``AgentRuntime`` over a ``ScriptedEngine``; the hook
use cases are real use cases executed by a real ``RuntimeExecutor`` over a
recording unit of work.  No network, no database, no model.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from typing import Any

import msgspec
import pytest

from loom.ai.abc import (
    AgentEvent,
    ErrorEvent,
    FinalEvent,
    TextDeltaEvent,
    ToolCallEvent,
    ToolResultEvent,
)
from loom.ai.compiler._plan import AgentPlan, CompiledOutputHook
from loom.ai.errors import AgentCompilationError, AgentErrorCode, AgentRunErrorCode
from loom.ai.runtime import AgentRunError, AgentRuntime
from loom.ai.runtime._hooks import HOOK_FAILED_MESSAGE
from loom.core.command import Command
from loom.core.di import LoomContainer
from loom.core.errors import Forbidden
from loom.core.identity import ANONYMOUS, Identity
from loom.core.use_case import Caller, Input, UseCase
from loom.core.use_case.keys import use_case_key
from tests.integration.ai.conftest import (
    DEFAULT_USAGE,
    CountingEngineProvider,
    RecordingDeps,
    RecordingDepsFactory,
    RecordingMcpSession,
    ScriptedEngine,
    StubDepsFactory,
    StubMcpClient,
    default_script,
    error_script,
    make_ai_config,
    make_mcp_capability,
    make_mcp_servers,
    make_plan,
    make_policies,
    mcp_client_factory,
)

_AGENT = "incident-triage"
_DENIED_MESSAGE = "the caller is not allowed to perform this operation"


# ---------------------------------------------------------------------------
# Incident-triage hook use cases
# ---------------------------------------------------------------------------


class TriageReport(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Structured triage verdict an agent returns."""

    incident_ref: str
    severity: str
    confidence: float
    alerts: list[str] = []


class TriageRecorded(msgspec.Struct, frozen=True):
    """Result of recording one triage."""

    triage_id: str


@dataclass(frozen=True)
class RecordedCall:
    """One execution of a hook use case: the command it received and its caller."""

    command: Command
    caller: Identity


@dataclass
class Recorder:
    """Shared observer of every hook execution, resolved from the container.

    Attributes:
        calls: One entry per execution, in order.
        timeline: ``"hook"`` appended when a hook runs; tests append events.
        entered: Set when the first execution starts.
        gate: When set, an execution waits on it before completing.
        sleep_s: Time an execution sleeps before completing.
        failure: Exception an execution raises after its waits; a
            ``CancelledError`` here models a hook cancelling itself.
        cancelled: Whether an execution observed ``CancelledError``.
    """

    calls: list[RecordedCall] = field(default_factory=list)
    timeline: list[str] = field(default_factory=list)
    entered: asyncio.Event = field(default_factory=asyncio.Event)
    gate: asyncio.Event | None = None
    sleep_s: float = 0.0
    failure: BaseException | None = None
    cancelled: bool = False

    async def record(self, command: Command, caller: Identity) -> None:
        """Record one execution, honouring the configured gate, sleep and failure."""
        self.calls.append(RecordedCall(command=command, caller=caller))
        self.timeline.append("hook")
        self.entered.set()
        try:
            if self.gate is not None:
                await self.gate.wait()
            if self.sleep_s:
                await asyncio.sleep(self.sleep_s)
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        if self.failure is not None:
            raise self.failure


class TriageCommand(Command, frozen=True, kw_only=True):
    """Typed output plus every context name the runtime offers."""

    output: TriageReport
    interaction_id: str
    subject: str
    mechanism: str
    agent: str
    provider: str
    model: str
    conversation_id: str | None = None


class AnswerCommand(Command, frozen=True, kw_only=True):
    """Untyped output plus the context names a spoofing test needs."""

    output: dict[str, Any]
    interaction_id: str
    subject: str
    conversation_id: str | None = None


class StrictCommand(Command, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Declares only ``output`` and ``interaction_id`` and refuses anything else."""

    output: dict[str, Any]
    interaction_id: str


@use_case_key("incidents.record_triage")
class RecordTriage(UseCase[Any, TriageRecorded]):
    """Records a typed triage report."""

    def __init__(self, recorder: Recorder) -> None:
        self._recorder = recorder

    async def execute(
        self, cmd: TriageCommand = Input(), caller: Identity = Caller()
    ) -> TriageRecorded:
        await self._recorder.record(cmd, caller)
        return TriageRecorded(triage_id=cmd.interaction_id)


@use_case_key("incidents.record_answer")
class RecordAnswer(UseCase[Any, TriageRecorded]):
    """Records an untyped answer."""

    def __init__(self, recorder: Recorder) -> None:
        self._recorder = recorder

    async def execute(
        self, cmd: AnswerCommand = Input(), caller: Identity = Caller()
    ) -> TriageRecorded:
        await self._recorder.record(cmd, caller)
        return TriageRecorded(triage_id=cmd.interaction_id)


@use_case_key("incidents.record_strict")
class RecordStrict(UseCase[Any, TriageRecorded]):
    """Records through a strict command."""

    def __init__(self, recorder: Recorder) -> None:
        self._recorder = recorder

    async def execute(
        self, cmd: StrictCommand = Input(), caller: Identity = Caller()
    ) -> TriageRecorded:
        await self._recorder.record(cmd, caller)
        return TriageRecorded(triage_id=cmd.interaction_id)


_COMMANDS: dict[type[UseCase[Any, Any]], type[Command]] = {
    RecordTriage: TriageCommand,
    RecordAnswer: AnswerCommand,
    RecordStrict: StrictCommand,
}

REPORT = TriageReport(incident_ref="INC-1", severity="high", confidence=0.71, alerts=["A-7"])


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _hooked_plan(use_case: type[UseCase[Any, Any]], **plan_kwargs: Any) -> AgentPlan:
    """Build a plan whose ``on_output`` names ``use_case``, as the compiler would."""
    command = _COMMANDS[use_case]
    hook = CompiledOutputHook(
        usecase=str(getattr(use_case, "__use_case_key__", use_case.__name__)),
        use_case=use_case,
        accepted=frozenset(info.name for info in msgspec.structs.fields(command)),
    )
    return msgspec.structs.replace(make_plan(_AGENT, **plan_kwargs), on_output=hook)


def _runtime(
    engine: ScriptedEngine,
    plan: AgentPlan,
    *,
    deps: object,
    container: LoomContainer,
    max_concurrent_runs: int = 8,
) -> AgentRuntime:
    """Build a runtime serving one scripted agent."""
    return AgentRuntime(
        plans=[plan],
        config=make_ai_config(max_concurrent_runs=max_concurrent_runs),
        engine_provider=CountingEngineProvider(engines={_AGENT: engine}),  # type: ignore[arg-type]
        deps=deps,  # type: ignore[arg-type]
        container=container,
    )


def _tool_script(output: object) -> tuple[AgentEvent, ...]:
    """A run with one full tool cycle before its ``final``."""
    return (
        TextDeltaEvent(text="thinking"),
        ToolCallEvent(tool="sql_observability", call_id="c1", arguments={}),
        ToolResultEvent(call_id="c1", ok=True, summary="3 rows"),
        FinalEvent(output=output, usage=DEFAULT_USAGE),
    )


async def _collect(runtime: AgentRuntime, identity: Identity, **kwargs: Any) -> list[AgentEvent]:
    """Consume a whole stream and return its events."""
    events: list[AgentEvent] = []
    async with runtime.run_stream(_AGENT, "prompt", identity=identity, **kwargs) as stream:
        async for event in stream:
            events.append(event)
    return events


@dataclass(frozen=True)
class _UnboundDepsFactory:
    """Deps factory whose bundle carries an invoker never bound to a caller."""

    inner: RecordingDepsFactory

    def build(self, identity: Identity, container: LoomContainer) -> object:
        """Return the inner bundle with its invoker's identity stripped."""
        bundle = self.inner.build(identity, container)
        assert isinstance(bundle, RecordingDeps)
        return replace(bundle, invoker=replace(bundle.invoker, identity=None))


@pytest.fixture
def recorder(container: LoomContainer) -> Recorder:
    """Recorder the hook use cases resolve from the container."""
    recorder = Recorder()
    container.register_instance(Recorder, recorder)
    return recorder


@pytest.fixture
def hook_deps() -> RecordingDepsFactory:
    """Deps factory serving the three hook use cases through a real executor."""
    return RecordingDepsFactory((RecordTriage, RecordAnswer, RecordStrict))


# ---------------------------------------------------------------------------
# AC5, AC6 — execution, command shape, caller
# ---------------------------------------------------------------------------


class TestEjecucionDelHook:
    async def test_ejecuta_el_hook_una_vez_con_el_output_tipado_cuando_el_run_completa(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The command carries the validated output as the declared type plus the context."""
        engine = ScriptedEngine(script=default_script(REPORT))
        plan = _hooked_plan(RecordTriage)
        runtime = _runtime(engine, plan, deps=hook_deps, container=container)

        async with runtime:
            result = await runtime.run(_AGENT, "prompt", identity=identity)

        assert len(recorder.calls) == 1
        command = recorder.calls[0].command
        assert isinstance(command, TriageCommand)
        assert command.output == REPORT
        assert command.subject == identity.subject
        assert command.mechanism == identity.mechanism
        assert command.agent == plan.name
        assert command.provider == plan.inference.provider
        assert command.model == plan.inference.model
        assert result.interaction_id is not None
        assert command.interaction_id == result.interaction_id
        assert result.hook_result == TriageRecorded(triage_id=result.interaction_id)
        assert result.output == REPORT

    async def test_entrega_el_output_como_dict_cuando_el_command_lo_declara_asi(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A dict output reaches a ``dict[str, Any]`` field unchanged."""
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        runtime = _runtime(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with runtime:
            await runtime.run(_AGENT, "prompt", identity=identity)

        command = recorder.calls[0].command
        assert isinstance(command, AnswerCommand)
        assert command.output == {"answer": "42"}

    async def test_no_deja_que_un_campo_subject_del_output_suplante_al_contexto(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """``subject`` inside the output stays nested; the command's is the caller's."""
        engine = ScriptedEngine(script=default_script({"answer": "42", "subject": "spoofed"}))
        runtime = _runtime(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with runtime:
            await runtime.run(_AGENT, "prompt", identity=identity)

        command = recorder.calls[0].command
        assert isinstance(command, AnswerCommand)
        assert command.subject == identity.subject
        assert command.output["subject"] == "spoofed"

    async def test_alimenta_un_command_estricto_cuando_solo_declara_output_e_interaction_id(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The offered dict is filtered to the declared names before ``from_payload``."""
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        runtime = _runtime(engine, _hooked_plan(RecordStrict), deps=hook_deps, container=container)

        async with runtime:
            result = await runtime.run(_AGENT, "prompt", identity=identity)

        command = recorder.calls[0].command
        assert isinstance(command, StrictCommand)
        assert result.interaction_id is not None
        assert command == StrictCommand(
            output={"answer": "42"}, interaction_id=result.interaction_id
        )

    async def test_ejecuta_el_hook_una_sola_vez_al_final_cuando_el_script_incluye_tool_events(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Tool events pass through untouched; the hook runs once, at the final event."""
        engine = ScriptedEngine(script=_tool_script({"answer": "42"}))
        runtime = _runtime(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with runtime, runtime.run_stream(_AGENT, "prompt", identity=identity) as stream:
            async for event in stream:
                recorder.timeline.append(type(event).__name__)

        assert len(recorder.calls) == 1
        assert recorder.timeline == [
            "TextDeltaEvent",
            "ToolCallEvent",
            "ToolResultEvent",
            "hook",
            "FinalEvent",
        ]

    async def test_ejecuta_el_hook_como_el_caller_verificado_cuando_hay_identidad(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """``Caller()`` inside the use case is the run's identity."""
        engine = ScriptedEngine(script=default_script(REPORT))
        runtime = _runtime(engine, _hooked_plan(RecordTriage), deps=hook_deps, container=container)

        async with runtime:
            await runtime.run(_AGENT, "prompt", identity=identity)

        assert recorder.calls[0].caller == identity

    async def test_ejecuta_el_hook_como_anonimo_cuando_el_caller_es_anonymous(
        self,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """An anonymous run still records, with ``Caller()`` bound to ``ANONYMOUS``."""
        engine = ScriptedEngine(script=default_script(REPORT))
        runtime = _runtime(engine, _hooked_plan(RecordTriage), deps=hook_deps, container=container)

        async with runtime:
            await runtime.run(_AGENT, "prompt", identity=ANONYMOUS)

        call = recorder.calls[0]
        assert call.caller == ANONYMOUS
        assert isinstance(call.command, TriageCommand)
        assert call.command.subject == ""

    async def test_marca_el_interaction_id_sin_hook_result_cuando_no_hay_hook(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Without a hook the run is still named; nothing is executed."""
        engine = ScriptedEngine()
        runtime = _runtime(engine, make_plan(_AGENT), deps=hook_deps, container=container)

        async with runtime:
            result = await runtime.run(_AGENT, "prompt", identity=identity)

        assert result.interaction_id is not None
        assert len(result.interaction_id) == 32
        assert result.hook_result is None
        assert recorder.calls == []


# ---------------------------------------------------------------------------
# AC7, AC8 — failure mode and transaction boundary
# ---------------------------------------------------------------------------


class TestFallosDelHook:
    async def test_falla_con_hook_failed_sin_el_detalle_cuando_el_hook_lanza(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The caller gets the coded error and the id; the exception text stays server-side."""
        recorder.failure = ValueError("secret detail")
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        runtime = _runtime(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with runtime:
            with pytest.raises(AgentRunError) as failure:
                await runtime.run(_AGENT, "prompt", identity=identity)

        assert failure.value.code is AgentRunErrorCode.HOOK_FAILED
        assert failure.value.interaction_id is not None
        assert str(failure.value) == HOOK_FAILED_MESSAGE
        assert "secret detail" not in str(failure.value)
        assert hook_deps.uow.log == ["begin", "rollback"]
        assert engine.stream_count == 1

    async def test_falla_con_hook_failed_cuando_el_hook_se_cancela_a_si_mismo(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A hook ending in its own ``CancelledError`` is a hook failure, not a consumer exit."""
        recorder.failure = asyncio.CancelledError()
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        runtime = _runtime(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with runtime:
            with pytest.raises(AgentRunError) as failure:
                await runtime.run(_AGENT, "prompt", identity=identity)

        assert failure.value.code is AgentRunErrorCode.HOOK_FAILED
        assert failure.value.interaction_id is not None
        assert str(failure.value) == HOOK_FAILED_MESSAGE
        assert len(recorder.calls) == 1

    async def test_emite_un_solo_error_sin_final_cuando_el_hook_se_cancela_a_si_mismo_en_stream(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The stream closes with one ``error`` carrying the id and no ``final``."""
        recorder.failure = asyncio.CancelledError()
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        runtime = _runtime(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with runtime:
            events = await _collect(runtime, identity)

        terminal = [event for event in events if isinstance(event, ErrorEvent | FinalEvent)]
        assert len(terminal) == 1
        error = terminal[0]
        assert isinstance(error, ErrorEvent)
        assert error.code is AgentRunErrorCode.HOOK_FAILED
        assert error.interaction_id is not None
        assert error.message == HOOK_FAILED_MESSAGE

    async def test_emite_un_solo_error_sin_final_cuando_el_hook_lanza_en_stream(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The stream ends in one ``error`` event carrying the id, and no ``final``."""
        recorder.failure = ValueError("secret detail")
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        runtime = _runtime(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with runtime:
            events = await _collect(runtime, identity)

        terminal = [event for event in events if isinstance(event, ErrorEvent | FinalEvent)]
        assert len(terminal) == 1
        error = terminal[0]
        assert isinstance(error, ErrorEvent)
        assert error.code is AgentRunErrorCode.HOOK_FAILED
        assert error.interaction_id is not None
        assert "secret detail" not in error.message
        assert engine.stream_count == 1

    async def test_mapea_forbidden_a_unauthorized_cuando_las_reglas_del_hook_rechazan(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """An authorization denial keeps its meaning and its fixed text."""
        recorder.failure = Forbidden("triage of INC-1 is restricted")
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        runtime = _runtime(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with runtime:
            with pytest.raises(AgentRunError) as failure:
                await runtime.run(_AGENT, "prompt", identity=identity)

        assert failure.value.code is AgentRunErrorCode.UNAUTHORIZED
        assert str(failure.value) == _DENIED_MESSAGE
        assert failure.value.interaction_id is not None

    async def test_falla_con_hook_failed_cuando_el_hook_excede_tool_timeout_ms(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A hook sleeping past the bound is cut and reported as ``HOOK_FAILED``."""
        recorder.sleep_s = 5.0
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        plan = _hooked_plan(RecordAnswer, policies=make_policies(tool_timeout_ms=50))
        runtime = _runtime(engine, plan, deps=hook_deps, container=container)
        loop = asyncio.get_running_loop()

        async with runtime:
            started = loop.time()
            with pytest.raises(AgentRunError) as failure:
                await runtime.run(_AGENT, "prompt", identity=identity)
            elapsed = loop.time() - started

        assert failure.value.code is AgentRunErrorCode.HOOK_FAILED
        assert elapsed < 2.0, f"the hook was not bounded: {elapsed:.3f}s"
        assert recorder.cancelled is True
        # The cut hook is cancelled and the executor rolls back on ``BaseException``,
        # so the unit of work it had begun is closed.
        assert hook_deps.uow.log == ["begin", "rollback"]

    async def test_confirma_la_transaccion_cuando_el_hook_termina(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A successful hook is one ``begin → commit`` through the executor."""
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        runtime = _runtime(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with runtime:
            await runtime.run(_AGENT, "prompt", identity=identity)

        assert hook_deps.uow.log == ["begin", "commit"]
        assert len(recorder.calls) == 1


# ---------------------------------------------------------------------------
# AC9 — only completed runs fire the hook
# ---------------------------------------------------------------------------


class TestRunsNoCompletados:
    async def test_no_ejecuta_el_hook_cuando_el_motor_termina_en_error(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """An engine failure passes through with the id and runs no hook."""
        engine = ScriptedEngine(script=error_script(AgentRunErrorCode.PROVIDER_UNAVAILABLE))
        runtime = _runtime(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with runtime:
            events = await _collect(runtime, identity)
            with pytest.raises(AgentRunError) as failure:
                await runtime.run(_AGENT, "prompt", identity=identity)

        error = events[-1]
        assert isinstance(error, ErrorEvent)
        assert error.code is AgentRunErrorCode.PROVIDER_UNAVAILABLE
        assert error.interaction_id is not None
        assert failure.value.interaction_id is not None
        assert recorder.calls == []

    async def test_no_ejecuta_el_hook_cuando_el_run_excede_su_presupuesto(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A ``RUN_TIMEOUT`` breach is terminal, named, and runs no hook."""
        engine = ScriptedEngine(script=default_script({"answer": "42"}), delays_ms=(0, 400))
        plan = _hooked_plan(RecordAnswer, policies=make_policies(run_timeout_ms=30))
        runtime = _runtime(engine, plan, deps=hook_deps, container=container)

        async with runtime:
            events = await _collect(runtime, identity)

        error = events[-1]
        assert isinstance(error, ErrorEvent)
        assert error.code is AgentRunErrorCode.RUN_TIMEOUT
        assert error.interaction_id is not None
        assert recorder.calls == []

    async def test_no_ejecuta_el_hook_cuando_el_consumidor_abandona_el_stream(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Leaving after the first delta closes the generators; nothing is recorded."""
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        runtime = _runtime(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with runtime, runtime.run_stream(_AGENT, "prompt", identity=identity) as stream:
            async for event in stream:
                assert isinstance(event, TextDeltaEvent)
                break

        assert recorder.calls == []
        assert hook_deps.uow.log == []

    async def test_cierra_con_provider_unavailable_cuando_el_motor_no_emite_evento_terminal(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A stream exhausted without a terminal event still ends named; no hook runs."""
        engine = ScriptedEngine(script=(TextDeltaEvent(text="ok"),))
        runtime = _runtime(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with runtime:
            with pytest.raises(AgentRunError) as failure:
                await runtime.run(_AGENT, "prompt", identity=identity)

        assert failure.value.code is AgentRunErrorCode.PROVIDER_UNAVAILABLE
        assert failure.value.interaction_id is not None
        assert _AGENT in str(failure.value)
        assert recorder.calls == []

    async def test_emite_un_solo_error_cuando_el_motor_no_emite_evento_terminal_en_stream(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Exactly one ``error`` event closes the stream, carrying the id."""
        engine = ScriptedEngine(script=(TextDeltaEvent(text="ok"),))
        runtime = _runtime(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with runtime:
            events = await _collect(runtime, identity)

        terminal = [event for event in events if isinstance(event, ErrorEvent | FinalEvent)]
        assert len(terminal) == 1
        error = terminal[0]
        assert isinstance(error, ErrorEvent)
        assert error.code is AgentRunErrorCode.PROVIDER_UNAVAILABLE
        assert error.interaction_id is not None
        assert recorder.calls == []


# ---------------------------------------------------------------------------
# AC10 — cancellation during the hook
# ---------------------------------------------------------------------------


class TestCancelacionDuranteElHook:
    async def test_completa_el_registro_una_vez_cuando_cancelan_al_consumidor(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The shielded hook commits, the consumer sees its cancellation, the permit frees."""
        recorder.gate = asyncio.Event()
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        plan = _hooked_plan(RecordAnswer)
        runtime = _runtime(engine, plan, deps=hook_deps, container=container, max_concurrent_runs=1)

        async with runtime:
            consumer = asyncio.create_task(runtime.run(_AGENT, "prompt", identity=identity))
            await recorder.entered.wait()
            consumer.cancel()
            recorder.gate.set()
            with pytest.raises(asyncio.CancelledError):
                await consumer
            assert len(recorder.calls) == 1
            assert hook_deps.uow.log == ["begin", "commit"]
            assert recorder.cancelled is False

            recorder.gate = None
            result = await runtime.run(_AGENT, "prompt", identity=identity)

        assert result.hook_result is not None
        assert len(recorder.calls) == 2

    async def test_cancela_el_hook_cuando_cancelan_al_consumidor_por_segunda_vez(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A second cancel while waiting for the hook to settle cuts the hook, not just the wait."""
        recorder.gate = asyncio.Event()
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        runtime = _runtime(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with runtime:
            consumer = asyncio.create_task(runtime.run(_AGENT, "prompt", identity=identity))
            await recorder.entered.wait()
            consumer.cancel()
            await asyncio.sleep(0)
            consumer.cancel()
            with pytest.raises(asyncio.CancelledError):
                await consumer
            await asyncio.sleep(0)

        assert recorder.cancelled is True
        assert hook_deps.uow.log == ["begin", "rollback"]


# ---------------------------------------------------------------------------
# AC11 — streaming order
# ---------------------------------------------------------------------------


class TestOrdenEnStream:
    async def test_ejecuta_el_hook_antes_de_entregar_el_final_cuando_se_hace_stream(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The record exists before the consumer sees ``final``, which carries the result."""
        engine = ScriptedEngine(script=default_script(REPORT))
        runtime = _runtime(engine, _hooked_plan(RecordTriage), deps=hook_deps, container=container)

        final: object = None
        async with runtime, runtime.run_stream(_AGENT, "prompt", identity=identity) as stream:
            async for event in stream:
                recorder.timeline.append(type(event).__name__)
                final = event

        assert recorder.timeline == ["TextDeltaEvent", "hook", "FinalEvent"]
        assert isinstance(final, FinalEvent)
        assert final.interaction_id is not None
        assert final.hook_result == TriageRecorded(triage_id=final.interaction_id)


# ---------------------------------------------------------------------------
# AC12 (runtime half) — conversation_id
# ---------------------------------------------------------------------------


class TestConversationId:
    async def test_entrega_el_conversation_id_al_command_cuando_se_indica(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The opaque value is copied verbatim into the command."""
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        runtime = _runtime(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with runtime:
            await runtime.run(_AGENT, "prompt", identity=identity, conversation_id="c-42")

        command = recorder.calls[0].command
        assert isinstance(command, AnswerCommand)
        assert command.conversation_id == "c-42"

    async def test_entrega_none_cuando_no_se_indica_conversation_id(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Absent means ``None``; the runtime never invents one."""
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        runtime = _runtime(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with runtime:
            events = await _collect(runtime, identity)

        command = recorder.calls[0].command
        assert isinstance(command, AnswerCommand)
        assert command.conversation_id is None
        assert isinstance(events[-1], FinalEvent)

    async def test_rechaza_con_value_error_cuando_conversation_id_excede_el_limite(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """An out-of-bound value is a programming error, refused before admission."""
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        runtime = _runtime(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with runtime:
            with pytest.raises(ValueError, match="conversation_id"):
                await runtime.run(_AGENT, "prompt", identity=identity, conversation_id="x" * 129)
            with pytest.raises(ValueError, match="conversation_id"):
                async with runtime.run_stream(
                    _AGENT, "prompt", identity=identity, conversation_id=""
                ):
                    pass
            await runtime.run(_AGENT, "prompt", identity=identity, conversation_id="x" * 128)

        assert engine.stream_count == 1
        assert len(recorder.calls) == 1


# ---------------------------------------------------------------------------
# AC14 — start-up probe
# ---------------------------------------------------------------------------


class TestSondaDeArranque:
    async def test_rechaza_el_arranque_cuando_hay_hook_y_el_bundle_no_lleva_invoker(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
        lifecycle_log: list[str],
    ) -> None:
        """The probe names the agents and runs before any client opens."""
        clients = {
            "tools": StubMcpClient(label="tools", session=RecordingMcpSession(), log=lifecycle_log)
        }
        plan = _hooked_plan(RecordAnswer, capabilities=(make_mcp_capability("tools"),))
        runtime = AgentRuntime(
            plans=[plan],
            config=make_ai_config(mcp_servers=make_mcp_servers("tools")),
            engine_provider=CountingEngineProvider(),  # type: ignore[arg-type]
            deps=deps,
            container=container,
            mcp_client_factory=mcp_client_factory(clients),  # type: ignore[arg-type]
        )

        with pytest.raises(AgentCompilationError) as failure:
            async with runtime:
                pass

        issues = failure.value.issues
        assert [issue.code for issue in issues] == [AgentErrorCode.ON_OUTPUT_INVOKER_MISSING]
        assert _AGENT in issues[0].message
        assert lifecycle_log == []

    async def test_rechaza_el_arranque_cuando_el_invoker_del_bundle_no_esta_ligado_a_un_caller(
        self,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """An invoker carrying no identity would run every hook as nobody."""
        runtime = _runtime(
            ScriptedEngine(),
            _hooked_plan(RecordAnswer),
            deps=_UnboundDepsFactory(hook_deps),
            container=container,
        )

        with pytest.raises(AgentCompilationError) as failure:
            async with runtime:
                pass

        issues = failure.value.issues
        assert [issue.code for issue in issues] == [AgentErrorCode.ON_OUTPUT_INVOKER_MISSING]
        assert "not bound" in issues[0].message
        assert _AGENT in issues[0].message

    async def test_arranca_con_normalidad_cuando_no_hay_hook(
        self,
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The dict bundle stays valid for a deployment without hooks."""
        engine = ScriptedEngine()
        runtime = _runtime(engine, make_plan(_AGENT), deps=deps, container=container)

        async with runtime:
            result = await runtime.run(_AGENT, "prompt", identity=identity)

        assert result.interaction_id is not None
