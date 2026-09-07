"""``loader_command``, ``_as_history`` and ``failure_error``: the loader side of a run (006 T5)."""

from __future__ import annotations

import logging

import msgspec
import pytest

from loom.ai.compiler._plan import CONVERSATION_CONTEXT_FIELDS, AgentPlan, CompiledOutput
from loom.ai.declarative import PolicySpec
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.inference import InferenceTarget
from loom.ai.runtime._bounded import RunContext, failure_error
from loom.ai.runtime._conversation import _as_history, loader_command
from loom.core.command import Command
from loom.core.errors import Forbidden
from loom.core.identity import Identity

_ALL_NAMES = frozenset(CONVERSATION_CONTEXT_FIELDS)
_DENIED_MESSAGE = "the caller is not allowed to perform this operation"


class _StrictCommand(Command, frozen=True, kw_only=True, forbid_unknown_fields=True):
    conversation_id: str


def _plan() -> AgentPlan:
    return AgentPlan(
        name="incident-triage",
        description="test agent",
        instructions="answer",
        spec_version=1,
        inference=InferenceTarget(provider="fake", model="fake-model"),
        output=CompiledOutput(schema={"type": "object"}, decoder=msgspec.json.Decoder(dict)),
        capabilities=(),
        policies=PolicySpec(),
        metadata={},
    )


@pytest.fixture
def run() -> RunContext:
    """One admitted run of a verified caller continuing a conversation."""
    return RunContext(
        plan=_plan(),
        identity=Identity(subject="user-1", roles=("analyst",), mechanism="test"),
        interaction_id="int-1",
        conversation_id="c-42",
    )


class TestLoaderCommand:
    def test_ofrece_los_cinco_valores_del_contexto_cuando_el_input_los_acepta(
        self, run: RunContext
    ) -> None:
        """Every offered name carries the run's, the identity's or the plan's value."""
        command = loader_command(run, _ALL_NAMES)

        assert command == {
            "conversation_id": "c-42",
            "interaction_id": "int-1",
            "subject": "user-1",
            "mechanism": "test",
            "agent": "incident-triage",
        }

    def test_filtra_a_los_nombres_aceptados_cuando_el_input_declara_menos(
        self, run: RunContext
    ) -> None:
        """Names the Input does not declare never reach it."""
        command = loader_command(run, frozenset({"conversation_id", "agent"}))

        assert command == {"conversation_id": "c-42", "agent": "incident-triage"}

    def test_decodifica_un_command_estricto_cuando_se_filtra_a_sus_nombres(
        self, run: RunContext
    ) -> None:
        """A ``forbid_unknown_fields`` Command declaring only ``conversation_id`` decodes."""
        accepted = frozenset(info.name for info in msgspec.structs.fields(_StrictCommand))

        instance, seen = _StrictCommand.from_payload(loader_command(run, accepted))

        assert instance == _StrictCommand(conversation_id="c-42")
        assert seen == frozenset({"conversation_id"})

    def test_ofrece_exactamente_los_nombres_que_el_compilador_promete(
        self, run: RunContext
    ) -> None:
        """The run-time command and the compile-time offer are one contract."""
        command = loader_command(run, _ALL_NAMES)

        assert set(command) == set(CONVERSATION_CONTEXT_FIELDS)


class TestFailureError:
    def test_conserva_codigo_y_mensaje_cuando_ya_es_un_agent_run_error(
        self, run: RunContext
    ) -> None:
        """An ``AgentRunError`` keeps both its code and its text and gains the run's id."""
        original = AgentRunError(AgentRunErrorCode.UNAUTHORIZED, "no invoker bound")

        error = failure_error(
            original,
            run,
            code=AgentRunErrorCode.CONVERSATION_LOAD_FAILED,
            message="fixed",
            what="conversation loader",
        )

        assert error.code is AgentRunErrorCode.UNAUTHORIZED
        assert str(error) == "no invoker bound"
        assert error.interaction_id == "int-1"
        assert error.usage is None

    def test_mapea_una_denegacion_a_unauthorized_cuando_las_reglas_rechazan(
        self, run: RunContext
    ) -> None:
        """A denial keeps its meaning and answers the fixed denial text."""
        error = failure_error(
            Forbidden("thread c-42 belongs to another subject"),
            run,
            code=AgentRunErrorCode.CONVERSATION_LOAD_FAILED,
            message="fixed",
            what="conversation loader",
        )

        assert error.code is AgentRunErrorCode.UNAUTHORIZED
        assert str(error) == _DENIED_MESSAGE
        assert error.interaction_id == "int-1"

    def test_usa_el_codigo_y_texto_dados_cuando_la_excepcion_es_generica(
        self, run: RunContext, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Anything else is logged server-side and answered with the given code and text."""
        with caplog.at_level(logging.ERROR, logger="loom.ai.runtime._bounded"):
            error = failure_error(
                ValueError("secret detail"),
                run,
                code=AgentRunErrorCode.CONVERSATION_LOAD_FAILED,
                message="fixed",
                what="conversation loader",
            )

        assert error.code is AgentRunErrorCode.CONVERSATION_LOAD_FAILED
        assert str(error) == "fixed"
        assert error.interaction_id == "int-1"
        assert "secret detail" not in str(error)
        assert any(
            "conversation loader" in record.getMessage() and "int-1" in record.getMessage()
            for record in caplog.records
        )


class TestAsHistory:
    def test_devuelve_none_cuando_el_loader_no_devuelve_nada(self) -> None:
        """``None`` is single-shot and is never measured."""
        assert _as_history(None, 1024) is None

    def test_devuelve_el_mismo_objeto_cuando_los_bytes_igualan_el_limite(self) -> None:
        """Bytes exactly at the bound pass through untouched."""
        history = b"x" * 1024

        assert _as_history(history, 1024) is history

    def test_lanza_value_error_con_ambos_tamanos_cuando_los_bytes_superan_el_limite(self) -> None:
        """One byte over the bound is refused and the message names both sizes."""
        with pytest.raises(ValueError, match=r"1025(?s:.*)1024"):
            _as_history(b"x" * 1025, 1024)

    def test_lanza_type_error_cuando_el_loader_devuelve_un_str(self) -> None:
        """A ``str`` is never coerced to bytes."""
        with pytest.raises(TypeError):
            _as_history("not bytes", 1024)
