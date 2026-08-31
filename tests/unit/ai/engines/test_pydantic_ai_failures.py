"""Failure classification, the retry rule, and event-union coverage (FR-028).

Only the infrastructure class is retried, and it is retried ``policies.retries``
times; model misbehaviour is final at this level. The last test is a canary:
it fails when pydantic-ai grows an event kind this adapter has not been taught,
so the translation never drifts silently.
"""

from __future__ import annotations

import typing

import pytest
from pydantic_ai.exceptions import ModelAPIError, ModelHTTPError, UnexpectedModelBehavior
from pydantic_ai.messages import AgentStreamEvent

from loom.ai.engines.pydantic_ai._errors import classify
from loom.ai.engines.pydantic_ai._events import IGNORED_EVENT_KINDS, MAPPED_EVENT_KINDS
from loom.ai.errors import AgentRunErrorClass, AgentRunErrorCode, is_retriable, run_error_class
from loom.ai.runtime import AgentRunError
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import NullDeps, failing_model, make_plan

_IDENTITY = Identity(subject="caller")


def _http(status: int) -> ModelHTTPError:
    return ModelHTTPError(status_code=status, model_name="scripted", body=None)


class TestClassification:
    @pytest.mark.parametrize(
        ("error", "expected"),
        [
            (_http(429), AgentRunErrorCode.PROVIDER_RATE_LIMITED),
            (_http(503), AgentRunErrorCode.PROVIDER_UNAVAILABLE),
            (_http(500), AgentRunErrorCode.PROVIDER_UNAVAILABLE),
            (_http(408), AgentRunErrorCode.PROVIDER_UNAVAILABLE),
            (_http(401), AgentRunErrorCode.UNAUTHORIZED),
            (ModelAPIError("scripted", "connection reset"), AgentRunErrorCode.PROVIDER_UNAVAILABLE),
            (
                UnexpectedModelBehavior("no output tool call"),
                AgentRunErrorCode.OUTPUT_SCHEMA_VIOLATION,
            ),
            (TimeoutError(), AgentRunErrorCode.RUN_TIMEOUT),
            (
                RuntimeError("something the adapter has never seen"),
                AgentRunErrorCode.PROVIDER_UNAVAILABLE,
            ),
        ],
    )
    def test_el_codigo_es_el_declarado_cuando_falla_el_proveedor(
        self, error: Exception, expected: AgentRunErrorCode
    ) -> None:
        """Each provider failure lands on its documented code."""
        assert classify(error) is expected

    def test_el_codigo_se_respeta_cuando_el_fallo_ya_venia_codificado(self) -> None:
        """An already-coded failure is never reclassified."""
        original = AgentRunError(AgentRunErrorCode.UNAUTHORIZED, "denied")

        assert classify(original) is AgentRunErrorCode.UNAUTHORIZED

    def test_los_fallos_de_proveedor_son_infraestructura_y_reintentables(self) -> None:
        """FR-028: both provider codes are the retriable class."""
        for code in (
            AgentRunErrorCode.PROVIDER_UNAVAILABLE,
            AgentRunErrorCode.PROVIDER_RATE_LIMITED,
        ):
            assert run_error_class(code) is AgentRunErrorClass.INFRASTRUCTURE
            assert is_retriable(code)

    def test_la_violacion_de_esquema_no_es_reintentable(self) -> None:
        """Model behaviour is final at this level; retrying it is the caller's call."""
        code = AgentRunErrorCode.OUTPUT_SCHEMA_VIOLATION

        assert run_error_class(code) is AgentRunErrorClass.MODEL_BEHAVIOUR
        assert not is_retriable(code)


class TestRetryPolicy:
    async def test_reintenta_el_numero_declarado_cuando_el_fallo_es_infraestructura(
        self,
    ) -> None:
        """``policies.retries`` extra attempts, and not one more."""
        attempts = _attempt_counter(_http(503))
        engine = _engine(retries=2, failure=attempts)

        with pytest.raises(AgentRunError):
            await engine.run("question", identity=_IDENTITY)

        assert attempts.calls == 3, "one attempt plus the two declared retries"

    async def test_no_reintenta_cuando_el_fallo_no_es_reintentable(self) -> None:
        """An authorization failure is final on the first attempt."""
        attempts = _attempt_counter(AgentRunError(AgentRunErrorCode.UNAUTHORIZED, "denied"))
        engine = _engine(retries=2, failure=attempts)

        with pytest.raises(AgentRunError) as failure:
            await engine.run("question", identity=_IDENTITY)

        assert failure.value.code is AgentRunErrorCode.UNAUTHORIZED
        assert attempts.calls == 1


class TestEventCoverage:
    def test_todos_los_eventos_del_motor_estan_clasificados(self) -> None:
        """Canary: a new engine event kind must be mapped or explicitly ignored."""
        known = MAPPED_EVENT_KINDS | IGNORED_EVENT_KINDS

        unclassified = _engine_event_kinds() - known

        assert not unclassified, f"pydantic-ai added event kinds: {sorted(unclassified)}"

    def test_no_se_clasifica_ningun_evento_inexistente(self) -> None:
        """The lists describe the engine, not a wish: no phantom kinds."""
        phantom = (MAPPED_EVENT_KINDS | IGNORED_EVENT_KINDS) - _engine_event_kinds()

        assert phantom == {"agent_run_result"}, "only the terminal event lives outside the union"


class _AttemptCounter:
    """Callable raising the same failure and counting how often it was asked."""

    def __init__(self, error: Exception) -> None:
        self._error = error
        self.calls = 0

    def __call__(self) -> Exception:
        self.calls += 1
        return self._error


def _attempt_counter(error: Exception) -> _AttemptCounter:
    return _AttemptCounter(error)


def _engine(*, retries: int, failure: _AttemptCounter) -> object:
    from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider

    provider = PydanticAIEngineProvider(model_resolver=lambda target: failing_model(failure))
    return provider.create_engine(
        make_plan(retries=retries), deps=NullDeps(), container=LoomContainer()
    )


def _engine_event_kinds() -> frozenset[str]:
    """Every ``event_kind`` the engine's stream union declares."""
    kinds: set[str] = set()
    _collect(AgentStreamEvent, kinds)
    return frozenset(kinds)


def _collect(annotation: object, kinds: set[str]) -> None:
    for argument in typing.get_args(annotation):
        if isinstance(argument, type):
            field = getattr(argument, "__dataclass_fields__", {}).get("event_kind")
            if field is not None:
                kinds.add(field.default)
            continue
        _collect(argument, kinds)
