"""Contract of the model-as-actor public surface (PR1, T101-T103).

Pins the shape the marker factory relies on before any resolution exists:
``AgentAnswer`` is a frozen, per-run carrier; ``AgentHandle.run`` exposes
exactly the four per-run arguments the spec allows, split across two
overloads; ``McpHandle`` offers a typed and an untyped call rather than one
call with an optional type; ``SqlGrantHandle`` exposes one bounded query.
"""

from __future__ import annotations

import inspect
import typing

import pytest

from loom.ai.abc import AgentAnswer, AgentHandle, AgentUsage, McpHandle, SqlGrantHandle


def _sample_usage() -> AgentUsage:
    return AgentUsage(input_tokens=10, output_tokens=5, requests=1, duration_ms=100)


class TestAgentAnswer:
    def test_is_immutable_once_constructed(self) -> None:
        respuesta = AgentAnswer(output="hola", usage=_sample_usage(), interaction_id="run-1")

        with pytest.raises(AttributeError):
            respuesta.output = "cambiada"  # type: ignore[misc]

    def test_usage_belongs_to_its_own_run_and_is_not_merged(self) -> None:
        primera = AgentAnswer(output=1, usage=_sample_usage(), interaction_id="run-1")
        segunda = AgentAnswer(output=2, usage=_sample_usage(), interaction_id="run-2")

        assert primera.usage is not segunda.usage
        assert primera.usage.requests == segunda.usage.requests == 1

    def test_interaction_id_is_optional(self) -> None:
        respuesta = AgentAnswer(output="hola", usage=_sample_usage())

        assert respuesta.interaction_id is None


class TestAgentHandleRun:
    def _signature_of(self, nombre: str) -> inspect.Signature:
        return inspect.signature(getattr(AgentHandle, nombre))

    def test_run_exposes_exactly_the_four_per_run_parameters(self) -> None:
        parametros = set(self._signature_of("run").parameters) - {"self"}

        assert parametros == {"prompt", "expect", "conversation_id", "state"}

    def test_run_text_does_not_accept_expect(self) -> None:
        parametros = set(self._signature_of("run_text").parameters) - {"self"}

        assert parametros == {"prompt", "conversation_id", "state"}

    def test_expect_and_conversation_id_are_keyword_only(self) -> None:
        firma = self._signature_of("run")

        assert firma.parameters["expect"].kind is inspect.Parameter.KEYWORD_ONLY
        assert firma.parameters["conversation_id"].kind is inspect.Parameter.KEYWORD_ONLY
        assert firma.parameters["state"].kind is inspect.Parameter.KEYWORD_ONLY
        assert firma.parameters["state"].default is None

    def test_run_has_two_declared_overloads(self) -> None:
        """``typing.overload`` registers both variants under the same name."""
        registered = typing.get_overloads(AgentHandle.run)

        assert len(registered) == 2


class TestAgentHandleGrants:
    def test_mcp_and_sql_return_their_own_handles(self) -> None:
        parametros_mcp = set(inspect.signature(AgentHandle.mcp).parameters) - {"self"}
        parametros_sql = set(inspect.signature(AgentHandle.sql).parameters) - {"self"}

        assert parametros_mcp == {"server"}
        assert parametros_sql == {"connection"}

    def test_grants_takes_no_arguments(self) -> None:
        parametros = set(inspect.signature(AgentHandle.grants).parameters) - {"self"}

        assert parametros == set()


class TestMcpHandle:
    def test_call_requires_expect_by_name(self) -> None:
        firma = inspect.signature(McpHandle.call)

        assert firma.parameters["expect"].kind is inspect.Parameter.KEYWORD_ONLY
        assert firma.parameters["expect"].default is inspect.Parameter.empty

    def test_call_untyped_has_no_expect_parameter(self) -> None:
        parametros = set(inspect.signature(McpHandle.call_untyped).parameters)

        assert "expect" not in parametros


class TestSqlGrantHandle:
    def test_query_exposes_statement_and_parameters(self) -> None:
        parametros = set(inspect.signature(SqlGrantHandle.query).parameters) - {"self"}

        assert parametros == {"statement", "parameters"}
