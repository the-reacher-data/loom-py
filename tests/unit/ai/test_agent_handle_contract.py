"""Contract of the model-as-actor public surface (PR1, T101-T103).

Pins the shape the marker factory relies on before any resolution exists:
``AgentAnswer`` is a frozen, per-run carrier; ``AgentHandle.run`` exposes
exactly the three per-run arguments the spec allows, split across two
overloads; ``McpHandle`` offers a typed and an untyped call rather than one
call with an optional type; ``SqlGrantHandle`` exposes one bounded query.
"""

from __future__ import annotations

import inspect
import typing

import pytest

from loom.ai.abc import AgentAnswer, AgentHandle, AgentUsage, McpHandle, SqlGrantHandle


def _uso_de_ejemplo() -> AgentUsage:
    return AgentUsage(input_tokens=10, output_tokens=5, requests=1, duration_ms=100)


class TestAgentAnswer:
    def test_es_inmutable_una_vez_construida(self) -> None:
        respuesta = AgentAnswer(output="hola", usage=_uso_de_ejemplo(), interaction_id="run-1")

        with pytest.raises(AttributeError):
            respuesta.output = "cambiada"  # type: ignore[misc]

    def test_el_uso_es_el_de_esta_corrida_y_no_se_fusiona(self) -> None:
        primera = AgentAnswer(output=1, usage=_uso_de_ejemplo(), interaction_id="run-1")
        segunda = AgentAnswer(output=2, usage=_uso_de_ejemplo(), interaction_id="run-2")

        assert primera.usage is not segunda.usage
        assert primera.usage.requests == segunda.usage.requests == 1

    def test_interaction_id_es_opcional(self) -> None:
        respuesta = AgentAnswer(output="hola", usage=_uso_de_ejemplo())

        assert respuesta.interaction_id is None


class TestAgentHandleRun:
    def _firma_de(self, nombre: str) -> inspect.Signature:
        return inspect.signature(getattr(AgentHandle, nombre))

    def test_run_expone_exactamente_los_tres_parametros_por_corrida(self) -> None:
        parametros = set(self._firma_de("run").parameters) - {"self"}

        assert parametros == {"prompt", "expect", "conversation_id"}

    def test_run_text_no_admite_expect(self) -> None:
        parametros = set(self._firma_de("run_text").parameters) - {"self"}

        assert parametros == {"prompt", "conversation_id"}

    def test_expect_y_conversation_id_son_solo_por_nombre(self) -> None:
        firma = self._firma_de("run")

        assert firma.parameters["expect"].kind is inspect.Parameter.KEYWORD_ONLY
        assert firma.parameters["conversation_id"].kind is inspect.Parameter.KEYWORD_ONLY

    def test_run_tiene_dos_sobrecargas_declaradas(self) -> None:
        """``typing.overload`` registers both variants under the same name."""
        registered = typing.get_overloads(AgentHandle.run)

        assert len(registered) == 2


class TestAgentHandleGrants:
    def test_mcp_y_sql_devuelven_sus_propios_handles(self) -> None:
        parametros_mcp = set(inspect.signature(AgentHandle.mcp).parameters) - {"self"}
        parametros_sql = set(inspect.signature(AgentHandle.sql).parameters) - {"self"}

        assert parametros_mcp == {"server"}
        assert parametros_sql == {"connection"}

    def test_grants_no_recibe_argumentos(self) -> None:
        parametros = set(inspect.signature(AgentHandle.grants).parameters) - {"self"}

        assert parametros == set()


class TestMcpHandle:
    def test_call_exige_expect_por_nombre(self) -> None:
        firma = inspect.signature(McpHandle.call)

        assert firma.parameters["expect"].kind is inspect.Parameter.KEYWORD_ONLY
        assert firma.parameters["expect"].default is inspect.Parameter.empty

    def test_call_untyped_no_tiene_parametro_expect(self) -> None:
        parametros = set(inspect.signature(McpHandle.call_untyped).parameters)

        assert "expect" not in parametros


class TestSqlGrantHandle:
    def test_query_expone_statement_y_parameters(self) -> None:
        parametros = set(inspect.signature(SqlGrantHandle.query).parameters) - {"self"}

        assert parametros == {"statement", "parameters"}
