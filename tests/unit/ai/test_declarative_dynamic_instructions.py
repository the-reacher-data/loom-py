"""Authored ``dynamic_instructions`` of spec version 1 (``loom.ai.declarative``).

``dynamic_instructions`` names the application code that contributes prompt
material per request (011/L18). These tests pin the artifact half of that
decision: it is a *block* with a factory reference and optional parameters —
the shape a ``kind: python`` capability already uses, because it genuinely has
two parts — it is optional, its reference is constrained by the same pattern
every other symbol reference uses, and the published JSON Schema accepts and
rejects exactly what the decoder does.

Assertions are made on error *codes*, never on messages: the codes are the
public contract, the wording is not.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from loom.ai.declarative import AgentSpecV1, agent_spec_json_schema, decode_spec
from loom.ai.errors import AgentCompilationError, AgentErrorCode

_FACTORY_REF = "myapp.agents.prompts:build_checklist"


def _payload_without_block() -> dict[str, Any]:
    """Build a minimal v1 artifact contributing no instructions per request."""
    return {
        "spec_version": 1,
        "name": "incident-triage",
        "description": "Classifies an incident from its description.",
        "instructions": "Classify the incident described by the user.",
        "output": {"kind": "type_ref", "ref": "myapp.domain.triage:TriageReport"},
    }


def _payload_with_block(block: Any = None) -> dict[str, Any]:
    """Build the same artifact declaring the block, as the spec's example writes it."""
    declared = block if block is not None else {"factory": _FACTORY_REF, "params": {"locale": "en"}}
    return {**_payload_without_block(), "dynamic_instructions": declared}


def _decode(payload: dict[str, Any]) -> AgentSpecV1:
    """Decode an artifact and narrow it to the v1 struct."""
    spec = decode_spec(json.dumps(payload).encode("utf-8")).spec
    assert isinstance(spec, AgentSpecV1)
    return spec


def _codes(error: AgentCompilationError) -> list[AgentErrorCode]:
    """Extract the ordered issue codes carried by a compilation error."""
    return [issue.code for issue in error.issues]


def _schema_errors(payload: dict[str, Any]) -> list[str]:
    """Validate an artifact against the published schema and return the messages."""
    validator = Draft202012Validator(agent_spec_json_schema(1))
    return [error.message for error in validator.iter_errors(payload)]


def test_decode_spec_devuelve_el_bloque_cuando_se_declaran_instrucciones_dinamicas() -> None:
    """The value has two parts, so it decodes to two: a reference and its parameters."""
    block = _decode(_payload_with_block()).dynamic_instructions

    assert block is not None
    assert (block.factory, block.params) == (_FACTORY_REF, {"locale": "en"})


def test_decode_spec_deja_las_instrucciones_dinamicas_en_none_cuando_no_se_declaran() -> None:
    """The field is optional and additive: an artifact without it keeps decoding."""
    assert _decode(_payload_without_block()).dynamic_instructions is None


def test_decode_spec_deja_los_params_vacios_cuando_solo_se_declara_la_factoria() -> None:
    """A factory that needs no settings declares none; ``params`` is not required."""
    block = _decode(_payload_with_block({"factory": _FACTORY_REF})).dynamic_instructions

    assert block is not None
    assert block.params == {}


def test_las_instrucciones_literales_siguen_siendo_obligatorias_cuando_hay_bloque() -> None:
    """The block never replaces ``instructions``: the literal stays mandatory."""
    payload = _payload_with_block()
    del payload["instructions"]

    with pytest.raises(AgentCompilationError) as exc:
        decode_spec(json.dumps(payload).encode("utf-8"))

    assert _codes(exc.value) == [AgentErrorCode.SPEC_MALFORMED]


@pytest.mark.parametrize(
    "ref",
    ["myapp/agents/prompts.py", "myapp.agents.prompts", "myapp.agents.prompts:", ":build"],
    ids=["filesystem_path", "no_symbol", "empty_symbol", "no_module"],
)
def test_decode_spec_falla_cuando_la_factoria_no_es_una_referencia_module_symbol(ref: str) -> None:
    """The pattern is the one every symbol reference uses, so a path cannot be written."""
    with pytest.raises(AgentCompilationError) as exc:
        decode_spec(json.dumps(_payload_with_block({"factory": ref})).encode("utf-8"))

    assert _codes(exc.value) == [AgentErrorCode.SPEC_MALFORMED]


def test_decode_spec_falla_cuando_las_instrucciones_dinamicas_son_una_cadena_suelta() -> None:
    """A bare reference is a different shape and is not part of the format."""
    with pytest.raises(AgentCompilationError):
        decode_spec(json.dumps(_payload_with_block(_FACTORY_REF)).encode("utf-8"))


def test_decode_spec_falla_cuando_el_bloque_lleva_una_clave_desconocida() -> None:
    """The block forbids unknown fields, so a typo is a decoding failure."""
    block = {"factory": _FACTORY_REF, "parameters": {"locale": "en"}}

    with pytest.raises(AgentCompilationError):
        decode_spec(json.dumps(_payload_with_block(block)).encode("utf-8"))


def test_el_esquema_publicado_acepta_el_artefacto_cuando_declara_el_bloque() -> None:
    """What the decoder accepts, the published schema accepts too."""
    assert _schema_errors(_payload_with_block()) == []


@pytest.mark.parametrize(
    "block",
    [
        _FACTORY_REF,
        {"factory": "myapp/agents/prompts.py"},
        {"params": {"locale": "en"}},
        {"factory": _FACTORY_REF, "parameters": {}},
    ],
    ids=["loose_string", "filesystem_path", "no_factory", "unknown_key"],
)
def test_el_esquema_publicado_rechaza_el_bloque_cuando_no_tiene_la_forma_declarada(
    block: Any,
) -> None:
    """The schema is as strict as the struct: same shape, same refusals."""
    assert _schema_errors(_payload_with_block(block)) != []
