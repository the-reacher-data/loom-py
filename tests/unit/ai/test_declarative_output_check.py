"""Authored ``output_check`` of spec version 1 (``loom.ai.declarative``).

``output_check`` names the rule an answer must satisfy beyond its schema
(011/L17). These tests pin the artifact half of that decision: it is a bare
``module:symbol`` string rather than a block, it is optional, it is constrained
by the same pattern every other symbol reference uses — so a filesystem path is
not representable — and the published JSON Schema accepts and rejects exactly
what the decoder does.

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

_CHECK_REF = "myapp.agents.checks:report_is_complete"


def _payload_without_check() -> dict[str, Any]:
    """Build a minimal v1 artifact declaring no answer rule."""
    return {
        "spec_version": 1,
        "name": "incident-triage",
        "description": "Classifies an incident from its description.",
        "instructions": "Classify the incident described by the user.",
        "output": {"kind": "type_ref", "ref": "myapp.domain.triage:TriageReport"},
    }


def _payload_with_check(ref: Any = _CHECK_REF) -> dict[str, Any]:
    """Build the same artifact declaring the rule, as the spec's example writes it."""
    return {**_payload_without_check(), "output_check": ref}


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


def test_decode_spec_devuelve_la_referencia_suelta_cuando_se_declara_output_check() -> None:
    """The value is the string itself: no block, no tag, nothing to unwrap."""
    assert _decode(_payload_with_check()).output_check == _CHECK_REF


def test_decode_spec_deja_output_check_en_none_cuando_no_se_declara() -> None:
    """The field is optional and additive: an artifact without it keeps decoding."""
    assert _decode(_payload_without_check()).output_check is None


@pytest.mark.parametrize(
    "ref",
    ["myapp/agents/checks.py", "myapp.agents.checks", "myapp.agents.checks:", ":report"],
    ids=["filesystem_path", "no_symbol", "empty_symbol", "no_module"],
)
def test_decode_spec_falla_cuando_output_check_no_es_una_referencia_module_symbol(
    ref: str,
) -> None:
    """The pattern is the one every symbol reference uses, so a path cannot be written."""
    with pytest.raises(AgentCompilationError) as exc:
        decode_spec(json.dumps(_payload_with_check(ref)).encode("utf-8"))

    assert _codes(exc.value) == [AgentErrorCode.SPEC_MALFORMED]


def test_decode_spec_falla_cuando_output_check_se_declara_como_bloque() -> None:
    """A ``{ref: ...}`` block is a different shape and is not part of the format."""
    with pytest.raises(AgentCompilationError):
        decode_spec(json.dumps(_payload_with_check({"ref": _CHECK_REF})).encode("utf-8"))


def test_el_esquema_publicado_acepta_el_artefacto_cuando_declara_output_check() -> None:
    """What the decoder accepts, the published schema accepts too."""
    assert _schema_errors(_payload_with_check()) == []


def test_el_esquema_publicado_rechaza_output_check_cuando_no_es_una_referencia() -> None:
    """The schema is as strict as the struct: a path fails validation as well."""
    assert _schema_errors(_payload_with_check("myapp/agents/checks.py")) != []
