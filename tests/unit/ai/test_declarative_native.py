"""Authored ``native`` capability of spec version 1 (``loom.ai.declarative``).

``- kind: native, tool: <name>`` grants a tool the model provider runs in its own
infrastructure (030/H1). These tests pin the artifact half of the feature: the
declaration decodes into :class:`NativeCapability`, the tool vocabulary is closed,
unknown keys are rejected the same way every other struct does, and the published
JSON Schema accepts and rejects the same documents the decoder does.

Assertions are made on error *codes*, never on messages, except for the field path
of a rejected tool name: an author must be pointed at ``capabilities[i].tool``.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from loom.ai.declarative import (
    NATIVE_TOOLS,
    AgentSpecV1,
    NativeCapability,
    agent_spec_json_schema,
    decode_spec,
)
from loom.ai.errors import AgentCompilationError, AgentErrorCode

_TOOL = "web_search"
_UNKNOWN_TOOL = "telepathy"


def _payload(capability: dict[str, Any]) -> dict[str, Any]:
    """Build a minimal v1 artifact declaring exactly one capability."""
    return {
        "spec_version": 1,
        "name": "incident-triage",
        "description": "Clasifica un incidente a partir de su descripcion.",
        "instructions": "Lee la descripcion del incidente y devuelve un informe.",
        "output": {"kind": "json_schema", "schema": {"type": "object"}},
        "capabilities": [capability],
    }


def _encode(payload: dict[str, Any]) -> bytes:
    """Render an artifact mapping as the JSON bytes that ``decode_spec`` consumes."""
    return json.dumps(payload).encode("utf-8")


def _decode(payload: dict[str, Any]) -> AgentSpecV1:
    """Decode an artifact and narrow it to the v1 struct."""
    spec = decode_spec(_encode(payload)).spec
    assert isinstance(spec, AgentSpecV1)
    return spec


def _codes(error: AgentCompilationError) -> list[AgentErrorCode]:
    """Extract the ordered issue codes carried by a compilation error."""
    return [issue.code for issue in error.issues]


def _schema_errors(payload: dict[str, Any]) -> list[str]:
    """Validate an artifact against the published schema and return the messages."""
    validator = Draft202012Validator(agent_spec_json_schema(1))
    return [error.message for error in validator.iter_errors(payload)]


def test_native_tools_publica_el_vocabulario_v1_cuando_se_importa() -> None:
    """The three names are the v1 vocabulary; a v1 name is forever (030/D1)."""
    assert NATIVE_TOOLS == ("web_search", "web_fetch", "code_execution")


@pytest.mark.parametrize("tool", NATIVE_TOOLS)
def test_decode_spec_devuelve_native_capability_cuando_el_tool_es_conocido(tool: str) -> None:
    """``kind: native, tool: t`` decodes to ``NativeCapability(tool="t")`` (030/H1)."""
    spec = _decode(_payload({"kind": "native", "tool": tool}))

    assert spec.capabilities == (NativeCapability(tool=tool),)


def test_decode_spec_falla_con_spec_malformed_cuando_el_tool_no_existe() -> None:
    """An unknown tool name is malformed and the issue points at the offending field."""
    encoded = _encode(_payload({"kind": "native", "tool": _UNKNOWN_TOOL}))

    with pytest.raises(AgentCompilationError) as exc:
        decode_spec(encoded)

    assert _codes(exc.value) == [AgentErrorCode.SPEC_MALFORMED]
    assert exc.value.issues[0].field == "capabilities[0].tool"
    assert "capabilities" in exc.value.issues[0].message


def test_decode_spec_falla_con_spec_unknown_field_cuando_native_lleva_una_clave_extra() -> None:
    """Options are not part of v1: an extra key is rejected, never dropped (FR-005)."""
    encoded = _encode(_payload({"kind": "native", "tool": _TOOL, "max_uses": 3}))

    with pytest.raises(AgentCompilationError) as exc:
        decode_spec(encoded)

    assert _codes(exc.value) == [AgentErrorCode.SPEC_UNKNOWN_FIELD]


def test_decode_spec_falla_con_spec_malformed_cuando_native_no_nombra_ningun_tool() -> None:
    """``tool`` is the whole declaration; a bare ``kind: native`` is malformed."""
    encoded = _encode(_payload({"kind": "native"}))

    with pytest.raises(AgentCompilationError) as exc:
        decode_spec(encoded)

    assert _codes(exc.value) == [AgentErrorCode.SPEC_MALFORMED]


def test_el_esquema_publicado_acepta_el_artefacto_cuando_declara_native() -> None:
    """What the decoder accepts, the published schema accepts too (030/AC-1)."""
    assert _schema_errors(_payload({"kind": "native", "tool": _TOOL})) == []


def test_el_esquema_publicado_rechaza_native_cuando_el_tool_no_existe() -> None:
    """The schema closes the vocabulary with ``enum``, so editors reject it offline."""
    assert _schema_errors(_payload({"kind": "native", "tool": _UNKNOWN_TOOL})) != []


def test_el_esquema_publicado_rechaza_native_cuando_lleva_una_clave_extra() -> None:
    """The schema is as strict as the struct: unknown keys inside ``native`` fail."""
    assert _schema_errors(_payload({"kind": "native", "tool": _TOOL, "max_uses": 3})) != []


def test_el_esquema_publicado_rechaza_native_cuando_no_nombra_ningun_tool() -> None:
    """``tool`` is required by the schema exactly as it is by the struct."""
    assert _schema_errors(_payload({"kind": "native"})) != []
