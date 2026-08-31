"""Envelope decoding contract for Tier-1 agent artifacts (``loom.ai.declarative``).

The two-phase decoder is the only supported entry point for authored artifacts:
phase 1 reads the envelope (``spec_version``) and phase 2 decodes the payload with
the struct registered for that version. These tests pin the observable contract of
``decode_spec``: which failures are fatal, which are merely reported as issues, and
which defaults an artifact inherits when it stays silent.

Assertions are made on error *codes*, never on messages: the codes are the public
contract, the wording is not.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from loom.ai.declarative import AgentSpecV1, DecodedSpec, decode_spec
from loom.ai.errors import AgentCompilationError, AgentErrorCode

_OUTPUT_JSON_SCHEMA: dict[str, Any] = {
    "kind": "json_schema",
    "schema": {"type": "object", "properties": {"answer": {"type": "string"}}},
}


def _valid_payload() -> dict[str, Any]:
    """Build the minimal artifact accepted by spec version 1."""
    return {
        "spec_version": 1,
        "name": "support-triage",
        "description": "Clasifica tickets de soporte entrantes.",
        "instructions": "Lee el ticket y devuelve la categoria adecuada.",
        "output": dict(_OUTPUT_JSON_SCHEMA),
    }


def _encode(payload: dict[str, Any]) -> bytes:
    """Render an artifact mapping as the JSON bytes that ``decode_spec`` consumes."""
    return json.dumps(payload).encode("utf-8")


def _decode_valid() -> DecodedSpec:
    """Decode the minimal valid artifact through the public entry point."""
    return decode_spec(_encode(_valid_payload()))


def _codes(error: AgentCompilationError) -> list[AgentErrorCode]:
    """Extract the ordered issue codes carried by a compilation error."""
    return [issue.code for issue in error.issues]


def test_decode_spec_devuelve_agent_spec_v1_cuando_el_artefacto_es_valido() -> None:
    """A well-formed v1 artifact decodes into the v1 struct."""
    decoded = _decode_valid()

    assert isinstance(decoded.spec, AgentSpecV1)


def test_decode_spec_conserva_los_campos_declarados_cuando_el_artefacto_es_valido() -> None:
    """Declared envelope fields survive decoding unchanged."""
    decoded = _decode_valid()

    assert (decoded.spec.spec_version, decoded.spec.name, decoded.spec.description) == (
        1,
        "support-triage",
        "Clasifica tickets de soporte entrantes.",
    )


def test_decode_spec_no_reporta_incidencias_cuando_el_artefacto_es_valido() -> None:
    """A supported, current spec version produces no non-fatal issues."""
    decoded = _decode_valid()

    assert decoded.issues == ()


def test_decode_spec_falla_con_spec_unknown_field_cuando_hay_una_clave_engine() -> None:
    """Deployment vocabulary in a Tier-1 artifact is rejected, never ignored."""
    payload = _valid_payload()
    payload["engine"] = "pydantic-ai"

    with pytest.raises(AgentCompilationError) as exc:
        decode_spec(_encode(payload))

    assert _codes(exc.value) == [AgentErrorCode.SPEC_UNKNOWN_FIELD]


def test_decode_spec_falla_con_spec_version_unsupported_cuando_la_version_es_dos() -> None:
    """An envelope version outside the registry is fatal."""
    payload = _valid_payload()
    payload["spec_version"] = 2

    with pytest.raises(AgentCompilationError) as exc:
        decode_spec(_encode(payload))

    assert _codes(exc.value) == [AgentErrorCode.SPEC_VERSION_UNSUPPORTED]


def test_decode_spec_falla_con_spec_version_missing_cuando_falta_la_version() -> None:
    """An artifact without ``spec_version`` cannot be routed to any struct."""
    payload = _valid_payload()
    del payload["spec_version"]

    with pytest.raises(AgentCompilationError) as exc:
        decode_spec(_encode(payload))

    assert _codes(exc.value) == [AgentErrorCode.SPEC_VERSION_MISSING]


def test_decode_spec_falla_con_agent_name_invalid_cuando_el_nombre_no_cumple_el_patron() -> None:
    """The agent name pattern is enforced during payload decoding."""
    payload = _valid_payload()
    payload["name"] = "Support Triage!"

    with pytest.raises(AgentCompilationError) as exc:
        decode_spec(_encode(payload))

    assert _codes(exc.value) == [AgentErrorCode.AGENT_NAME_INVALID]


def test_decode_spec_devuelve_spec_version_deprecated_cuando_existe_una_version_posterior() -> None:
    """A supported but superseded version is accepted with a deprecation issue."""
    decoded = decode_spec(
        _encode(_valid_payload()),
        versions={1: AgentSpecV1, 2: AgentSpecV1},
    )

    assert [issue.code for issue in decoded.issues] == [AgentErrorCode.SPEC_VERSION_DEPRECATED]


def test_decode_spec_decodifica_el_artefacto_cuando_la_version_esta_deprecada() -> None:
    """Deprecation is an issue, not a failure: the spec is still returned."""
    decoded = decode_spec(
        _encode(_valid_payload()),
        versions={1: AgentSpecV1, 2: AgentSpecV1},
    )

    assert isinstance(decoded.spec, AgentSpecV1)


def test_decode_spec_aplica_model_role_por_defecto_cuando_no_se_declara() -> None:
    """An artifact that omits ``model_role`` binds to the default role."""
    decoded = _decode_valid()

    assert decoded.spec.model_role == "default"


def test_decode_spec_aplica_run_timeout_por_defecto_cuando_no_hay_policies() -> None:
    """Omitted policies materialise with their documented defaults."""
    decoded = _decode_valid()

    assert decoded.spec.policies.run_timeout_ms == 120000


def test_decode_spec_deja_capabilities_vacias_cuando_no_se_declaran() -> None:
    """An artifact without capabilities decodes to an empty tuple, not ``None``."""
    decoded = _decode_valid()

    assert decoded.spec.capabilities == ()
