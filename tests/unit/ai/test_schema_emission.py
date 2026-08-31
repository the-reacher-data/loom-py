"""Contract between the emitted JSON Schema and the committed schema file.

``contracts/agent-spec-v1.schema.json`` is the published artifact contract (FR-009).
The byte-for-byte test keeps it from drifting from the Tier-1 structs: the schema the
code emits and the file consumers validate against must be the same document, byte for
byte. Regenerate the file from ``_schema.py`` when the structs legitimately change —
never edit it by hand to make this test pass.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from loom.ai.declarative import agent_spec_json_schema

_REPO_ROOT: Path = Path(__file__).resolve().parents[3]
_CONTRACT_PATH: Path = (
    _REPO_ROOT / "specs" / "001-ai-agent-layer" / "contracts" / "agent-spec-v1.schema.json"
)


def _emitted() -> dict[str, Any]:
    """Build the schema document Loom publishes for spec version 1."""
    return agent_spec_json_schema(1)


def _committed() -> dict[str, Any]:
    """Parse the committed schema contract file."""
    document: dict[str, Any] = json.loads(_CONTRACT_PATH.read_text(encoding="utf-8"))
    return document


def _serialise(document: dict[str, Any]) -> str:
    """Render a schema document in the canonical published form."""
    return json.dumps(document, indent=2, ensure_ascii=False) + "\n"


def _sql_variant(document: dict[str, Any]) -> dict[str, Any]:
    """Return the ``sql`` variant of the capability union of a schema document."""
    variants: list[dict[str, Any]] = document["$defs"]["capability"]["oneOf"]
    return next(variant for variant in variants if variant["properties"]["kind"]["const"] == "sql")


def _policy_properties(document: dict[str, Any]) -> dict[str, Any]:
    """Return the property map of the ``policies`` definition of a schema document."""
    properties: dict[str, Any] = document["$defs"]["policies"]["properties"]
    return properties


def _ordered(value: Any) -> Any:
    """Project a JSON document onto an order-sensitive structure.

    Mappings become lists of key/value pairs, so comparing two projections detects a
    reordered key, not only a changed value.
    """
    if isinstance(value, dict):
        return [(key, _ordered(item)) for key, item in value.items()]
    if isinstance(value, list):
        return [_ordered(item) for item in value]
    return value


def test_el_esquema_emitido_es_identico_al_contrato_publicado_cuando_se_serializa() -> None:
    """The emitted schema should be byte-for-byte the committed contract file."""
    assert _serialise(_emitted()) == _CONTRACT_PATH.read_text(encoding="utf-8")


def test_las_politicas_emitidas_declaran_run_timeout_ms_cuando_se_construye_el_esquema() -> None:
    """First known divergence: the emitted policies carry ``run_timeout_ms``."""
    assert _policy_properties(_emitted())["run_timeout_ms"] == {
        "type": "integer",
        "minimum": 1000,
        "maximum": 1800000,
        "default": 120000,
    }


def test_la_capacidad_sql_emitida_exige_las_cotas_de_resultado_cuando_se_construye() -> None:
    """Second known divergence: the emitted ``sql`` variant requires the bounds."""
    assert _sql_variant(_emitted())["required"] == [
        "kind",
        "connection",
        "max_rows",
        "max_result_bytes",
    ]


def test_agent_spec_json_schema_falla_cuando_la_version_no_esta_publicada() -> None:
    """An unpublished spec version is a programming error, not an empty document."""
    with pytest.raises(ValueError, match="spec version 2"):
        agent_spec_json_schema(2)
