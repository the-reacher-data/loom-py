"""Contract between the emitted JSON Schema and the schema file that ships.

``loom/ai/declarative/schemas/agent-spec-v1.schema.json`` is the published artifact
contract (FR-009). It is *shipped inside the distribution* rather than referenced from
``specs/``: that directory is not distributed (it is git-ignored working state), so a
test anchored there proves nothing on a fresh checkout and the file it guards would
never reach a consumer. The shipped copy is what a generator downloads, so the shipped
copy is what the ratchet must guard.

The byte-for-byte test keeps it from drifting from the Tier-1 structs: the schema the
code emits and the file consumers validate against must be the same document, byte for
byte. Regenerate the file from ``_schema.py`` when the structs legitimately change —
never edit it by hand to make this test pass.

The contract and the emitter agree again on ``policies.run_timeout_ms`` and on the
mandatory ``sql`` result bounds; both used to be documented divergences and are pinned
below so a regression shows up as a named failure instead of a diff.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from loom.ai.declarative import agent_spec_json_schema, agent_spec_schema_path

_CONTRACT_PATH: Path = agent_spec_schema_path(1)


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


def _capability_variant(document: dict[str, Any], kind: str) -> dict[str, Any]:
    """Return one variant of the capability union of a schema document."""
    variants: list[dict[str, Any]] = document["$defs"]["capability"]["oneOf"]
    return next(variant for variant in variants if variant["properties"]["kind"]["const"] == kind)


def _sql_variant(document: dict[str, Any]) -> dict[str, Any]:
    """Return the ``sql`` variant of the capability union of a schema document."""
    return _capability_variant(document, "sql")


def _property_names(node: Any) -> set[str]:
    """Collect every property name declared anywhere in a schema document."""
    if isinstance(node, dict):
        names: set[str] = set(node.get("properties", {}))
        for value in node.values():
            names |= _property_names(value)
        return names
    if isinstance(node, list):
        names = set()
        for item in node:
            names |= _property_names(item)
        return names
    return set()


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
    """The whole-run budget is part of the published policy vocabulary (FR-033a)."""
    assert _policy_properties(_emitted())["run_timeout_ms"] == {
        "type": "integer",
        "minimum": 1000,
        "maximum": 1800000,
        "default": 120000,
    }


def test_la_capacidad_sql_emitida_exige_las_cotas_de_resultado_cuando_se_construye() -> None:
    """An unbounded query is not representable: both bounds are required (FR-046b)."""
    assert _sql_variant(_emitted())["required"] == [
        "kind",
        "connection",
        "max_rows",
        "max_result_bytes",
    ]


def test_el_hook_de_salida_emitido_exige_el_usecase_cuando_se_construye() -> None:
    """``on_output`` names one use case and naming it is the whole declaration (002/AC1)."""
    assert _emitted()["$defs"]["on_output"]["required"] == ["usecase"]


@pytest.mark.parametrize(
    ("kind", "reference"),
    [("mcp", "server"), ("a2a", "agent"), ("skills", "library")],
)
def test_la_capacidad_emitida_exige_su_referencia_por_nombre_cuando_se_construye(
    kind: str,
    reference: str,
) -> None:
    """Every outward-pointing capability names its target, and naming it is mandatory."""
    variant = _capability_variant(_emitted(), kind)

    assert variant["required"] == ["kind", reference]


def test_la_capacidad_native_emitida_cierra_el_vocabulario_de_tools_cuando_se_construye() -> None:
    """``native`` names one provider tool from a closed list, and nothing else (030/AC-1)."""
    variant = _capability_variant(_emitted(), "native")

    assert variant["required"] == ["kind", "tool"]
    assert variant["additionalProperties"] is False
    assert variant["properties"]["tool"]["enum"] == ["web_search", "web_fetch", "code_execution"]


@pytest.mark.parametrize("kind", ["mcp", "a2a", "skills"])
def test_la_capacidad_emitida_declara_el_filtro_plano_cuando_se_construye(kind: str) -> None:
    """The three filtered kinds share one flat include/exclude vocabulary."""
    properties = _capability_variant(_emitted(), kind)["properties"]

    assert {"include", "exclude"} <= set(properties)


@pytest.mark.parametrize("retired", ["tool_filter", "refs", "url", "headers_ref", "timeout_ms"])
def test_el_esquema_emitido_no_declara_el_vocabulario_retirado_cuando_se_construye(
    retired: str,
) -> None:
    """Addresses and credentials are deployment facts; they left the artifact."""
    assert retired not in _property_names(_emitted())


def test_agent_spec_json_schema_falla_cuando_la_version_no_esta_publicada() -> None:
    """An unpublished spec version is a programming error, not an empty document."""
    with pytest.raises(ValueError, match="spec version 2"):
        agent_spec_json_schema(2)


def test_el_esquema_publicado_viaja_dentro_del_paquete_cuando_se_localiza() -> None:
    """The schema file must live under the installed package, not under 'specs/'.

    A consumer validates against a file it can extract from the wheel or the sdist;
    anchoring the contract in git-ignored working state would publish nothing (FR-009).
    """
    path = agent_spec_schema_path(1)

    assert path.is_file()
    assert path.parent.name == "schemas"
    assert path.parent.parent.name == "declarative"


def test_localizar_el_esquema_falla_cuando_la_version_no_esta_publicada() -> None:
    """An unpublished spec version is a programming error, not a missing file."""
    with pytest.raises(ValueError, match="spec version 2"):
        agent_spec_schema_path(2)
