"""Regression corpus for authored agent artifacts of spec version 1.

Every fixture under ``fixtures/corpus_v1`` is an append-only piece of evidence that a
v1 artifact keeps decoding forever. These tests pin two independent guarantees for
each entry: it decodes through the public loader into a v1 struct, and it validates
against the schema Loom publishes for editors and generators
(``agent_spec_json_schema(1)``).

The committed ``contracts/agent-spec-v1.schema.json`` file is deliberately *not* used
here: it is behind the structs in two documented places, and validating the corpus
against it would fail for artifacts that are perfectly legal v1. That gap is the
subject of ``test_schema_emission.py``.

A coverage test closes the format: the corpus must exercise every capability kind and
every output kind, so a new kind cannot enter the format without a fixture.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from jsonschema import Draft202012Validator

from loom.ai.declarative import AgentSpecV1, DecodedSpec, agent_spec_json_schema, load_specs

_CORPUS_DIR: Path = Path(__file__).parent / "fixtures" / "corpus_v1"
_CORPUS_PATTERN: str = "*.agent.yaml"

_CAPABILITY_KINDS: frozenset[str] = frozenset({"usecase", "sql", "mcp", "skills", "python", "a2a"})
_OUTPUT_KINDS: frozenset[str] = frozenset({"json_schema", "type_ref"})


def _corpus_paths() -> tuple[Path, ...]:
    """List the corpus artifacts, sorted so parametrisation is deterministic."""
    return tuple(sorted(_CORPUS_DIR.glob(_CORPUS_PATTERN)))


def _corpus_ids() -> tuple[str, ...]:
    """Name each parametrised case after its artifact file."""
    return tuple(path.name for path in _corpus_paths())


def _load_corpus() -> tuple[DecodedSpec, ...]:
    """Decode the whole corpus through the public loader."""
    return load_specs([_CORPUS_PATTERN], _CORPUS_DIR)


def _read_artifact(path: Path) -> Any:
    """Read one artifact as the plain document a JSON Schema validator consumes."""
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _declared_capability_kinds() -> set[str]:
    """Collect every capability kind declared anywhere in the corpus."""
    kinds: set[str] = set()
    for path in _corpus_paths():
        document = _read_artifact(path)
        for capability in document.get("capabilities", ()):
            kinds.add(capability["kind"])
    return kinds


def _declared_output_kinds() -> set[str]:
    """Collect every output kind declared anywhere in the corpus."""
    return {_read_artifact(path)["output"]["kind"] for path in _corpus_paths()}


def test_el_corpus_no_esta_vacio_cuando_se_resuelve_el_patron() -> None:
    """A silently empty corpus would make every per-file test vacuously green."""
    assert _corpus_paths() != ()


@pytest.mark.parametrize("path", _corpus_paths(), ids=_corpus_ids())
def test_el_artefacto_decodifica_cuando_pertenece_al_corpus(path: Path) -> None:
    """Each corpus entry decodes through ``load_specs`` into the v1 struct."""
    decoded = load_specs([path.name], _CORPUS_DIR)

    assert [type(entry.spec) for entry in decoded] == [AgentSpecV1]


@pytest.mark.parametrize("path", _corpus_paths(), ids=_corpus_ids())
def test_el_artefacto_valida_contra_el_esquema_emitido_cuando_pertenece_al_corpus(
    path: Path,
) -> None:
    """Each corpus entry validates against the schema Loom publishes for v1."""
    validator = Draft202012Validator(agent_spec_json_schema(1))

    assert list(validator.iter_errors(_read_artifact(path))) == []


def test_la_decodificacion_no_reporta_incidencias_cuando_el_corpus_es_v1_vigente() -> None:
    """The corpus targets the current spec version, so nothing is deprecated."""
    assert [entry.issues for entry in _load_corpus()] == [()] * len(_corpus_paths())


def test_el_corpus_cubre_todos_los_kinds_de_capacidad_cuando_se_recorre_entero() -> None:
    """Every capability kind of the format has at least one fixture."""
    assert _declared_capability_kinds() == set(_CAPABILITY_KINDS)


def test_el_corpus_cubre_todos_los_kinds_de_salida_cuando_se_recorre_entero() -> None:
    """Every output kind of the format has at least one fixture."""
    assert _declared_output_kinds() == set(_OUTPUT_KINDS)
