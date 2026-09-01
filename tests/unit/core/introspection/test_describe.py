"""Data-driven self-description core (US7).

``loom.core.introspection`` is the neutral half of the feature: it knows how
to assemble one JSON-encodable document out of an application identity plus a
set of *references* to pillar contributors.  It resolves those references by
``importlib`` at call time, which is exactly what keeps ``loom.core`` free of
any pillar import (principle I, FR-050, SC-013) — the last test of this module
guards that containment in a clean interpreter.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import types
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from loom.core.introspection import (
    AppIntrospection,
    ContributorRef,
    IntrospectionError,
    describe_app,
)

_SRC = Path(__file__).resolve().parents[4] / "src"

_MODULE_NAME = "tests.unit.core.introspection._describe_contributors"

_APP_NAME = "introspection-demo"
_APP_VERSION = "3.2.1"


def _echo(subject: Any, /) -> dict[str, Any]:
    """Contributor projecting its subject into a builtin mapping."""
    return {"seen": subject}


@pytest.fixture
def contributor_module() -> Iterator[str]:
    """Register an importable module exposing one callable and one plain value."""
    module = types.ModuleType(_MODULE_NAME)
    module.echo = _echo
    module.not_callable = "plain value"
    sys.modules[_MODULE_NAME] = module
    try:
        yield _MODULE_NAME
    finally:
        sys.modules.pop(_MODULE_NAME, None)


def _introspection(*contributors: ContributorRef) -> AppIntrospection:
    """Build the application introspection under test."""
    return AppIntrospection(
        name=_APP_NAME,
        version=_APP_VERSION,
        contributors=contributors,
    )


class TestDescripcionSinContribuidores:
    """An application that contributes nothing still describes itself."""

    def test_publica_solo_la_identidad_cuando_no_hay_contribuidores(self) -> None:
        """The identity section is the whole document when no pillar contributes."""
        assert describe_app(_introspection()) == {
            "app": {"name": _APP_NAME, "version": _APP_VERSION}
        }


class TestResolucionDeContribuidores:
    """A contributor is a string until ``describe_app`` is actually called."""

    def test_coloca_la_contribucion_bajo_su_seccion_cuando_el_ref_resuelve(
        self, contributor_module: str
    ) -> None:
        """The resolved callable's return value lands under its declared section."""
        ref = ContributorRef(
            section="agents",
            contributor=f"{contributor_module}:echo",
            subject=("alpha", "beta"),
        )

        assert describe_app(_introspection(ref))["agents"] == {"seen": ("alpha", "beta")}

    def test_conserva_la_identidad_cuando_hay_una_contribucion(
        self, contributor_module: str
    ) -> None:
        """A contribution never displaces the reserved ``app`` section."""
        ref = ContributorRef(
            section="agents",
            contributor=f"{contributor_module}:echo",
            subject=(),
        )

        assert describe_app(_introspection(ref))["app"] == {
            "name": _APP_NAME,
            "version": _APP_VERSION,
        }


class TestContribuidoresInvalidos:
    """Every unusable reference is reported as an ``IntrospectionError``."""

    def test_falla_cuando_el_ref_no_tiene_la_forma_modulo_dos_puntos_callable(self) -> None:
        """A reference without ``:`` names no callable at all."""
        ref = ContributorRef(section="agents", contributor="loom.core.introspection", subject=())

        with pytest.raises(IntrospectionError):
            describe_app(_introspection(ref))

    def test_falla_cuando_el_modulo_no_existe(self) -> None:
        """An unimportable module is a wiring error, reported as such."""
        ref = ContributorRef(
            section="agents",
            contributor="loom.core.introspection_absent_module:describe",
            subject=(),
        )

        with pytest.raises(IntrospectionError):
            describe_app(_introspection(ref))

    def test_falla_cuando_el_atributo_no_existe(self, contributor_module: str) -> None:
        """A module that does not expose the named attribute is a wiring error."""
        ref = ContributorRef(
            section="agents",
            contributor=f"{contributor_module}:absent",
            subject=(),
        )

        with pytest.raises(IntrospectionError):
            describe_app(_introspection(ref))

    def test_falla_cuando_el_atributo_no_es_invocable(self, contributor_module: str) -> None:
        """A contributor must be callable; a plain value cannot project anything."""
        ref = ContributorRef(
            section="agents",
            contributor=f"{contributor_module}:not_callable",
            subject=(),
        )

        with pytest.raises(IntrospectionError):
            describe_app(_introspection(ref))

    def test_falla_cuando_dos_contribuidores_declaran_la_misma_seccion(
        self, contributor_module: str
    ) -> None:
        """Silently overwriting one pillar's contribution with another is not an option."""
        ref = ContributorRef(
            section="agents",
            contributor=f"{contributor_module}:echo",
            subject=(),
        )

        with pytest.raises(IntrospectionError):
            describe_app(_introspection(ref, ref))

    def test_falla_cuando_la_seccion_es_app(self, contributor_module: str) -> None:
        """``app`` is reserved for the application identity."""
        ref = ContributorRef(
            section="app",
            contributor=f"{contributor_module}:echo",
            subject=(),
        )

        with pytest.raises(IntrospectionError):
            describe_app(_introspection(ref))


_FORBIDDEN_ROOTS: tuple[str, ...] = (
    "loom.ai",
    "loom.rest",
    "loom.streaming",
    "loom.etl",
)

_FORBIDDEN_THIRD_PARTY: tuple[str, ...] = ("fastapi", "pydantic")

_CONTAINMENT_SCRIPT = """
import json
import sys

baseline = frozenset(sys.modules)

import loom.core.introspection  # noqa: F401

delta = frozenset(sys.modules) - baseline
roots = {name.split(".")[0] for name in delta}
print(
    json.dumps(
        {
            "modules": sorted(delta),
            "third_party": sorted(roots - sys.stdlib_module_names - {"loom"}),
        }
    )
)
"""


def _run_in_clean_interpreter(script: str) -> subprocess.CompletedProcess[str]:
    """Run ``script`` in a fresh interpreter that can see the repository ``src``."""
    env = {**os.environ, "PYTHONPATH": str(_SRC)}
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


@pytest.fixture(scope="module")
def imported_modules() -> dict[str, list[str]]:
    """Modules a clean interpreter gains from importing the introspection package."""
    result = _run_in_clean_interpreter(_CONTAINMENT_SCRIPT)
    if result.returncode != 0:
        raise AssertionError(result.stderr)
    payload: dict[str, list[str]] = json.loads(result.stdout.strip().splitlines()[-1])
    return payload


class TestContencionDeImports:
    """Importing the core package must not drag a single pillar in (principle I)."""

    def test_no_importa_ningun_pilar_cuando_se_importa_la_introspeccion(
        self, imported_modules: dict[str, list[str]]
    ) -> None:
        """No ``loom.ai`` / ``loom.rest`` / ``loom.streaming`` / ``loom.etl`` module appears."""
        leaked = [
            name
            for name in imported_modules["modules"]
            if any(name == root or name.startswith(f"{root}.") for root in _FORBIDDEN_ROOTS)
        ]

        assert leaked == []

    def test_no_importa_fastapi_ni_pydantic_cuando_se_importa_la_introspeccion(
        self, imported_modules: dict[str, list[str]]
    ) -> None:
        """The REST stack is not a dependency of describing an application."""
        leaked = sorted(set(imported_modules["third_party"]) & set(_FORBIDDEN_THIRD_PARTY))

        assert leaked == []
