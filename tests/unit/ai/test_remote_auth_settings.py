"""Settings reach a strategy as the types its signature declares.

Configuration carries every setting as a string, because each one passes the
inline-credential refusal, which admits no spaces.  That is loom's own
constraint, so loom converts back rather than handing a strategy a string where
its signature asked for an ``int``.
"""

from __future__ import annotations

import functools
from typing import Any

import msgspec
import pytest

from loom.ai.remote_auth import _coerce_settings


class _Opaque:
    """A type msgspec cannot build from a string."""


def _funcion(*, url: str, timeout: int, ratio: float, verify: bool) -> None: ...


class _Clase:
    def __init__(self, *, timeout: int, cfg: _Opaque | None = None, libre: Any = None) -> None:
        self.timeout = timeout
        self.cfg = cfg
        self.libre = libre


class TestTiposDeclarados:
    def test_convierte_cada_primitivo_al_tipo_de_su_parametro(self) -> None:
        got = _coerce_settings(
            _funcion,
            {"url": "https://x", "timeout": "30", "ratio": "1.5", "verify": "false"},
        )
        assert got["url"] == "https://x"
        assert got["timeout"] == 30
        assert got["ratio"] == 1.5
        # `is False`, not `not got[...]`: the string "false" is truthy, so a
        # naive implementation returns True here and `assert not` would also
        # pass on "" or 0. This is the assertion that catches the inversion.
        assert got["verify"] is False

    def test_lee_la_firma_de_una_funcion_y_no_su_dunder_init(self) -> None:
        """Two of the three strategies loom registers are plain functions.

        ``inspect.signature(fn.__init__)`` yields ``object``'s ``(*args,
        **kwargs)`` - no parameters, so nothing converts, in silence. This is
        the defect a plausible implementation actually ships.
        """
        got = _coerce_settings(_funcion, {"timeout": "30"})
        assert got["timeout"] == 30, "a function strategy converted nothing"

    def test_convierte_igual_cuando_la_estrategia_es_una_clase(self) -> None:
        assert _coerce_settings(_Clase, {"timeout": "30"})["timeout"] == 30


class TestLoQueNoSeToca:
    def test_deja_intacto_un_tipo_propio(self) -> None:
        """The whole non-break guarantee: a strategy declaring its own type
        receives exactly what it receives today."""
        raw = "algo"
        got = _coerce_settings(_Clase, {"timeout": "1", "cfg": raw})
        assert got["cfg"] is raw

    def test_deja_intacto_un_parametro_sin_anotacion_util(self) -> None:
        raw = "algo"
        assert _coerce_settings(_Clase, {"timeout": "1", "libre": raw})["libre"] is raw

    def test_deja_intacto_un_ajuste_que_la_estrategia_no_declara(self) -> None:
        raw = "algo"
        assert _coerce_settings(_funcion, {"desconocido": raw})["desconocido"] is raw


class TestBordes:
    def test_rechaza_un_valor_que_no_es_su_primitivo(self) -> None:
        with pytest.raises(msgspec.ValidationError, match="int"):
            _coerce_settings(_funcion, {"timeout": "pronto"})

    def test_pasa_todo_tal_cual_cuando_no_puede_inspeccionar(self) -> None:
        """Introspection is best effort: it must never break a working strategy."""
        got = _coerce_settings(functools.partial(dict), {"timeout": "30"})
        assert got == {"timeout": "30"}
