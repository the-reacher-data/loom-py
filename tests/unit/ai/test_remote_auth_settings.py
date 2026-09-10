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


def _function(*, url: str, timeout: int, ratio: float, verify: bool) -> None: ...


class _Class:
    def __init__(self, *, timeout: int, cfg: _Opaque | None = None, libre: Any = None) -> None:
        self.timeout = timeout
        self.cfg = cfg
        self.libre = libre


class TestDeclaredTypes:
    def test_converts_each_primitive_to_its_parameter_type(self) -> None:
        got = _coerce_settings(
            _function,
            {"url": "https://x", "timeout": "30", "ratio": "1.5", "verify": "false"},
        )
        assert got["url"] == "https://x"
        assert got["timeout"] == 30
        assert got["ratio"] == 1.5
        # `is False`, not `not got[...]`: the string "false" is truthy, so a
        # naive implementation returns True here and `assert not` would also
        # pass on "" or 0. This is the assertion that catches the inversion.
        assert got["verify"] is False

    def test_reads_a_functions_signature_not_its_dunder_init(self) -> None:
        """Two of the three strategies loom registers are plain functions.

        ``inspect.signature(fn.__init__)`` yields ``object``'s ``(*args,
        **kwargs)`` - no parameters, so nothing converts, in silence. This is
        the defect a plausible implementation actually ships.
        """
        got = _coerce_settings(_function, {"timeout": "30"})
        assert got["timeout"] == 30, "a function strategy converted nothing"

    def test_converts_the_same_way_when_the_strategy_is_a_class(self) -> None:
        assert _coerce_settings(_Class, {"timeout": "30"})["timeout"] == 30


class TestWhatIsLeftUntouched:
    def test_leaves_a_custom_type_untouched(self) -> None:
        """The whole non-break guarantee: a strategy declaring its own type
        receives exactly what it receives today."""
        raw = "algo"
        got = _coerce_settings(_Class, {"timeout": "1", "cfg": raw})
        assert got["cfg"] is raw

    def test_leaves_a_parameter_without_a_useful_annotation_untouched(self) -> None:
        raw = "algo"
        assert _coerce_settings(_Class, {"timeout": "1", "libre": raw})["libre"] is raw

    def test_leaves_a_setting_the_strategy_does_not_declare_untouched(self) -> None:
        raw = "algo"
        assert _coerce_settings(_function, {"desconocido": raw})["desconocido"] is raw


class TestEdgeCases:
    def test_rejects_a_value_that_does_not_match_its_primitive(self) -> None:
        with pytest.raises(msgspec.ValidationError, match="int"):
            _coerce_settings(_function, {"timeout": "pronto"})

    def test_passes_everything_through_when_it_cannot_inspect(self) -> None:
        """Introspection is best effort: it must never break a working strategy."""
        got = _coerce_settings(functools.partial(dict), {"timeout": "30"})
        assert got == {"timeout": "30"}
