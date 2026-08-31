"""Secret containment for ``loom.ai.inference.InferenceTarget`` (T025).

``InferenceTarget`` carries a ``credentials_ref`` and vendor ``options``; both
may point at (or parametrise) secrets. The struct must therefore redact them in
``__repr__``/``__str__``, and any msgspec encoding of the struct must either be
refused outright or omit those values in clear.

These tests pin FR-018 / data-model invariant 4: no secret-bearing value may
leak through a traceback repr or through an encoded plan.
"""

from __future__ import annotations

import msgspec
import pytest
from loom.ai.inference import InferenceTarget

_CREDENTIALS_REF = "ref/to/secret-name"
_OPTION_SENTINEL = "SENTINEL_OPT_VALUE"


@pytest.fixture()
def target() -> InferenceTarget:
    """Build a target carrying both redactable fields."""
    return InferenceTarget(
        provider="openai",
        model="gpt-test",
        credentials_ref=_CREDENTIALS_REF,
        options={"api_key_param": _OPTION_SENTINEL},
    )


def test_repr_no_contiene_credentials_ref_cuando_esta_definido(
    target: InferenceTarget,
) -> None:
    """``repr`` must redact the credentials reference."""
    assert _CREDENTIALS_REF not in repr(target)


def test_repr_no_contiene_valores_de_options_cuando_estan_definidos(
    target: InferenceTarget,
) -> None:
    """``repr`` must redact vendor option values."""
    assert _OPTION_SENTINEL not in repr(target)


def test_str_no_contiene_credentials_ref_cuando_esta_definido(
    target: InferenceTarget,
) -> None:
    """``str`` must redact the credentials reference."""
    assert _CREDENTIALS_REF not in str(target)


def test_str_no_contiene_valores_de_options_cuando_estan_definidos(
    target: InferenceTarget,
) -> None:
    """``str`` must redact vendor option values."""
    assert _OPTION_SENTINEL not in str(target)


def test_encoding_json_no_expone_secretos_cuando_se_codifica_el_struct(
    target: InferenceTarget,
) -> None:
    """Encoding the struct either fails or omits the secret-bearing values.

    Both behaviours satisfy invariant 4: what must never happen is JSON bytes
    carrying the reference or the option value in clear.
    """
    try:
        encoded = msgspec.json.encode(target)
    except TypeError:
        return
    text = encoded.decode("utf-8")
    assert _CREDENTIALS_REF not in text and _OPTION_SENTINEL not in text
