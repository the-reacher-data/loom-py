"""The shared ``factory(context, **params)`` signature check (011/T303).

Two declarations bind that call — a ``kind: python`` capability and
``dynamic_instructions`` — and both must refuse the same two faults. The check
lives in one place, and what it returns is a *named* fault rather than a
message or a ready-made issue, so each caller maps it to the codes of its own
field.

These tests pin that contract directly, because the two callers can only
observe it through their own codes: a string return would have made "the
context does not bind" and "the params do not bind" indistinguishable at the
call site, which is precisely the distinction the two codes carry.
"""

from __future__ import annotations

from typing import Any

from loom.ai.compiler.phases._factories import FactoryFault, reject_factory_signature


def _with_settings(context: object, *, locale: str = "en", limit: int = 3) -> str:
    """A well-shaped factory: a context positional plus named settings."""
    del context
    return f"{locale}-{limit}"


def _without_context(*, locale: str = "en") -> str:
    """A factory with no slot for the build-time context: the wrong shape."""
    return locale


def _variadic(context: object, **params: Any) -> object:
    """A factory naming no setting of its own, which must accept any of them."""
    del context
    return params


def test_acepta_cuando_los_params_declarados_encajan_en_la_firma() -> None:
    """The whole point of the check is that a correct declaration passes it."""
    assert reject_factory_signature(_with_settings, {"locale": "es", "limit": 1}) is None


def test_acepta_cuando_no_se_declara_ningun_param() -> None:
    """Every setting has a default, so an artifact may declare none."""
    assert reject_factory_signature(_with_settings, {}) is None


def test_acepta_cuando_la_factoria_absorbe_cualquier_param() -> None:
    """A ``**params`` factory names nothing, so nothing can be rejected."""
    assert reject_factory_signature(_variadic, {"anything": 1}) is None


def test_acepta_cuando_la_firma_no_se_puede_inspeccionar() -> None:
    """Some callables have no inspectable signature; Python's own call reports the fault.

    ``min`` is one of them, and refusing every such callable here would refuse
    a legitimate factory for a reason it cannot fix.
    """
    assert reject_factory_signature(min, {"unknown": 1}) is None


def test_devuelve_not_callable_cuando_no_hay_hueco_para_el_contexto() -> None:
    """The context binds alone first, so the wrong shape is not a params fault."""
    rejection = reject_factory_signature(_without_context, {"locale": "es"})

    assert rejection is not None
    assert rejection.fault is FactoryFault.NOT_CALLABLE


def test_devuelve_params_rejected_cuando_el_param_no_existe_en_la_firma() -> None:
    """The detail carries what Python said, so a caller can name the setting."""
    rejection = reject_factory_signature(_with_settings, {"unknown_setting": 1})

    assert rejection is not None
    assert rejection.fault is FactoryFault.PARAMS_REJECTED
    assert "unknown_setting" in rejection.detail
