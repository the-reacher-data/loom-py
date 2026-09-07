"""``_bounded``: the run primitives the hook and the loader share (007 T1)."""

from __future__ import annotations

from loom.ai.runtime import _bounded, _hooks


def test_hook_run_es_run_context_cuando_se_importa_el_nombre_antiguo() -> None:
    """The old name stays importable one release and is the very same class."""
    assert _hooks.HookRun is _bounded.RunContext
