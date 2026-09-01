"""Offline ``module:symbol`` reference resolution shared by the phases."""

from __future__ import annotations

from importlib import import_module


def import_symbol(ref: str) -> object:
    """Import the symbol a ``module:symbol`` reference points at.

    Args:
        ref: Reference in ``module:symbol`` form, as constrained by the
            artifact pattern.

    Returns:
        The imported symbol.

    Raises:
        ImportError: If the module does not import.
        AttributeError: If the module lacks the symbol.
        ValueError: If the reference is not in ``module:symbol`` form.
    """
    module_name, _, symbol_name = ref.partition(":")
    if not module_name or not symbol_name:
        raise ValueError(f"reference {ref!r} is not in module:symbol form")
    module = import_module(module_name)
    return getattr(module, symbol_name)
