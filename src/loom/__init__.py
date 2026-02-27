"""Public package root for loom-kernel.

Exposes top-level namespaces used by docs/autosummary and end users.
"""

from loom import prometheus, rest, testing

__all__ = [
    "prometheus",
    "rest",
    "testing",
]
