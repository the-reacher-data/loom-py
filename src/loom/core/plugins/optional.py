"""The one way loom loads code that needs an optional extra.

A dependency loom does not require lives behind an extra, and the code that
uses it lives in an **island**: a module that imports the vendor SDK at the
top, like any other import, and nothing else in loom imports that SDK. What
selects the island — a registry keyed by provider, engine or backend name —
loads it here, by module path, once it knows the name; the island's own
``ImportError`` becomes a :class:`MissingExtraError` naming the extra to
install. So the pattern needs no ``try``/``except`` in any island, no
sentinel ``None`` next to a guarded import, no ``import`` inside a function
and no attribute lookup on a vendor module.

This module is a stdlib-only leaf, like its sibling ``entrypoints``: it must
never import from ``loom`` or from any third-party package.
"""

from __future__ import annotations

from importlib import import_module
from types import ModuleType

__all__ = ["MissingExtraError", "import_optional"]


class MissingExtraError(ImportError):
    """An island could not be imported because its extra is not installed.

    Attributes:
        module: The island that failed to import.
        extra: The loom extra whose installation brings what it needs.
        missing: The module the interpreter could not find, when it said so —
            the vendor SDK in the ordinary case; a loom module of the island's
            own when the failure is a bug in it rather than a missing extra.
    """

    def __init__(self, module: str, extra: str, missing: str | None) -> None:
        self.module = module
        self.extra = extra
        self.missing = missing
        detail = f": no module named {missing!r}" if missing else ""
        super().__init__(f"{module} needs the {extra!r} extra{detail}", name=missing)


def import_optional(module: str, *, extra: str) -> ModuleType:
    """Import an island of loom that needs an optional extra.

    Args:
        module: Importable path of the island.
        extra: Loom extra whose installation brings what the island imports.

    Returns:
        The imported island.

    Raises:
        MissingExtraError: When the island cannot be imported. The cause is
            chained, so a failure that is not a missing extra stays readable.

    Example::

        island = import_optional("loom.ai.engines.pydantic_ai.providers.bedrock",
                                 extra="ai-bedrock")
    """
    try:
        return import_module(module)
    except ImportError as exc:
        raise MissingExtraError(module, extra, exc.name) from exc
