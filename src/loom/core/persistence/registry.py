"""Resolution of persistence backends from the ``loom.persistence.backends`` group."""

from __future__ import annotations

from typing import cast

from loom.core.config import ConfigError
from loom.core.persistence.abc import PersistenceBackend
from loom.core.plugins.entrypoints import EntryPointNotFoundError, load_entry_point

BACKEND_ENTRY_POINT_GROUP = "loom.persistence.backends"
"""Entry point group persistence backends register under."""


def resolve_backend(name: str) -> PersistenceBackend:
    """Resolve the persistence backend registered under ``name``.

    Args:
        name: ``persistence.backend`` value naming the backend.

    Returns:
        An instance of the registered backend class.

    Raises:
        ConfigError: When no backend is registered under ``name`` (the message
            lists the registered names), or when the backend module cannot be
            imported because its optional extra is not installed.
        DuplicateEntryPointError: When several distributions register ``name``.
    """
    try:
        target = load_entry_point(BACKEND_ENTRY_POINT_GROUP, name, on_duplicate="error")
    except EntryPointNotFoundError as exc:
        raise ConfigError(_unknown_backend_message(name, exc.available)) from exc
    except ImportError as exc:
        raise ConfigError(
            f"Persistence backend {name!r} could not be imported ({exc}). Install the "
            f"distribution providing it (built-in backends: `pip install 'loom-kernel[{name}]'`)."
        ) from exc
    backend_cls = cast(type[PersistenceBackend], target)
    return backend_cls()


def _unknown_backend_message(name: str, available: tuple[str, ...]) -> str:
    if not available:
        return f"Unknown persistence backend {name!r}. No persistence backends are registered."
    registered = ", ".join(available)
    return f"Unknown persistence backend {name!r}. Registered backends: {registered}."


__all__ = ["BACKEND_ENTRY_POINT_GROUP", "resolve_backend"]
