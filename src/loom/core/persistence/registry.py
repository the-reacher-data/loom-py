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
        ConfigError: When no backend is registered under ``name``; the message
            lists the registered names.
        DuplicateEntryPointError: When several distributions register ``name``.
    """
    try:
        target = load_entry_point(BACKEND_ENTRY_POINT_GROUP, name, on_duplicate="error")
    except EntryPointNotFoundError as exc:
        registered = ", ".join(exc.available) if exc.available else "none"
        raise ConfigError(
            f"Unknown persistence backend {name!r}. Registered backends: {registered}."
        ) from exc
    backend_cls = cast(type[PersistenceBackend], target)
    return backend_cls()


__all__ = ["BACKEND_ENTRY_POINT_GROUP", "resolve_backend"]
