"""Backend provider loading for ETL runner wiring."""

from __future__ import annotations

from importlib.metadata import EntryPoint, entry_points
from typing import Any, Protocol, cast, runtime_checkable

from loom.etl.runtime.contracts import SourceReader, TargetWriter
from loom.etl.storage._config import StorageConfig

_BACKEND_EP_GROUP = "loom.etl.backends"


@runtime_checkable
class BackendProvider(Protocol):
    """Factory protocol for backend reader/writer pairs."""

    def create_backends(
        self,
        config: StorageConfig,
        spark: Any = None,
    ) -> tuple[SourceReader, TargetWriter]:
        """Create backend reader/writer pair for one engine."""
        ...


def load_backend_provider(engine: str) -> BackendProvider:
    """Load backend provider implementation from package entry points."""
    ep = _find_backend_entrypoint(engine)
    provider_cls = cast(type[BackendProvider], ep.load())
    provider = provider_cls()
    return provider


def _find_backend_entrypoint(engine: str) -> EntryPoint:
    for ep in _iter_backend_entrypoints():
        if ep.name == engine:
            return ep
    raise ValueError(
        f"Unsupported storage.engine={engine!r}. "
        f"No backend provider registered in entry point group '{_BACKEND_EP_GROUP}'."
    )


def _iter_backend_entrypoints() -> tuple[EntryPoint, ...]:
    eps = entry_points()
    if hasattr(eps, "select"):
        return tuple(eps.select(group=_BACKEND_EP_GROUP))
    legacy = eps.get(_BACKEND_EP_GROUP, ())
    return tuple(legacy)


__all__ = ["BackendProvider", "load_backend_provider"]
