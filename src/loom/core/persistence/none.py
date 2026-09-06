"""The ``none`` persistence backend: no persistence at all."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import ClassVar

from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer
from loom.core.model import BaseModel
from loom.core.persistence.abc import PersistenceWiring


class NoneBackend:
    """Backend for ``persistence.backend: none``.

    No unit-of-work factory, no repositories, no startup resource, no default
    repository type; a ``database`` section, if present, is ignored.
    """

    name: ClassVar[str] = "none"

    def build(self, ctx: ConfigContext, models: Sequence[type[BaseModel]]) -> PersistenceWiring:
        """Build a wiring in which every hook is a no-op.

        Args:
            ctx: Ignored; this backend reads no configuration.
            models: Ignored; this backend serves no repositories.

        Returns:
            The no-persistence wiring.
        """
        return PersistenceWiring(
            uow_factory=None,
            repo_registration_module=_register_no_repositories,
            lifespan_init=_noop_lifespan,
            default_repository_type=None,
            prepare_models=_prepare_no_models,
        )


def _register_no_repositories(container: LoomContainer) -> None:
    """No-op DI module: nothing to register."""


def _prepare_no_models(models: Sequence[type[BaseModel]]) -> None:
    """No-op preparation: nothing to compile or validate."""


@asynccontextmanager
async def _noop_lifespan() -> AsyncIterator[None]:
    """No-op lifespan: no shared startup resource."""
    yield


__all__ = ["NoneBackend"]
