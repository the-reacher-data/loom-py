"""Common contracts for component discovery engines."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from loom.core.model import BaseModel
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface


@dataclass(frozen=True)
class DiscoveryResult:
    """Discovered application components used by bootstrap."""

    models: tuple[type[BaseModel], ...]
    use_cases: tuple[type[UseCase[object, object]], ...]
    interfaces: tuple[type[RestInterface[object]], ...]


class DiscoveryEngine(Protocol):
    """Discovery strategy contract."""

    def discover(self) -> DiscoveryResult:
        """Return discovered models, use cases, and REST interfaces."""
        ...
