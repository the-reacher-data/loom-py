"""Discovery engine that loads components from a manifest module."""

from __future__ import annotations

import importlib
from typing import Any, cast

from loom.core.discovery.base import DiscoveryResult
from loom.core.model import BaseModel
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface


class ManifestDiscoveryEngine:
    """Discover components from explicit lists in a manifest module."""

    def __init__(self, manifest_module: str) -> None:
        self._manifest_module = manifest_module

    def discover(self) -> DiscoveryResult:
        if not self._manifest_module:
            raise ValueError("manifest discovery requires a module path.")

        module = importlib.import_module(self._manifest_module)
        raw_models = cast(list[Any], getattr(module, "MODELS", []))
        raw_use_cases = cast(list[Any], getattr(module, "USE_CASES", []))
        raw_interfaces = cast(list[Any], getattr(module, "INTERFACES", []))

        models = [cast(type[BaseModel], item) for item in raw_models]
        use_cases = [cast(type[UseCase[object, object]], item) for item in raw_use_cases]
        interfaces = [cast(type[RestInterface[object]], item) for item in raw_interfaces]

        if not interfaces and not use_cases and not models:
            raise ValueError(
                f"Manifest module {self._manifest_module!r} exposes no components. "
                "Expected MODELS/USE_CASES/INTERFACES."
            )

        return DiscoveryResult(
            models=tuple(models),
            use_cases=tuple(use_cases),
            interfaces=tuple(interfaces),
        )
