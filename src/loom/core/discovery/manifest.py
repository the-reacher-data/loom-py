"""Discovery engine that loads components from a manifest module."""

from __future__ import annotations

import importlib
from typing import Any, cast

from loom.core.contracts.manifest import AppManifestAttr
from loom.core.discovery._utils import _append_unique, collect_use_cases_from_interfaces
from loom.core.discovery.base import DiscoveryResult
from loom.core.model import BaseModel
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface


class ManifestDiscoveryEngine:
    """Discover components from explicit lists in a manifest module."""

    def __init__(self, manifest_module: str) -> None:
        self._manifest_module = manifest_module

    def discover(self) -> DiscoveryResult:
        """Load components from manifest lists and supplement auto-CRUD use cases.

        The manifest module is expected to expose ``MODELS``, ``USE_CASES``,
        and ``INTERFACES`` lists.  When ``USE_CASES`` is empty but
        ``INTERFACES`` contain ``auto=True`` interfaces, the generated use
        cases are discovered automatically from the interface routes.

        Returns:
            :class:`~loom.core.discovery.base.DiscoveryResult` with all
            discovered models, use cases, and interfaces.

        Raises:
            ValueError: When no manifest module path is provided, or when the
                manifest exposes no components at all.
        """
        if not self._manifest_module:
            raise ValueError("manifest discovery requires a module path.")

        module = importlib.import_module(self._manifest_module)
        raw_models = cast(list[Any], getattr(module, AppManifestAttr.MODELS, []))
        raw_use_cases = cast(list[Any], getattr(module, AppManifestAttr.USE_CASES, []))
        raw_interfaces = cast(list[Any], getattr(module, AppManifestAttr.INTERFACES, []))

        models = [cast(type[BaseModel], item) for item in raw_models]
        use_cases = [cast(type[UseCase[object, object]], item) for item in raw_use_cases]
        interfaces = [cast(type[RestInterface[object]], item) for item in raw_interfaces]

        seen_ucs: set[type[UseCase[object, object]]] = set(use_cases)
        for uc in collect_use_cases_from_interfaces(interfaces):
            _append_unique(use_cases, seen_ucs, uc)

        if not interfaces and not use_cases and not models:
            expected = (
                f"{AppManifestAttr.MODELS}/{AppManifestAttr.USE_CASES}/{AppManifestAttr.INTERFACES}"
            )
            raise ValueError(
                f"Manifest module {self._manifest_module!r} exposes no components. "
                f"Expected {expected}."
            )

        return DiscoveryResult(
            models=tuple(models),
            use_cases=tuple(use_cases),
            interfaces=tuple(interfaces),
        )
