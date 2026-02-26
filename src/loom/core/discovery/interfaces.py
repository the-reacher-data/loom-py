"""Discovery engine that starts from REST interface modules."""

from __future__ import annotations

import warnings
from typing import cast

from loom.core.discovery._utils import (
    collect_from_modules,
    import_modules,
    infer_model_from_use_case,
)
from loom.core.discovery.base import DiscoveryResult
from loom.core.model import BaseModel
from loom.core.use_case.use_case import UseCase


class InterfacesDiscoveryEngine:
    """Discover components from modules that define ``RestInterface`` classes."""

    def __init__(
        self,
        interface_modules: list[str],
        *,
        warn_recommended: bool = True,
    ) -> None:
        self._interface_modules = interface_modules
        self._warn_recommended = warn_recommended

    def discover(self) -> DiscoveryResult:
        if not self._interface_modules:
            raise ValueError("interfaces discovery requires at least one module path.")

        modules = import_modules(self._interface_modules)
        _, _, interfaces = collect_from_modules(modules)
        if not interfaces:
            raise ValueError("No RestInterface subclasses discovered in interface modules.")

        use_cases: list[type[UseCase[object, object]]] = []
        seen_use_cases: set[type[UseCase[object, object]]] = set()
        for interface in interfaces:
            for route in interface.routes:
                uc = cast(type[UseCase[object, object]], route.use_case)
                if uc not in seen_use_cases:
                    use_cases.append(uc)
                    seen_use_cases.add(uc)

        models: list[type[BaseModel]] = []
        seen_models: set[type[BaseModel]] = set()
        for use_case in use_cases:
            model = infer_model_from_use_case(use_case)
            if model is not None and model not in seen_models:
                models.append(model)
                seen_models.add(model)

        if self._warn_recommended:
            warnings.warn(
                "Discovery mode 'interfaces' is convenient. "
                "For fully deterministic production bootstrap, prefer 'manifest'.",
                stacklevel=2,
            )

        return DiscoveryResult(
            models=tuple(models),
            use_cases=tuple(use_cases),
            interfaces=tuple(interfaces),
        )
