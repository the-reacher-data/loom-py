"""Discovery engine that starts from REST interface modules."""

from __future__ import annotations

import warnings

from loom.core.discovery._utils import (
    collect_from_modules,
    collect_use_cases_from_interfaces,
    import_modules,
    infer_model_from_use_case,
)
from loom.core.discovery.base import DiscoveryResult
from loom.core.model import BaseModel


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
        """Scan interface modules and extract all UseCases referenced by routes.

        Auto-generated UseCases (from ``auto=True`` interfaces) are included
        because they are registered on the interface class at definition time
        via :func:`~loom.rest.model.RestInterface.__init_subclass__`.

        Returns:
            :class:`~loom.core.discovery.base.DiscoveryResult` with all
            discovered models, use cases, and interfaces.

        Raises:
            ValueError: When no interface module paths are provided, or when
                no ``RestInterface`` subclasses are found.
        """
        if not self._interface_modules:
            raise ValueError("interfaces discovery requires at least one module path.")

        modules = import_modules(self._interface_modules)
        _, _, interfaces, _ = collect_from_modules(modules)
        if not interfaces:
            raise ValueError("No RestInterface subclasses discovered in interface modules.")

        use_cases = collect_use_cases_from_interfaces(list(interfaces))

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
