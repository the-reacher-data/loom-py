"""Discovery engine that scans explicitly declared modules."""

from __future__ import annotations

from loom.core.discovery._utils import collect_from_modules, import_modules
from loom.core.discovery.base import DiscoveryResult


class ModulesDiscoveryEngine:
    """Discover models, use cases and interfaces from module paths."""

    def __init__(self, module_paths: list[str]) -> None:
        self._module_paths = module_paths

    def discover(self) -> DiscoveryResult:
        if not self._module_paths:
            raise ValueError("modules discovery requires at least one module path.")

        modules = import_modules(self._module_paths)
        models, use_cases, interfaces = collect_from_modules(modules)
        return DiscoveryResult(
            models=tuple(models),
            use_cases=tuple(use_cases),
            interfaces=tuple(interfaces),
        )
