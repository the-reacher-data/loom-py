"""Application bootstrap pipeline for Loom.

Provides a single, deterministic startup sequence that wires configuration,
logging, the DI container, use-case compilation, and the factory — without
coupling to any web framework.
"""

from loom.core.bootstrap.bootstrap import BootstrapError, BootstrapResult, bootstrap_app

__all__ = ["BootstrapError", "BootstrapResult", "bootstrap_app"]
