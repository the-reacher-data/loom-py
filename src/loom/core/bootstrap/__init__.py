"""Application bootstrap pipeline for Loom.

Provides a single, deterministic startup sequence that wires configuration,
logging, the DI container, use-case compilation, and the factory — without
coupling to any web framework.
"""

from loom.core.bootstrap.bootstrap import BootstrapError, BootstrapResult, bootstrap_app
from loom.core.bootstrap.kernel import KernelRuntime, create_kernel

__all__ = [
    "BootstrapError",
    "BootstrapResult",
    "KernelRuntime",
    "bootstrap_app",
    "create_kernel",
]
