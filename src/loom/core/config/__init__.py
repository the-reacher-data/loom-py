"""Configuration loading utilities for Loom applications.

Provides an omegaConf-backed loader that supports multiple YAML files and
typed section extraction.  The framework imposes no shape on the config
object — the user owns the structure.

Example::

    from loom.core.config import load_config, section

    cfg = load_config("config/base.yaml", "config/local.yaml")
    db = section(cfg, "database", DatabaseConfig)
"""

from loom.core.config.errors import ConfigError
from loom.core.config.loader import load_config, section
from loom.core.config.resolver import ConfigResolver

__all__ = ["ConfigError", "ConfigResolver", "load_config", "section"]
