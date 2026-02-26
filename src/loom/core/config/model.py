"""Configuration model — intentionally empty.

The framework does not impose any base class for application configuration.
Users define their own config structures (``msgspec.Struct``, dataclasses,
``TypedDict``, or plain dicts loaded from omegaConf).

Use :func:`~loom.core.config.loader.load_config` to load YAML files and
:func:`~loom.core.config.loader.section` to extract typed sub-sections.
"""
