"""Canonical configuration section keys used across bootstrap layers."""

from __future__ import annotations

from enum import StrEnum


class ConfigKey(StrEnum):
    """Top-level YAML section keys consumed by :func:`~loom.core.config.loader.section`.

    Using ``StrEnum`` keeps references typo-proof while remaining compatible
    with any API that expects plain ``str`` section identifiers.

    Example::

        cfg = section(raw, ConfigKey.CELERY, CeleryConfig)
    """

    APP = "app"
    DATABASE = "database"
    CELERY = "celery"
    OBSERVABILITY = "observability"
    LOGGER = "logger"
    METRICS = "metrics"
    JOBS = "jobs"
