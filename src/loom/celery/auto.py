"""Automatic Celery app creation from YAML configuration."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from celery import Celery  # type: ignore[import-untyped]

from loom.celery.bootstrap import bootstrap_worker
from loom.core.di.container import LoomContainer

if TYPE_CHECKING:
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.job.job import Job


def create_app(
    *config_paths: str,
    jobs: Sequence[type[Job[Any]]] = (),
    callbacks: Sequence[type[Any]] = (),
    modules: Sequence[Callable[[LoomContainer], None]] = (),
    metrics: MetricsAdapter | None = None,
) -> Celery:
    """Create a Celery app with all Loom job tasks registered.

    Args:
        *config_paths: One or more YAML config files merged left-to-right.
        jobs: Concrete ``Job`` subclasses to register as Celery tasks.
            Optional when ``app.discovery`` (modules/manifest) is configured.
        callbacks: Optional callback classes for success/failure hooks.
        modules: Optional DI registration modules.
        metrics: Optional metrics adapter.

    Returns:
        Configured Celery application.
    """
    result = bootstrap_worker(
        *config_paths,
        jobs=jobs,
        callbacks=callbacks,
        modules=modules,
        metrics=metrics,
    )
    return result.celery_app
