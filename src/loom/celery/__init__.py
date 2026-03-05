"""Loom Celery integration.

Provides distributed job dispatching via Celery.  All symbols here are
considered part of the public API of the optional ``loom[celery]`` extra.

Install with::

    pip install "loom-py[celery]"
"""

from loom.celery.auto import create_app
from loom.celery.bootstrap import WorkerBootstrapResult, bootstrap_worker
from loom.celery.service import CeleryJobService

__all__ = [
    "create_app",
    "bootstrap_worker",
    "WorkerBootstrapResult",
    "CeleryJobService",
]
