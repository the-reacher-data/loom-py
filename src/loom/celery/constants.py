"""Celery-layer constants: task names, queues, and worker manifest keys."""

from __future__ import annotations

from enum import StrEnum

TASK_JOB_PREFIX = "loom.job"
TASK_CALLBACK_PREFIX = "loom.callback"
TASK_CALLBACK_ERROR_PREFIX = "loom.callback_error"

QUEUE_DEFAULT = "default"


class WorkerManifestAttr(StrEnum):
    """Attribute names expected on worker manifest modules."""

    MANIFEST = "MANIFEST"
    MODELS = "MODELS"
    USE_CASES = "USE_CASES"
    INTERFACES = "INTERFACES"
    JOBS = "JOBS"
    CALLBACKS = "CALLBACKS"
