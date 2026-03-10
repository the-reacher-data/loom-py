"""Celery-layer constants: task name prefixes, queue names, manifest attribute."""

from __future__ import annotations

TASK_JOB_PREFIX = "loom.job"
TASK_CALLBACK_PREFIX = "loom.callback"
TASK_CALLBACK_ERROR_PREFIX = "loom.callback_error"

QUEUE_DEFAULT = "default"

MANIFEST_ATTR = "MANIFEST"
