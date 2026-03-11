"""Machine-readable error codes shared across the framework.

``ErrorCode`` is a ``StrEnum`` so values are plain strings at runtime —
existing code that compares ``error.code == "not_found"`` continues to work
while new code can use the enum for typo-proof, IDE-navigable references.

Example::

    from loom.core.errors.codes import ErrorCode

    if error.code == ErrorCode.NOT_FOUND:
        ...
"""

from __future__ import annotations

from enum import StrEnum


class ErrorCode(StrEnum):
    """Canonical error discriminators used by :class:`~loom.core.errors.LoomError` subclasses.

    The REST adapter maps these to HTTP status codes via
    :class:`~loom.rest.errors.HttpErrorMapper`.
    """

    NOT_FOUND = "not_found"
    FORBIDDEN = "forbidden"
    CONFLICT = "conflict"
    RULE_VIOLATION = "rule_violation"
    RULE_VIOLATIONS = "rule_violations"
    SYSTEM_ERROR = "system_error"
