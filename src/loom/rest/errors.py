"""HTTP error mapping for REST endpoints."""

from __future__ import annotations

from typing import Any, ClassVar

from fastapi import HTTPException

from loom.core.errors import LoomError, RuleViolations


class HttpErrorMapper:
    """Maps ``LoomError`` subclasses to FastAPI ``HTTPException`` instances.

    Uses the ``code`` discriminator on each ``LoomError`` to select the
    appropriate HTTP status code. ``RuleViolations`` receives structured
    detail including per-field violation data.

    Unknown error codes default to ``500 Internal Server Error``.

    Example::

        mapper = HttpErrorMapper()
        try:
            ...
        except LoomError as exc:
            raise mapper.to_http(exc) from exc
    """

    _STATUS: ClassVar[dict[str, int]] = {
        "not_found": 404,
        "forbidden": 403,
        "conflict": 409,
        "rule_violations": 422,
        "rule_violation": 422,
        "system_error": 500,
    }

    def to_http(self, error: LoomError) -> HTTPException:
        """Convert a ``LoomError`` to an ``HTTPException``.

        Args:
            error: Domain or system error raised by the UseCase pipeline.

        Returns:
            ``HTTPException`` with the appropriate status code and structured
            detail body.
        """
        status = self._STATUS.get(error.code, 500)
        detail: dict[str, Any] = {"code": error.code, "message": error.message}

        if isinstance(error, RuleViolations):
            detail["violations"] = [
                {"field": v.field, "message": v.message} for v in error.violations
            ]

        return HTTPException(status_code=status, detail=detail)
