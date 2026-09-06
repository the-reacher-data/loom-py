"""Errors a repository raises for query shapes its backend cannot serve."""

from __future__ import annotations

from loom.core.errors import DomainError
from loom.core.errors.codes import ErrorCode


class UnsupportedQuery(DomainError):
    """Raised when a backend cannot serve a query because of its arguments.

    A capability the repository class does not declare is absent from DI and
    from the generated routes; this error covers the gaps that depend on a
    run-time argument instead, such as a lookup on a field the backend can
    only reach through a scan.

    Args:
        backend: Name of the persistence backend.
        model: Qualified name of the model being queried.
        reason: What the backend cannot do with the given arguments.
    """

    def __init__(self, backend: str, model: str, reason: str) -> None:
        self.backend = backend
        self.model = model
        self.reason = reason
        super().__init__(
            f"{backend} cannot serve this query on {model}: {reason}",
            code=ErrorCode.UNSUPPORTED_QUERY,
        )
