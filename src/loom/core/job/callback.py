from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class JobCallback(Protocol):
    """Protocol for job lifecycle hooks.

    Implement this to react to job completion or failure.  Both methods
    may be plain ``def`` or ``async def`` — the runner detects the kind
    via ``inspect.iscoroutinefunction`` at execution time.

    Implementations are instantiated by the factory, so constructor
    dependencies are injected via DI like any other service.

    Example::

        class AuditCallback:
            def __init__(self, audit_repo: AuditRepository) -> None:
                self._repo = audit_repo

            async def on_success(
                self, job_id: str, result: Any, **context: Any
            ) -> None:
                await self._repo.record_success(job_id, result)

            async def on_failure(
                self, job_id: str, exc_type: str, exc_msg: str, **context: Any
            ) -> None:
                await self._repo.record_failure(job_id, exc_type, exc_msg)
    """

    def on_success(self, job_id: str, result: Any, **context: Any) -> Any:
        """Called when the job completes successfully.

        Args:
            job_id: UUID of the completed job.
            result: Value returned by ``Job.execute()``.
            **context: Original dispatch payload forwarded as keyword args.
        """
        ...

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: Any) -> Any:
        """Called when the job raises an unrecoverable exception.

        Args:
            job_id: UUID of the failed job.
            exc_type: Fully-qualified exception class name.
            exc_msg: String representation of the exception.
            **context: Original dispatch payload forwarded as keyword args.
        """
        ...


class NullJobCallback:
    """No-op callback for tests and contexts that do not need lifecycle hooks.

    Satisfies the ``JobCallback`` protocol without any side effects.

    Example::

        service = InlineJobService(factory, executor, callback=NullJobCallback())
    """

    def on_success(self, job_id: str, result: Any, **context: Any) -> None:
        """Accept success notification and do nothing."""

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: Any) -> None:
        """Accept failure notification and do nothing."""
