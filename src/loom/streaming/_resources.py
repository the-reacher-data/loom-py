"""Resource lifecycle contracts for streaming tasks."""

from __future__ import annotations

from typing import Protocol, TypeVar, runtime_checkable

ResourceT = TypeVar("ResourceT")
ResourceCoT = TypeVar("ResourceCoT", covariant=True)


@runtime_checkable
class ResourceFactory(Protocol[ResourceT]):
    """Create and close task resources under runtime control."""

    def create(self) -> ResourceT:
        """Create one worker-local resource."""
        ...

    def close(self, resource: ResourceT) -> None:
        """Close one resource created by this factory."""
        ...


@runtime_checkable
class TaskContext(Protocol[ResourceCoT]):
    """Execution context with explicit resource access."""

    @property
    def resource(self) -> ResourceCoT:
        """Worker-local resource available to the task."""
        ...


__all__ = ["ResourceFactory", "TaskContext"]
