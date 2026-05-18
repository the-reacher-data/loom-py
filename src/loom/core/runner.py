"""Runner contracts shared by Loom domains.

The protocol in this module intentionally stays small. A runner is the
already-built façade that executes a workload; it does not own YAML
loading or compilation responsibilities. Optional lifecycle hooks are
split into separate structural protocols so domains only implement what
they actually need.
"""

from __future__ import annotations

from typing import Protocol, TypeVar, runtime_checkable

ResultT = TypeVar("ResultT", covariant=True)


@runtime_checkable
class RunnerProtocol(Protocol[ResultT]):
    """Structural protocol for an already-constructed runner.

    A runner executes work and returns a domain-specific result. It is the
    public façade consumed by applications, tests, and orchestration layers.
    """

    def run(self) -> ResultT:
        """Execute the underlying workload and return its result."""
        ...


@runtime_checkable
class SupportsShutdown(Protocol):
    """Structural protocol for runners that own shutdownable resources."""

    def shutdown(self) -> None:
        """Release any runner-owned resources."""
        ...


@runtime_checkable
class SupportsFlush(Protocol):
    """Structural protocol for runners that can flush buffered state."""

    def flush(self) -> None:
        """Flush buffered metrics, events, or other deferred state."""
        ...


__all__ = ["ResultT", "RunnerProtocol", "SupportsFlush", "SupportsShutdown"]
