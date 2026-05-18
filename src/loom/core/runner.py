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


def shutdown_runner(runner: object) -> None:
    """Call ``shutdown()`` when *runner* exposes the shutdown capability."""
    shutdown = getattr(runner, "shutdown", None)
    if callable(shutdown):
        shutdown()


def flush_runner(runner: object) -> None:
    """Call ``flush()`` when *runner* exposes the flush capability."""
    flush = getattr(runner, "flush", None)
    if callable(flush):
        flush()


__all__ = [
    "ResultT",
    "RunnerProtocol",
    "SupportsFlush",
    "SupportsShutdown",
    "flush_runner",
    "shutdown_runner",
]
