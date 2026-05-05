"""Output wiring helpers for the Bytewax adapter."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

from bytewax.operators import output as bw_output
from bytewax.outputs import DynamicSink, StatelessSinkPartition

from loom.streaming.bytewax.handlers._shared import _OutputWiringProtocol
from loom.streaming.compiler._plan import CompiledPlan
from loom.streaming.core._errors import ErrorKind

logger = logging.getLogger(__name__)

Stream = Any


@runtime_checkable
class _OutputWiringManagerProtocol(Protocol):
    """Write flow outputs and error routes during adapter wiring."""

    def needs_flow_output(self) -> bool:
        """Return whether the flow output still needs wiring."""
        ...

    def wire_terminal(self, step_id: str, stream: Stream) -> None:
        """Wire one terminal sink."""
        ...

    def wire_branch_terminal(self, step_id: str, stream: Stream, path: tuple[int, ...]) -> None:
        """Wire one branch terminal sink."""
        ...

    def wire_node_error(self, kind: ErrorKind, step_id: str, stream: Stream) -> None:
        """Wire one node error branch."""
        ...

    def wire_flow_output(self, stream: Stream, plan: CompiledPlan) -> None:
        """Wire the flow-level output and declared error routes."""
        ...

    def wire_decode_error(self, stream: Stream, plan: CompiledPlan) -> None:
        """Wire source decode errors."""
        ...


class _DropObserverSinkPartition(StatelessSinkPartition[Any]):
    """Sink partition that notifies the observer for each unrouted error item."""

    def __init__(self, observer: Any, flow_name: str, kind: ErrorKind) -> None:
        self._observer = observer
        self._flow_name = flow_name
        self._kind = kind

    def write_batch(self, items: list[Any]) -> None:
        exc = RuntimeError(
            f"No sink registered for {self._kind.value} error route — message dropped"
        )
        for _ in items:
            self._observer.on_node_error(
                self._flow_name,
                -1,
                node_type=f"unrouted_{self._kind.value}_error",
                exc=exc,
            )


class _DropObserverSink(DynamicSink[Any]):
    """Notifies the observer for every item on an unrouted error stream."""

    def __init__(self, observer: Any, flow_name: str, kind: ErrorKind) -> None:
        self._observer = observer
        self._flow_name = flow_name
        self._kind = kind

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> StatelessSinkPartition[Any]:
        del step_id, worker_index, worker_count
        return _DropObserverSinkPartition(self._observer, self._flow_name, self._kind)


class _DropLoggingSinkPartition(StatelessSinkPartition[Any]):
    """Sink partition that logs every unrouted error item."""

    def __init__(self, flow_name: str, kind: ErrorKind) -> None:
        self._flow_name = flow_name
        self._kind = kind

    def write_batch(self, items: list[Any]) -> None:
        for _ in items:
            logger.warning(
                "unrouted_error_dropped",
                extra={"flow": self._flow_name, "error_kind": self._kind.value},
            )


class _DropLoggingSink(DynamicSink[Any]):
    """Logs every item on an unrouted error stream."""

    def __init__(self, flow_name: str, kind: ErrorKind) -> None:
        self._flow_name = flow_name
        self._kind = kind

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> StatelessSinkPartition[Any]:
        del step_id, worker_index, worker_count
        return _DropLoggingSinkPartition(self._flow_name, self._kind)


class OutputWiringManager(_OutputWiringProtocol):
    """Coordinate terminal output and explicit error routes for one build."""

    __slots__ = (
        "sink",
        "error_sinks",
        "terminal_sinks",
        "_terminal_output_wired",
        "_observer",
        "_flow_name",
    )

    def __init__(
        self,
        *,
        sink: Any | None,
        error_sinks: Mapping[ErrorKind, Any],
        terminal_sinks: Mapping[tuple[int, ...], Any],
        observer: Any | None = None,
        flow_name: str = "",
    ) -> None:
        self.sink = sink
        self.error_sinks = error_sinks
        self.terminal_sinks = terminal_sinks
        self._terminal_output_wired = False
        self._observer = observer
        self._flow_name = flow_name

    def needs_flow_output(self) -> bool:
        return not self._terminal_output_wired

    def wire_terminal(self, step_id: str, stream: Stream) -> None:
        if self.sink is None:
            raise RuntimeError("Bytewax sink is required for terminal output wiring.")
        bw_output(step_id, stream, self.sink)
        self._terminal_output_wired = True

    def wire_branch_terminal(self, step_id: str, stream: Stream, path: tuple[int, ...]) -> None:
        sink = self.terminal_sinks.get(path)
        if sink is None:
            return
        bw_output(_qualified_step_id(step_id, path), stream, sink)

    def wire_node_error(self, kind: ErrorKind, step_id: str, stream: Stream) -> None:
        sink = self.error_sinks.get(kind)
        if sink is not None:
            bw_output(f"{step_id}_{kind.value}_errors", stream, sink)
            return
        drop_sink: DynamicSink[Any]
        if self._observer is not None:
            drop_sink = _DropObserverSink(self._observer, self._flow_name, kind)
            bw_output(f"{step_id}_{kind.value}_dropped", stream, drop_sink)
            return
        drop_sink = _DropLoggingSink(self._flow_name, kind)
        bw_output(f"{step_id}_{kind.value}_dropped", stream, drop_sink)

    def wire_flow_output(self, stream: Stream, plan: CompiledPlan) -> None:
        if not self.needs_flow_output():
            return

        if self.sink is None and plan.output is not None:
            raise RuntimeError("Bytewax sink is required for terminal output wiring.")
        if self.sink is not None:
            self.wire_terminal("output", stream)

        for kind, sink in plan.error_routes.items():
            logger.info(
                "error_route_registered",
                extra={"error_kind": kind.value, "topic": sink.topic},
            )
            if kind not in self.error_sinks:
                raise RuntimeError(f"Bytewax sink is required for error route {kind.value}.")

    def wire_decode_error(self, stream: Stream, plan: CompiledPlan) -> None:
        if self.error_sinks.get(ErrorKind.WIRE) is not None:
            self.wire_node_error(ErrorKind.WIRE, "decode", stream)
            return

        if ErrorKind.WIRE in plan.error_routes:
            raise RuntimeError("Bytewax sink is required for WIRE error routing.")


def _qualified_step_id(step_id: str, path: tuple[int, ...]) -> str:
    if not path:
        return step_id
    return "_".join((step_id, *map(str, path)))


__all__ = ["OutputWiringManager", "_OutputWiringManagerProtocol"]
