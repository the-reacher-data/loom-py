"""Bytewax handler for IntoSink storage terminal nodes.

Bridges the loom SinkPartition protocol to the Bytewax DynamicSink API.
One _StorageDynamicSink per IntoSink node; one _StorageSinkPartition per
Bytewax worker, built via node.build_partition(config, worker_index, worker_count).

Observability
-------------
``_StorageSinkPartition.write_batch`` emits a ``Scope.WRITE`` span around
every epoch flush.  The loom ``SinkPartition`` implementation stays free of
framework dependencies — observability lives entirely in the adapter layer.
"""

from __future__ import annotations

from typing import Any

from bytewax.operators import output as bw_output
from bytewax.outputs import DynamicSink, StatelessSinkPartition

from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.streaming.bytewax.handlers._shared import _BuildContextProtocol, _step_id
from loom.streaming.compiler._plan import CompiledStorageSink
from loom.streaming.core._exceptions import UnsupportedNodeError
from loom.streaming.core._message import Message
from loom.streaming.nodes._sink import IntoSink, SinkPartition

Stream = Any


class _StorageSinkPartition(StatelessSinkPartition[Message[Any]]):
    """Bytewax sink partition that delegates writes to a loom SinkPartition.

    Extracts the typed payload from each Loom ``Message`` envelope, emits a
    ``Scope.WRITE`` observability span around the batch flush, then forwards
    to the underlying partition.  Storage backends remain free of transport
    and observability concerns.

    Args:
        partition:  Loom SinkPartition built by ``IntoSink.build_partition``.
        node_name:  Human-readable sink identifier used in observability events.
        flow_name:  Name of the enclosing streaming flow.
        observer:   Observability runtime that receives WRITE lifecycle events.
    """

    def __init__(
        self,
        partition: SinkPartition[Any],
        *,
        node_name: str,
        flow_name: str,
        observer: ObservabilityRuntime,
    ) -> None:
        self._partition = partition
        self._node_name = node_name
        self._flow_name = flow_name
        self._observer = observer

    def write_batch(self, items: list[Message[Any]]) -> None:
        """Extract payloads and forward to the underlying partition.

        Emits ``Scope.WRITE`` START / END (or ERROR) events so that every
        epoch flush appears in logs, OTEL traces, and Prometheus metrics
        alongside regular node lifecycle events.

        Args:
            items: Loom messages delivered by Bytewax for the current epoch.
        """
        payloads = [item.payload for item in items]
        with self._observer.span(
            Scope.WRITE,
            f"{self._flow_name}:{self._node_name}",
            flow=self._flow_name,
            sink=self._node_name,
            batch_size=len(payloads),
        ):
            self._partition.write_batch(payloads)

    def close(self) -> None:
        """Delegate close to the underlying partition."""
        self._partition.close()


class _StorageDynamicSink(DynamicSink[Message[Any]]):
    """Bytewax DynamicSink that builds one _StorageSinkPartition per worker.

    Args:
        compiled:   Pre-resolved storage sink carrying the DSL node and config.
        observer:   Observability runtime forwarded to each per-worker partition.
        flow_name:  Name of the enclosing streaming flow.
    """

    def __init__(
        self,
        compiled: CompiledStorageSink,
        observer: ObservabilityRuntime,
        flow_name: str,
    ) -> None:
        self._compiled = compiled
        self._observer = observer
        self._flow_name = flow_name

    def build(
        self,
        step_id: str,
        worker_index: int,
        worker_count: int,
    ) -> _StorageSinkPartition:
        """Build the per-worker partition by delegating to the DSL node.

        Args:
            step_id:      Bytewax step identifier (unused by the partition).
            worker_index: Zero-based index of the calling worker.
            worker_count: Total number of workers in this run.

        Returns:
            A ready-to-write ``_StorageSinkPartition`` with observability wired.
        """
        del step_id
        node = self._compiled.node
        partition = node.build_partition(self._compiled.config, worker_index, worker_count)
        node_name = node.name or type(node).__name__
        return _StorageSinkPartition(
            partition,
            node_name=node_name,
            flow_name=self._flow_name,
            observer=self._observer,
        )


def _apply_into_sink(
    stream: Stream,
    raw: object,
    idx: int,
    ctx: _BuildContextProtocol,
) -> Stream:
    """Wire an IntoSink terminal node to a Bytewax output operator.

    Looks up the pre-compiled storage sink for the current path and registers
    a ``_StorageDynamicSink`` as the Bytewax output.  The sink is built once
    per worker at dataflow startup via ``IntoSink.build_partition``.

    Observability is threaded from the adapter context into each per-worker
    partition so that every epoch flush appears in the flow's lifecycle events.

    Args:
        stream: Incoming Bytewax stream of ``Message`` items.
        raw:    DSL node, expected to satisfy ``IntoSink``.
        idx:    Position of this node in the process.
        ctx:    Adapter build context.

    Returns:
        The input stream unchanged (terminal wiring is a side effect).

    Raises:
        UnsupportedNodeError: If *raw* does not satisfy ``IntoSink``.
        RuntimeError:         If no compiled storage sink exists for the path.
    """
    if not isinstance(raw, IntoSink):
        raise UnsupportedNodeError(f"Expected IntoSink, got {type(raw).__name__}.")
    path = ctx.current_path
    compiled = ctx.plan.terminal_storage_sinks.get(path)
    if compiled is None:
        raise RuntimeError(
            f"IntoSink '{type(raw).__name__}' at path {path} has no compiled storage sink; "
            "ensure compile_flow ran before building the dataflow."
        )
    sid = _step_id(f"storage_sink_{idx}", ctx)
    bw_output(sid, stream, _StorageDynamicSink(compiled, ctx.flow_runtime, ctx.plan.name))
    return stream
