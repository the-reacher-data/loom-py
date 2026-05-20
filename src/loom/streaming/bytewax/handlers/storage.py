"""Bytewax handler for IntoSink storage terminal nodes.

Bridges the loom SinkPartition protocol to the Bytewax DynamicSink API.
One _StorageDynamicSink per IntoSink node; one _StorageSinkPartition per
Bytewax worker, built via node.build_partition(config, worker_index, worker_count).
"""

from __future__ import annotations

from typing import Any

from bytewax.operators import output as bw_output
from bytewax.outputs import DynamicSink, StatelessSinkPartition

from loom.streaming.bytewax.handlers._shared import _BuildContextProtocol, _step_id
from loom.streaming.compiler._plan import CompiledStorageSink
from loom.streaming.core._exceptions import UnsupportedNodeError
from loom.streaming.core._message import Message
from loom.streaming.nodes._sink import IntoSink, SinkPartition

Stream = Any


class _StorageSinkPartition(StatelessSinkPartition[Message[Any]]):
    """Bytewax sink partition that delegates writes to a loom SinkPartition.

    Extracts the typed payload from each Loom ``Message`` envelope before
    forwarding to the underlying partition, keeping storage backends free of
    transport concerns.

    Args:
        partition: Loom SinkPartition built by ``IntoSink.build_partition``.
    """

    def __init__(self, partition: SinkPartition[Any]) -> None:
        self._partition = partition

    def write_batch(self, items: list[Message[Any]]) -> None:
        """Forward payload-only items to the underlying partition.

        Implementations may process records one-by-one or in bulk — the
        ``write_batch`` contract imposes no restriction.

        Args:
            items: Loom messages delivered by Bytewax for the current epoch.
        """
        self._partition.write_batch([item.payload for item in items])

    def close(self) -> None:
        """Delegate close to the underlying partition."""
        self._partition.close()


class _StorageDynamicSink(DynamicSink[Message[Any]]):
    """Bytewax DynamicSink that builds one _StorageSinkPartition per worker.

    Args:
        compiled: Pre-resolved storage sink carrying the DSL node and config.
    """

    def __init__(self, compiled: CompiledStorageSink) -> None:
        self._compiled = compiled

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
            A ready-to-write ``_StorageSinkPartition``.
        """
        del step_id
        partition = self._compiled.node.build_partition(
            self._compiled.config, worker_index, worker_count
        )
        return _StorageSinkPartition(partition)


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
    bw_output(sid, stream, _StorageDynamicSink(compiled))
    return stream
