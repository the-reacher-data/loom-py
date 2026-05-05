"""Bytewax runtime adapter.

Translates a :class:`CompiledPlan` into a Bytewax :class:`Dataflow`,
wiring decode, node dispatch, encode, and output routing operators.

Requires ``bytewax`` to be installed.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Protocol, TypeAlias, cast, runtime_checkable

import bytewax.dataflow as _bytewax_dataflow
from bytewax.operators import branch
from bytewax.operators import input as bw_input
from bytewax.operators import map as bw_map
from bytewax.outputs import StatelessSinkPartition

from loom.core.async_bridge import AsyncBridge
from loom.streaming.bytewax._output_wiring import OutputWiringManager
from loom.streaming.bytewax._resource_manager import ResourceManager
from loom.streaming.bytewax._runtime_io import build_runtime_terminal_sinks
from loom.streaming.bytewax.handlers._shared import _OutputWiringProtocol
from loom.streaming.bytewax.handlers.dispatcher import (
    _NODE_HANDLERS,
    _wire_process,
)
from loom.streaming.compiler import CompiledPlan
from loom.streaming.compiler._plan import CompiledMultiSource
from loom.streaming.core._errors import ErrorKind
from loom.streaming.core._message import Message
from loom.streaming.core._typing import StreamPayload
from loom.streaming.kafka._codec import MsgspecCodec
from loom.streaming.kafka._record import KafkaRecord
from loom.streaming.kafka._wire import (
    DecodeOk,
    DecodeResult,
    try_decode_multi_record,
    try_decode_record,
)
from loom.streaming.nodes._with import With, WithAsync
from loom.streaming.observability.observers.protocol import StreamingFlowObserver

logger = logging.getLogger(__name__)
Stream: TypeAlias = Any

__all__ = ["build_dataflow", "build_dataflow_with_shutdown", "_NODE_HANDLERS"]


@dataclass(frozen=True)
class _BuiltDataflow:
    """Bytewax dataflow plus adapter-owned shutdown callback."""

    dataflow: Any
    shutdown: Callable[[], None]


def build_dataflow(
    plan: CompiledPlan,
    *,
    flow_observer: StreamingFlowObserver | None = None,
    source: Any | None = None,
    sink: Any | None = None,
    terminal_sinks: Mapping[tuple[int, ...], Any] | None = None,
    error_sinks: Mapping[ErrorKind, Any] | None = None,
) -> Any:
    """Build a Bytewax Dataflow from a compiled plan."""
    return build_dataflow_with_shutdown(
        plan,
        flow_observer=flow_observer,
        source=source,
        sink=sink,
        terminal_sinks=terminal_sinks,
        error_sinks=error_sinks,
    ).dataflow


def build_dataflow_with_shutdown(
    plan: CompiledPlan,
    *,
    flow_observer: StreamingFlowObserver | None = None,
    source: Any | None = None,
    sink: Any | None = None,
    terminal_sinks: Mapping[tuple[int, ...], Any] | None = None,
    error_sinks: Mapping[ErrorKind, Any] | None = None,
    bridge: AsyncBridge | None = None,
    commit_tracker: Any | None = None,
) -> _BuiltDataflow:
    """Build a Bytewax Dataflow and expose its shutdown callback.

    Args:
        plan: Compiled flow plan.
        flow_observer: Optional observer for lifecycle events.
        source: Optional Bytewax source override (used in tests).
        sink: Optional Bytewax sink override (used in tests).
        terminal_sinks: Optional per-branch sink overrides.
        error_sinks: Optional per-kind error sink overrides.
        bridge: Pre-configured :class:`AsyncBridge`.  When ``None``, a default
            asyncio bridge is created if the plan requires async execution.
            Pass an explicit bridge to control backend and uvloop settings.
    """
    if terminal_sinks is None:
        terminal_sinks = build_runtime_terminal_sinks(plan.terminal_sinks, commit_tracker)
    _bind_commit_tracker_object(source, commit_tracker)
    _bind_commit_tracker_object(sink, commit_tracker)
    _bind_commit_tracker_mapping(terminal_sinks, commit_tracker)
    _bind_commit_tracker_mapping(error_sinks, commit_tracker)
    resolved_bridge = bridge if bridge is not None else _maybe_create_bridge(plan)
    ctx = _BuildContext(
        plan=plan,
        bridge=resolved_bridge,
        flow_observer=flow_observer,
        source=source,
        sink=sink,
        terminal_sinks=terminal_sinks,
        error_sinks=error_sinks,
        commit_tracker=commit_tracker,
    )
    return _BuiltDataflow(dataflow=_assemble_dataflow(plan, ctx), shutdown=ctx.shutdown_all)


@runtime_checkable
class _SupportsCommitBind(Protocol):
    """Runtime object that accepts a Kafka commit tracker."""

    def bind_commit_tracker(self, tracker: Any) -> None:
        """Bind a commit tracker to this runtime object."""


def _bind_commit_tracker_object(item: object | None, commit_tracker: object | None) -> None:
    """Bind a commit tracker to one runtime object when supported."""
    if item is None or commit_tracker is None:
        return
    if isinstance(item, _SupportsCommitBind):
        item.bind_commit_tracker(commit_tracker)


def _bind_commit_tracker_mapping(
    items: Mapping[Any, Any] | None,
    commit_tracker: Any | None,
) -> None:
    """Bind a commit tracker to each runtime object in a mapping when supported."""
    if items is None or commit_tracker is None:
        return
    for item in items.values():
        _bind_commit_tracker_object(item, commit_tracker)


def _assemble_dataflow(plan: CompiledPlan, ctx: _BuildContext) -> Any:
    """Assemble a Bytewax Dataflow from a pre-built context."""
    flow = _bytewax_dataflow.Dataflow(plan.name)
    stream = _build_source_pipeline(flow, ctx)
    stream = _wire_process(stream, tuple(node.node for node in plan.nodes), ctx)

    _wire_output(stream, ctx)
    return flow


class _BuildContext:
    """Wiring-phase state shared across operator builders."""

    __slots__ = (
        "plan",
        "bridge",
        "commit_tracker",
        "flow_observer",
        "source",
        "outputs",
        "resource_manager",
        "_path",
        "_terminal_sinks",
    )

    def __init__(
        self,
        plan: CompiledPlan,
        bridge: AsyncBridge | None,
        flow_observer: StreamingFlowObserver | None = None,
        source: Any | None = None,
        sink: Any | None = None,
        terminal_sinks: Mapping[tuple[int, ...], Any] | None = None,
        error_sinks: Mapping[ErrorKind, Any] | None = None,
        commit_tracker: Any | None = None,
    ) -> None:
        self.plan = plan
        self.bridge = bridge
        self.commit_tracker = commit_tracker
        self.flow_observer = flow_observer
        self.source = source
        self._terminal_sinks: Mapping[tuple[int, ...], Any] = terminal_sinks or {}
        self.outputs: _OutputWiringProtocol = OutputWiringManager(
            sink=sink,
            terminal_sinks=self._terminal_sinks,
            error_sinks=error_sinks or {},
            observer=flow_observer,
            flow_name=plan.name,
        )
        self.resource_manager = ResourceManager(bridge)
        self._path: tuple[int, ...] = ()

    def inline_sink_partition_for(
        self,
        path: tuple[int, ...],
    ) -> StatelessSinkPartition[Any] | None:
        """Build an inline sink partition for the given path.

        Delegates to the runtime-wired Bytewax ``Sink`` for the path so that
        test doubles (e.g. ``TestingSink``) are honoured instead of always
        creating real Kafka producers.

        Args:
            path: Compiled path identifying the terminal sink.

        Returns:
            A ready-to-write ``StatelessSinkPartition``, or ``None`` if no
            sink is registered for *path*.
        """
        sink = self._terminal_sinks.get(path)
        if sink is None:
            return None
        step_id = "inline_" + "_".join(str(p) for p in path)
        return cast(StatelessSinkPartition[Any], sink.build(step_id, 0, 1))

    def manager_for(
        self,
        idx: int,
        node: With[StreamPayload, StreamPayload] | WithAsync[StreamPayload, StreamPayload],
    ) -> Any:
        """Get or create a resource manager for *node* at position *idx*."""
        return self.resource_manager.manager_for(idx, node)

    @property
    def current_path(self) -> tuple[int, ...]:
        """Return the current wiring path inside the process tree."""
        return self._path

    @contextmanager
    def enter_path(self, path: tuple[int, ...]) -> Iterator[None]:
        """Temporarily set the current wiring path."""
        previous = self._path
        self._path = path
        try:
            yield
        finally:
            self._path = previous

    def wire_process(
        self,
        stream: Any,
        nodes: tuple[object, ...],
        *,
        path_prefix: tuple[int, ...] = (),
    ) -> Any:
        """Wire one nested process subtree."""
        return _wire_process(stream, nodes, self, path_prefix=path_prefix)

    def shutdown_all(self) -> None:
        """Shutdown all resource managers."""
        self.resource_manager.shutdown_all()


def _build_source_pipeline(flow: Any, ctx: _BuildContext) -> Stream:
    """Build the source-side pipeline up to the first decoded Message stream."""
    if ctx.source is None:
        raise RuntimeError("Bytewax source is required to build a runtime dataflow.")
    source = ctx.source
    codec: MsgspecCodec[Any] = MsgspecCodec()
    strategy = ctx.plan.source.decode_strategy
    step_id = f"decode_{strategy}"

    stream: Stream = bw_input("source", flow, source)
    decoded = _decode_source_stream(stream, ctx, codec, step_id)
    decoded_branch = _split_decode_results(decoded, step_id)
    ctx.outputs.wire_decode_error(decoded_branch.falses, ctx.plan)
    return bw_map(f"{step_id}_message", decoded_branch.trues, _decode_ok_message)


def _decode_source_stream(
    stream: Stream,
    ctx: _BuildContext,
    codec: MsgspecCodec[Any],
    step_id: str,
) -> Stream:
    """Map raw source items into decode results without raising wire errors."""
    return bw_map(step_id, stream, lambda item: _decode_source_record(item, ctx, codec))


def _split_decode_results(stream: Stream, step_id: str) -> Any:
    """Split decode results into successful messages and wire errors."""
    return branch(f"{step_id}_is_ok", stream, _is_decode_ok)


def _wire_output(stream: Any, ctx: _BuildContext) -> None:
    """Wire the output sink and error routes."""
    ctx.outputs.wire_flow_output(stream, ctx.plan)


def _maybe_create_bridge(plan: CompiledPlan) -> AsyncBridge | None:
    """Create a default asyncio AsyncBridge if the plan requires async execution.

    Used as a fallback when no pre-configured bridge is supplied to
    :func:`build_dataflow_with_shutdown` — e.g. in test helpers or direct
    adapter use.  Production runners should pass an explicit bridge created
    via :func:`~loom.streaming.bytewax.runner._create_bridge` so that backend
    and uvloop settings from :class:`BytewaxRuntimeConfig` are applied.
    """
    if not plan.needs_async_bridge:
        return None
    return AsyncBridge()


def _decode_source_record(
    payload: Any,
    ctx: _BuildContext,
    codec: MsgspecCodec[Any],
) -> DecodeResult[StreamPayload]:
    """Decode source records into DSL messages without raising decode errors."""
    if isinstance(payload, Message):
        return DecodeOk(message=cast(Message[StreamPayload], payload))
    if isinstance(payload, KafkaRecord):
        record = cast(KafkaRecord[bytes], payload)
        source = ctx.plan.source
        if isinstance(source, CompiledMultiSource):
            return try_decode_multi_record(record, source.dispatch, codec)
        return try_decode_record(record, source.payload_type, codec)
    raise TypeError(f"Expected Message or KafkaRecord, got {type(payload).__name__}.")


def _is_decode_ok(result: DecodeResult[StreamPayload]) -> bool:
    """Return whether a source decode result can continue through the flow."""
    return isinstance(result, DecodeOk)


def _decode_ok_message(result: DecodeResult[StreamPayload]) -> Message[StreamPayload]:
    """Unwrap a successful source decode result."""
    if isinstance(result, DecodeOk):
        return result.message
    raise TypeError(f"Expected DecodeOk, got {type(result).__name__}.")
