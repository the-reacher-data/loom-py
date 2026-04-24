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
from typing import Any, TypeAlias, cast

import bytewax.dataflow as _bytewax_dataflow
from bytewax.operators import branch
from bytewax.operators import input as bw_input
from bytewax.operators import map as bw_map
from bytewax.operators import output as bw_output

from loom.core.async_bridge import AsyncBridge
from loom.streaming.bytewax._node_handlers import (
    _NODE_HANDLERS,
    _OutputWiringProtocol,
    _wire_process,
)
from loom.streaming.bytewax._node_handlers import (
    _batch_key as _node_batch_key,
)
from loom.streaming.bytewax._operators import ResourceLifecycle, lifecycle_for
from loom.streaming.bytewax._runtime_io import build_runtime_terminal_sinks
from loom.streaming.compiler import CompiledPlan
from loom.streaming.core._errors import ErrorKind
from loom.streaming.core._message import Message
from loom.streaming.core._typing import StreamPayload
from loom.streaming.kafka._codec import MsgspecCodec
from loom.streaming.kafka._record import KafkaRecord
from loom.streaming.kafka._wire import DecodeOk, DecodeResult, try_decode_record
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
) -> _BuiltDataflow:
    """Build a Bytewax Dataflow and expose its shutdown callback."""
    if terminal_sinks is None:
        terminal_sinks = build_runtime_terminal_sinks(plan.terminal_sinks)
    ctx = _BuildContext(
        plan=plan,
        bridge=_maybe_create_bridge(plan),
        flow_observer=flow_observer,
        source=source,
        sink=sink,
        terminal_sinks=terminal_sinks,
        error_sinks=error_sinks,
    )
    return _BuiltDataflow(dataflow=_assemble_dataflow(plan, ctx), shutdown=ctx.shutdown_all)


def _assemble_dataflow(plan: CompiledPlan, ctx: _BuildContext) -> Any:
    """Assemble a Bytewax Dataflow from a pre-built context."""
    flow = _bytewax_dataflow.Dataflow(plan.name)
    stream = _build_source_pipeline(flow, ctx)
    stream = _wire_process(stream, tuple(node.node for node in plan.nodes), ctx)

    _wire_output(stream, ctx)
    return flow


class _BuildContext:
    """Wiring-phase state shared across operator builders."""

    __slots__ = ("plan", "bridge", "flow_observer", "source", "outputs", "_managers", "_path")

    def __init__(
        self,
        plan: CompiledPlan,
        bridge: AsyncBridge | None,
        flow_observer: StreamingFlowObserver | None = None,
        source: Any | None = None,
        sink: Any | None = None,
        terminal_sinks: Mapping[tuple[int, ...], Any] | None = None,
        error_sinks: Mapping[ErrorKind, Any] | None = None,
    ) -> None:
        self.plan = plan
        self.bridge = bridge
        self.flow_observer = flow_observer
        self.source = source
        self.outputs: _OutputWiringProtocol = _OutputWiring(
            sink=sink,
            terminal_sinks=terminal_sinks or {},
            error_sinks=error_sinks or {},
        )
        self._managers: dict[int, ResourceLifecycle] = {}
        self._path: tuple[int, ...] = ()

    def manager_for(
        self,
        idx: int,
        node: With[StreamPayload, StreamPayload] | WithAsync[StreamPayload, StreamPayload],
    ) -> ResourceLifecycle:
        """Get or create a resource manager for *node* at position *idx*."""
        if idx not in self._managers:
            self._managers[idx] = lifecycle_for(node, bridge=self.bridge)
        return self._managers[idx]

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

    def shutdown_all(self) -> None:
        """Shutdown all resource managers."""
        for manager in self._managers.values():
            manager.shutdown()
        if self.bridge is not None:
            self.bridge.shutdown()
            self.bridge = None


class _OutputWiring:
    """Coordinate terminal output and explicit error routes for one build."""

    __slots__ = ("sink", "error_sinks", "terminal_sinks", "_terminal_output_wired")

    def __init__(
        self,
        *,
        sink: Any | None,
        error_sinks: Mapping[ErrorKind, Any],
        terminal_sinks: Mapping[tuple[int, ...], Any],
    ) -> None:
        self.sink = sink
        self.error_sinks = error_sinks
        self.terminal_sinks = terminal_sinks
        self._terminal_output_wired = False

    def needs_flow_output(self) -> bool:
        """Return whether the flow-level output still needs wiring."""
        return not self._terminal_output_wired

    def wire_terminal(self, step_id: str, stream: Stream) -> None:
        """Wire the configured terminal sink exactly once."""
        if self.sink is None:
            raise RuntimeError("Bytewax sink is required for terminal output wiring.")
        bw_output(step_id, stream, self.sink)
        self._terminal_output_wired = True

    def wire_branch_terminal(self, step_id: str, stream: Stream, path: tuple[int, ...]) -> None:
        """Wire one branch terminal sink by its compiled path."""
        sink = self.terminal_sinks.get(path)
        if sink is None and self.sink is not None and not self.terminal_sinks:
            sink = self.sink
            self._terminal_output_wired = True
        if sink is None:
            raise RuntimeError(f"Bytewax sink is required for branch terminal path {path}.")
        bw_output(_qualified_step_id(step_id, path), stream, sink)

    def wire_node_error(self, kind: ErrorKind, step_id: str, stream: Stream) -> None:
        """Wire one node-error branch to the configured sink, when present."""
        sink = self.error_sinks.get(kind)
        if sink is not None:
            bw_output(f"{step_id}_{kind.value}_errors", stream, sink)

    def wire_flow_output(self, stream: Stream, plan: CompiledPlan) -> None:
        """Wire the flow-level output and declared error routes."""
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
        """Wire source decode errors to an explicit test sink or runtime route."""
        if self.error_sinks.get(ErrorKind.WIRE) is not None:
            self.wire_node_error(ErrorKind.WIRE, "decode", stream)
            return

        if ErrorKind.WIRE in plan.error_routes:
            raise RuntimeError("Bytewax sink is required for WIRE error routing.")


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
    """Create an AsyncBridge if the plan requires async task execution."""
    if not plan.needs_async_bridge:
        return None

    return AsyncBridge(thread_name=f"loom-{plan.name}-async")


def _decode_source_record(
    payload: Any,
    ctx: _BuildContext,
    codec: MsgspecCodec[Any],
) -> DecodeResult[StreamPayload]:
    """Decode source records into DSL messages without raising decode errors."""
    if isinstance(payload, Message):
        return DecodeOk(message=cast(Message[StreamPayload], payload))
    if isinstance(payload, KafkaRecord):
        return try_decode_record(
            cast(KafkaRecord[bytes], payload),
            ctx.plan.source.payload_type,
            codec,
        )
    raise TypeError(f"Expected Message or KafkaRecord, got {type(payload).__name__}.")


def _is_decode_ok(result: DecodeResult[StreamPayload]) -> bool:
    """Return whether a source decode result can continue through the flow."""
    return isinstance(result, DecodeOk)


def _decode_ok_message(result: DecodeResult[StreamPayload]) -> Message[StreamPayload]:
    """Unwrap a successful source decode result."""
    if isinstance(result, DecodeOk):
        return result.message
    raise TypeError(f"Expected DecodeOk, got {type(result).__name__}.")


def _qualified_step_id(step_id: str, path: tuple[int, ...]) -> str:
    """Return a branch-stable Bytewax step id."""
    if not path:
        return step_id
    suffix = "_".join(str(part) for part in path)
    return f"{step_id}_{suffix}"


def _batch_key(item: Any) -> str:
    """Return the collect key used for batch grouping tests and runtime wiring."""
    return _node_batch_key(item)
