"""Bytewax handler family for step-shaped nodes."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from bytewax.operators import map as bw_map

from loom.core.observability.runtime import ObservabilityRuntime
from loom.streaming.bytewax._error_boundary import (
    NodeResult,
    _classify_task,
    _execute_batch_in_boundary,
    _execute_in_boundary,
    _split_batch_node_result,
    _split_node_result,
)
from loom.streaming.bytewax.handlers._shared import (
    _BuildContextProtocol,
    _ExecutableBatchExpandStep,
    _ExecutableBatchStep,
    _ExecutableExpandStep,
    _ExecutableRecordStep,
    _identity,
    _observe_node,
    _replace_payload,
    _replace_payloads,
    _require_message,
    _resolve_batch_result,
    _resolve_expand_result,
    _resolve_node_name,
    _resolve_record_result,
    _step_id,
)
from loom.streaming.core._errors import ErrorKind
from loom.streaming.core._exceptions import UnsupportedNodeError
from loom.streaming.core._message import Message
from loom.streaming.core._typing import StreamPayload

Stream = Any


def _apply_record_step(stream: Stream, raw: object, idx: int, ctx: _BuildContextProtocol) -> Stream:
    if not isinstance(raw, _ExecutableRecordStep):
        raise UnsupportedNodeError(f"Unsupported record step {type(raw).__name__}.")
    record_step = raw
    name = _resolve_node_name(record_step)
    observer = ctx.flow_runtime
    flow_name = ctx.plan.name

    def step_fn(msg: Any) -> NodeResult:
        message = _require_message(msg)
        return _execute_in_boundary(
            _classify_task,
            message,
            lambda: _execute_record_step(observer, flow_name, idx, name, record_step, message),
        )

    sid = _step_id(f"record_{idx}_{name}", ctx)
    mapped = bw_map(sid, stream, step_fn)
    return _split_node_result(mapped, sid, ctx, ErrorKind.TASK)


def _apply_batch_step(stream: Stream, raw: object, idx: int, ctx: _BuildContextProtocol) -> Stream:
    if not isinstance(raw, _ExecutableBatchStep):
        raise UnsupportedNodeError(f"Unsupported batch step {type(raw).__name__}.")
    batch_step = raw
    name = _resolve_node_name(batch_step)
    observer = ctx.flow_runtime
    flow_name = ctx.plan.name

    def step_fn(batch: list[Any]) -> list[NodeResult]:
        messages = [_require_message(item) for item in batch]
        return _execute_batch_in_boundary(
            _classify_task,
            messages,
            lambda: _execute_batch_step(
                observer,
                flow_name,
                idx,
                name,
                batch_step,
                messages,
            ),
        )

    sid = _step_id(f"batch_{idx}_{name}", ctx)
    mapped = bw_map(sid, stream, step_fn)
    return _split_batch_node_result(mapped, sid, ctx, ErrorKind.TASK)


def _apply_expand_step(
    stream: Stream,
    raw: object,
    idx: int,
    ctx: _BuildContextProtocol,
) -> Stream:
    if not isinstance(raw, _ExecutableExpandStep):
        raise UnsupportedNodeError(f"Unsupported expand step {type(raw).__name__}.")
    expand_step = raw
    name = _resolve_node_name(expand_step)
    observer = ctx.flow_runtime
    flow_name = ctx.plan.name

    def step_fn(msg: Any) -> list[NodeResult]:
        message = _require_message(msg)
        return _execute_batch_in_boundary(
            _classify_task,
            [message],
            lambda: _execute_expand_step(
                observer,
                flow_name,
                idx,
                name,
                expand_step,
                message,
            ),
        )

    sid = _step_id(f"expand_{idx}_{name}", ctx)
    mapped = bw_map(sid, stream, step_fn)
    from bytewax.operators import flat_map

    flattened_stream = flat_map(_step_id(f"flatten_expand_{idx}", ctx), mapped, _identity)
    return _split_node_result(flattened_stream, sid, ctx, ErrorKind.TASK)


def _apply_batch_expand_step(
    stream: Stream,
    raw: object,
    idx: int,
    ctx: _BuildContextProtocol,
) -> Stream:
    if not isinstance(raw, _ExecutableBatchExpandStep):
        raise UnsupportedNodeError(f"Unsupported batch-expand step {type(raw).__name__}.")
    batch_expand_step = raw
    name = _resolve_node_name(batch_expand_step)
    observer = ctx.flow_runtime
    flow_name = ctx.plan.name

    def step_fn(batch: list[Any]) -> list[NodeResult]:
        messages = [_require_message(item) for item in batch]
        return _execute_batch_in_boundary(
            _classify_task,
            messages,
            lambda: _execute_batch_expand_step(
                observer,
                flow_name,
                idx,
                name,
                batch_expand_step,
                messages,
            ),
        )

    sid = _step_id(f"batch_expand_{idx}_{name}", ctx)
    mapped = bw_map(sid, stream, step_fn)
    from bytewax.operators import flat_map

    flattened = flat_map(_step_id(f"flatten_batch_expand_{idx}", ctx), mapped, _identity)
    return _split_node_result(flattened, sid, ctx, ErrorKind.TASK)


def _execute_record_step(
    observer: ObservabilityRuntime,
    flow_name: str,
    idx: int,
    name: str,
    record_step: _ExecutableRecordStep,
    message: Message[StreamPayload],
) -> Message[StreamPayload]:
    with _observe_node(observer, flow_name, idx, name):
        result = _resolve_record_result(record_step.execute(message), name)
        return _replace_payload(message, result)


def _execute_batch_step(
    observer: ObservabilityRuntime,
    flow_name: str,
    idx: int,
    name: str,
    batch_step: _ExecutableBatchStep,
    messages: list[Message[StreamPayload]],
) -> list[Message[StreamPayload]]:
    with _observe_node(observer, flow_name, idx, name):
        result = _resolve_batch_result(batch_step.execute(messages), name)
        if not isinstance(result, list):
            raise TypeError(f"{name} must return a list of payloads.")
        return _replace_payloads(messages, result)


def _execute_expand_step(
    observer: ObservabilityRuntime,
    flow_name: str,
    idx: int,
    name: str,
    expand_step: _ExecutableExpandStep,
    message: Message[StreamPayload],
) -> list[Message[StreamPayload]]:
    with _observe_node(observer, flow_name, idx, name):
        result = _resolve_expand_result(expand_step.execute(message), name)
        if not isinstance(result, Iterable):
            raise TypeError(f"{name} must return an iterable of payloads.")
        return [_replace_payload(message, payload) for payload in result]


def _execute_batch_expand_step(
    observer: ObservabilityRuntime,
    flow_name: str,
    idx: int,
    name: str,
    batch_expand_step: _ExecutableBatchExpandStep,
    messages: list[Message[StreamPayload]],
) -> list[Message[StreamPayload]]:
    with _observe_node(observer, flow_name, idx, name):
        result = _resolve_expand_result(batch_expand_step.execute(messages), name)
        if not isinstance(result, Iterable):
            raise TypeError(f"{name} must return an iterable of payloads.")
        return _replace_payloads(messages, list(result))
