"""Bytewax handler family for shape nodes."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from bytewax.operators import collect, flat_map, key_on, key_rm
from bytewax.operators import map as bw_map

from loom.streaming.bytewax.handlers._shared import (
    _BuildContextProtocol,
    _empty,
    _identity,
    _is_message,
    _step_id,
)
from loom.streaming.nodes._shape import CollectBatch, WindowStrategy

Stream = Any


def _batch_key(item: Any) -> str:
    """Return a grouping key for Bytewax ``collect``."""
    if not _is_message(item):
        return "loom"
    meta = item.meta
    if meta.partition is not None:
        return f"{meta.topic or 'default'}:{meta.partition}"
    if meta.key is not None:
        raw_key = meta.key
        return raw_key if isinstance(raw_key, str) else raw_key.decode("utf-8", errors="replace")
    return "loom"


def _collect_batch_reason(batch: list[Any], max_records: int) -> str:
    """Best-effort reason label for a collected batch."""
    if len(batch) >= max_records:
        return "size"
    return "timeout_or_flush"


def _apply_collect_batch(
    stream: Stream,
    raw: object,
    idx: int,
    ctx: _BuildContextProtocol,
) -> Stream:
    if not isinstance(raw, CollectBatch):
        raise TypeError(f"Unsupported collect-batch node {type(raw).__name__}.")
    node = raw
    if node.window is WindowStrategy.COLLECT:
        return _apply_collect_batch_default(
            stream,
            node,
            _step_id(str(idx), ctx),
            observer=ctx.flow_observer,
            flow_name=ctx.plan.name,
            idx=idx,
        )
    raise TypeError(
        f"WindowStrategy.{node.window} reached the adapter — "
        "this should have been rejected at compile time."
    )


def _apply_collect_batch_default(
    stream: Stream,
    node: CollectBatch,
    step_prefix: str,
    *,
    observer: Any = None,
    flow_name: str = "",
    idx: int = 0,
) -> Stream:
    """Apply processing-time count-and-timeout collect (``WindowStrategy.COLLECT``)."""
    keyed = key_on(f"collect_key_{step_prefix}", stream, _batch_key)
    collected = collect(
        f"collect_{step_prefix}",
        keyed,
        timeout=timedelta(milliseconds=node.timeout_ms),
        max_size=node.max_records,
    )

    def observe(item: tuple[str, list[Any]]) -> tuple[str, list[Any]]:
        key, batch = item
        if observer is not None:
            observer.on_collect_batch(
                flow_name,
                idx,
                node_type="CollectBatch",
                batch_size=len(batch),
                max_records=node.max_records,
                timeout_ms=node.timeout_ms,
                reason=_collect_batch_reason(batch, node.max_records),
            )
        return key, batch

    observed = bw_map(f"collect_observe_{step_prefix}", collected, observe)
    return key_rm(f"collect_unkey_{step_prefix}", observed)


def _apply_for_each(stream: Stream, _raw: object, idx: int, ctx: _BuildContextProtocol) -> Stream:
    return flat_map(_step_id(f"foreach_{idx}", ctx), stream, _identity)


def _apply_drain(stream: Stream, _raw: object, idx: int, ctx: _BuildContextProtocol) -> Stream:
    tracker = ctx.commit_tracker
    if tracker is None:
        return flat_map(_step_id(f"drain_{idx}", ctx), stream, _empty)

    def drop_and_commit(item: Any) -> tuple[()]:
        from loom.streaming.bytewax.handlers._shared import _drop_and_commit

        return _drop_and_commit(item, tracker)

    return flat_map(_step_id(f"drain_{idx}", ctx), stream, drop_and_commit)
