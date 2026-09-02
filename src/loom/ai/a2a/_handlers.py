"""The A2A methods themselves: ``message/send``, ``message/stream``, the rest.

One handler per method, over :class:`~loom.ai.runtime.AgentRuntime`, reusing
the pure event projection of :mod:`loom.ai.a2a.events`. The rules every agent
transport shares — the span that survives a disconnect and the heartbeat race —
come from :mod:`loom.ai._transport` rather than from the HTTP surface's private
names.

``fasta2a``'s ``TypedDict``s alias to camelCase only when routed through
pydantic, so ``Task`` and the streaming events are built here in their already
serialised form — the choice :mod:`loom.ai.a2a.events` already made.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Final
from uuid import uuid4

from starlette.responses import Response, StreamingResponse

from loom.ai._transport import HEARTBEAT_MS, always_closed, failure_event, with_heartbeats
from loom.ai.a2a._binding import PublishedAgent
from loom.ai.a2a._rpc import (
    RpcFault,
    error_response,
    internal_error,
    invalid_params_error,
    rpc_response,
    unsupported_error,
)
from loom.ai.a2a.events import A2AEventProjector
from loom.ai.config import AiConfig
from loom.ai.errors import AgentRunError
from loom.ai.fastapi.response import ENCODER, AgentJSONResponse
from loom.ai.runtime import AgentRuntime
from loom.core.identity import Identity
from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.tracing import get_trace_id

_logger = logging.getLogger(__name__)

_MEDIA_TYPE_SSE: Final[str] = "text/event-stream"

_DATA_PREFIX: Final[bytes] = b"data: "
_FRAME_SUFFIX: Final[bytes] = b"\n\n"

_OUTPUT_ARTIFACT_ID: Final[str] = "output"

_SEND: Final[str] = "message/send"
_STREAM: Final[str] = "message/stream"

# Methods the card explicitly advertises as absent (R-006): no persisted task
# state means no retrieval, no cancellation, no resubscription and no push
# notification configuration. Each answers a named JSON-RPC error, never a 500.
_UNSUPPORTED_METHODS: Final[tuple[str, ...]] = (
    "tasks/get",
    "tasks/list",
    "tasks/cancel",
    "tasks/resubscribe",
    "tasks/pushNotificationConfig/set",
    "tasks/pushNotificationConfig/get",
    "tasks/pushNotificationConfig/list",
    "tasks/pushNotificationConfig/delete",
)


@dataclass(frozen=True, slots=True)
class Call:
    """One decoded JSON-RPC call against one published agent."""

    agent: PublishedAgent
    request_id: int | str | None
    params: Mapping[str, Any] | None
    identity: Identity


Handler = Callable[[Call], Awaitable[Response]]


def _text_of(part: object) -> str | None:
    if not isinstance(part, Mapping) or part.get("kind") != "text":
        return None
    text = part.get("text")
    return text if isinstance(text, str) else None


def _extract_prompt(params: Mapping[str, Any] | None, *, max_prompt_bytes: int) -> str:
    """Read the caller prompt out of the message parts.

    Raises:
        RpcFault: ``-32602`` when no text part is present or the concatenated
            text exceeds ``ai.max_prompt_bytes``.
    """
    message = params.get("message") if params is not None else None
    parts = message.get("parts") if isinstance(message, Mapping) else None
    texts = [text for part in parts or () if (text := _text_of(part)) is not None]
    if not texts:
        raise RpcFault(invalid_params_error("'params.message.parts' must carry a text part"))
    prompt = "".join(texts)
    if len(prompt.encode("utf-8")) > max_prompt_bytes:
        raise RpcFault(
            invalid_params_error(
                f"the prompt exceeds the maximum accepted size ({max_prompt_bytes} bytes)"
            )
        )
    return prompt


def _task(task_id: str, context_id: str, status: Mapping[str, object]) -> Mapping[str, object]:
    """Build a task in its already serialised (camelCase) wire form."""
    return {"id": task_id, "contextId": context_id, "kind": "task", "status": dict(status)}


def _completed_task(task_id: str, context_id: str, output: object) -> Mapping[str, object]:
    task = dict(_task(task_id, context_id, {"state": "completed"}))
    task["artifacts"] = [
        {"artifactId": _OUTPUT_ARTIFACT_ID, "parts": [{"kind": "data", "data": output}]}
    ]
    return task


def _sse_frame(request_id: int | str | None, event: Mapping[str, object]) -> bytes:
    """Encode one streamed A2A event as its own JSON-RPC response frame."""
    return _DATA_PREFIX + ENCODER.encode(rpc_response(request_id, event)) + _FRAME_SUFFIX


def _make_send_handler(
    runtime: AgentRuntime,
    config: AiConfig,
    *,
    prefix: str,
    observability_runtime: ObservabilityRuntime,
) -> Handler:
    """Build the ``message/send`` handler: one run, one terminal task."""

    async def send_message(call: Call) -> Response:
        name = call.agent.name
        prompt = _extract_prompt(call.params, max_prompt_bytes=config.max_prompt_bytes)
        try:
            with observability_runtime.span(
                Scope.AGENT,
                "agent_run",
                trace_id=get_trace_id(),
                route=f"{prefix}/{name}",
                method="POST",
                status_code=200,
                agent=name,
                subject=call.identity.subject,
                mechanism=call.identity.mechanism,
            ):
                result = await runtime.run(name, prompt, identity=call.identity)
        except AgentRunError as exc:
            # The failure text stays server-side: only the code and its fixed
            # catalogue detail travel outward.
            _logger.warning("a2a run of agent %r failed: %s", name, exc)
            return error_response(call.request_id, internal_error(exc.code))
        task = _completed_task(uuid4().hex, uuid4().hex, result.output)
        return AgentJSONResponse(content=rpc_response(call.request_id, task))

    return send_message


def _run_frames(
    runtime: AgentRuntime, call: Call, prompt: str, *, task_id: str, context_id: str
) -> AsyncIterator[bytes]:
    """Project one run onto A2A frames, terminal failure included."""

    async def _frames() -> AsyncIterator[bytes]:
        projector = A2AEventProjector(
            task_id=task_id, context_id=context_id, max_steps=call.agent.max_steps
        )
        yield _sse_frame(call.request_id, _task(task_id, context_id, {"state": "submitted"}))
        try:
            async with runtime.run_stream(
                call.agent.name, prompt, identity=call.identity
            ) as events:
                async for event in events:
                    for projected in projector.project(event):
                        yield _sse_frame(call.request_id, projected)
        except Exception as exc:
            # The status line is long gone, so this failure can only travel
            # in-band, as the stream's terminal event (FR-032).
            _logger.warning("a2a stream failed after the first byte", exc_info=exc)
            for projected in projector.project(failure_event(exc)):
                yield _sse_frame(call.request_id, projected)

    return _frames()


def _make_stream_handler(
    runtime: AgentRuntime,
    config: AiConfig,
    *,
    prefix: str,
    observability_runtime: ObservabilityRuntime,
) -> Handler:
    """Build the ``message/stream`` handler: the run's events, as SSE."""

    async def stream_message(call: Call) -> Response:
        name = call.agent.name
        prompt = _extract_prompt(call.params, max_prompt_bytes=config.max_prompt_bytes)
        task_id, context_id = uuid4().hex, uuid4().hex

        async def _framed() -> AsyncIterator[bytes]:
            with always_closed(
                observability_runtime.open_span(
                    Scope.AGENT,
                    "agent_run",
                    trace_id=get_trace_id(),
                    route=f"{prefix}/{name}",
                    method="POST",
                    agent=name,
                    subject=call.identity.subject,
                    mechanism=call.identity.mechanism,
                )
            ):
                frames = _run_frames(runtime, call, prompt, task_id=task_id, context_id=context_id)
                async for frame in with_heartbeats(frames, heartbeat_ms=HEARTBEAT_MS):
                    yield frame

        return StreamingResponse(_framed(), media_type=_MEDIA_TYPE_SSE)

    return stream_message


def _make_unsupported_handler(method: str) -> Handler:
    """Build the handler answering one advertised-absent method with ``-32004``."""

    async def refuse(call: Call) -> Response:
        return error_response(call.request_id, unsupported_error(method))

    return refuse


def make_handlers(
    runtime: AgentRuntime,
    config: AiConfig,
    *,
    prefix: str,
    observability_runtime: ObservabilityRuntime,
) -> Mapping[str, Handler]:
    """Build the method dispatch table, shared by every published agent."""
    handlers: dict[str, Handler] = {
        method: _make_unsupported_handler(method) for method in _UNSUPPORTED_METHODS
    }
    handlers[_SEND] = _make_send_handler(
        runtime, config, prefix=prefix, observability_runtime=observability_runtime
    )
    handlers[_STREAM] = _make_stream_handler(
        runtime, config, prefix=prefix, observability_runtime=observability_runtime
    )
    return handlers
