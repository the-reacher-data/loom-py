"""Inbound A2A transport: one JSON-RPC endpoint and one card per published agent.

Implements ``specs/001-ai-agent-layer/contracts/a2a.md`` on top of
:class:`~loom.ai.runtime.AgentRuntime`, reusing the pure projections of
:mod:`loom.ai.a2a.card` and :mod:`loom.ai.a2a.events`. Nothing here re-derives
the card or the event mapping: this module is transport only.

**Deviation from ``fasta2a.pydantic_ai.agent_to_a2a``.** The dependency is real
— ``fasta2a.schema`` is the wire contract and the error taxonomy used below —
but its ready-made application is not mounted, for four reasons:

1. ``fasta2a.task_manager.TaskManager.stream_message`` raises
   ``NotImplementedError``, so ``message/stream`` — the method this contract
   requires (FR-039a) — is not served at all.
2. Unimplemented methods surface as HTTP 500, not as the JSON-RPC error the
   contract requires; the card it builds even advertises ``streaming: false``.
3. ``agent_to_a2a`` requires a concrete ``pydantic_ai.Agent`` and drives the run
   through its own worker, storage and broker. That bypasses the caller's
   ``Identity``, ``max_concurrent_runs``, ``run_timeout_ms``, the run span and
   the outward redaction — every guarantee the runtime exists to enforce.
4. Its card is fixed (protocol 0.3.0, hardcoded modes, all capabilities false)
   and derived from engine state, which is exactly what R-005 rejects.

``fasta2a``'s ``TypedDict``s alias to camelCase only when routed through
pydantic, so ``Task`` and the streaming events are built here in their already
serialised form — the choice :mod:`loom.ai.a2a.events` already made — while the
JSON-RPC envelope and the error objects, whose field names *are* the wire
names, are constructed as the ``fasta2a`` types themselves.

**Only the card is anonymous.** Binding reads the exclusions mounted on the
application's authentication middleware and refuses to start when any of them
covers a path under the A2A or the agents prefix other than a card path
(FR-041b); it then registers the card paths as the sole exclusion. Publishing
an agent no caller could ever authenticate against is refused too, and nothing
an external caller receives is derived from an exception's text: a failed run
answers its stable code and a fixed detail, never the failure message.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Final, cast
from uuid import uuid4

import msgspec
from fasta2a.schema import (
    InternalError,
    InvalidParamsError,
    InvalidRequestError,
    JSONParseError,
    JSONRPCError,
    JSONRPCResponse,
    MethodNotFoundError,
    UnsupportedOperationError,
)
from fastapi import FastAPI
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse

from loom.ai.a2a.card import DEFAULT_A2A_PREFIX, build_agent_card, card_path
from loom.ai.a2a.events import A2AEventProjector
from loom.ai.abc import ErrorEvent
from loom.ai.compiler import AgentPlan
from loom.ai.config import AgentEndpointConfig, AiConfig
from loom.ai.errors import (
    AgentCompilationError,
    AgentRunErrorCode,
    auth_exclusion_overlaps_agents,
)
from loom.ai.fastapi.endpoints import (
    _BODY_OVERHEAD_BYTES,
    _HEARTBEAT_MS,
    _AgentHttpError,
    _always_closed,
    _read_body_capped,
    _require_caller,
)
from loom.ai.fastapi.response import ENCODER, AgentJSONResponse
from loom.ai.fastapi.streaming import HEARTBEAT_FRAME
from loom.ai.runtime import AgentRunError, AgentRuntime
from loom.core.config.errors import ConfigError
from loom.core.identity import Identity
from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.tracing import get_trace_id
from loom.rest.auth.abc import Authenticator
from loom.rest.auth.middleware import AuthenticationMiddleware

_logger = logging.getLogger(__name__)

_JSONRPC_VERSION: Final = "2.0"
_MEDIA_TYPE_JSON: Final[str] = "application/json"
_MEDIA_TYPE_SSE: Final[str] = "text/event-stream"

_DATA_PREFIX: Final[bytes] = b"data: "
_FRAME_SUFFIX: Final[bytes] = b"\n\n"

_OUTPUT_ARTIFACT_ID: Final[str] = "output"

# Default prefix :func:`loom.ai.fastapi.endpoints.bind_agent_endpoints` mounts
# under. Guarded here because an exclusion written against it would open the
# HTTP invocation surface just as widely as one written against the A2A prefix.
# A deployment mounting the agents elsewhere passes its own ``agents_prefix``.
_DEFAULT_AGENTS_PREFIX: Final[str] = "/agents"

# Everything an external caller is ever told about a failed run, keyed by the
# stable code. Nothing here is derived from ``AgentRunError.message``: that
# text names capability keys, SQL connections, remote agent hosts and model
# bindings — exactly what the card and the stream redact (FR-030a, FR-038).
_RUN_ERROR_DETAILS: Final[Mapping[AgentRunErrorCode, str]] = MappingProxyType(
    {
        AgentRunErrorCode.PROVIDER_UNAVAILABLE: "the model provider is unavailable",
        AgentRunErrorCode.PROVIDER_RATE_LIMITED: "the model provider rate limited this run",
        AgentRunErrorCode.TOOL_TIMEOUT: "a capability call exceeded its time limit",
        AgentRunErrorCode.TOOL_UNAVAILABLE: "a capability is unavailable",
        AgentRunErrorCode.OUTPUT_SCHEMA_VIOLATION: "the run produced an invalid output",
        AgentRunErrorCode.MAX_ITERATIONS_EXCEEDED: "the run exceeded its step limit",
        AgentRunErrorCode.RUN_TIMEOUT: "the run exceeded its time limit",
        AgentRunErrorCode.TOO_MANY_RUNS: "the agent is at its concurrency limit",
        AgentRunErrorCode.UNAUTHORIZED: "the caller is not permitted to perform this run",
        AgentRunErrorCode.CANCELLED: "the run was cancelled",
    }
)

# Detail of a failure with no catalogue entry, including the catch-all. Mirrors
# the wording of the HTTP surface's own unexpected-failure body.
_UNEXPECTED_DETAIL: Final[str] = "An unexpected error occurred"

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
class _PublishedAgent:
    """One agent published over A2A, with everything the routes need precomputed."""

    name: str
    card: bytes
    max_steps: int
    endpoint: AgentEndpointConfig | None


@dataclass(frozen=True, slots=True)
class _Call:
    """One decoded JSON-RPC call against one published agent."""

    agent: _PublishedAgent
    request_id: int | str | None
    params: Mapping[str, Any] | None
    identity: Identity


_Handler = Callable[[_Call], Awaitable[Response]]


class _RpcEnvelope(msgspec.Struct, frozen=True, kw_only=True):
    """Permissive view of a JSON-RPC request, so a malformed one still echoes its id."""

    jsonrpc: str | None = None
    id: int | str | None = None
    method: str | None = None
    params: dict[str, Any] | None = None


_ENVELOPE_DECODER = msgspec.json.Decoder(_RpcEnvelope)


class _RpcFault(Exception):
    """A failure already shaped as the JSON-RPC error it answers with."""

    def __init__(self, error: JSONRPCError[Any, Any]) -> None:
        super().__init__(str(error["message"]))
        self.error = error


def _rpc_response(request_id: int | str | None, result: object) -> JSONRPCResponse[Any, Any]:
    return JSONRPCResponse(jsonrpc=_JSONRPC_VERSION, id=request_id, result=result)


def _rpc_error(
    request_id: int | str | None, error: JSONRPCError[Any, Any]
) -> JSONRPCResponse[Any, Any]:
    return JSONRPCResponse(jsonrpc=_JSONRPC_VERSION, id=request_id, error=error)


def _error_response(request_id: int | str | None, error: JSONRPCError[Any, Any]) -> Response:
    """Answer a JSON-RPC failure: HTTP 200 carrying the error object."""
    return AgentJSONResponse(content=_rpc_error(request_id, error))


def _unsupported_error(method: str) -> UnsupportedOperationError:
    return UnsupportedOperationError(
        code=-32004,
        message="This operation is not supported",
        data={
            "method": method,
            "reason": "no task state is persisted, as the agent card advertises",
        },
    )


def _method_not_found_error(method: str) -> MethodNotFoundError:
    return MethodNotFoundError(code=-32601, message="Method not found", data={"method": method})


def _invalid_params_error(reason: str) -> InvalidParamsError:
    return InvalidParamsError(code=-32602, message="Invalid parameters", data={"reason": reason})


def _invalid_request_error(reason: str) -> InvalidRequestError:
    return InvalidRequestError(
        code=-32600, message="Request payload validation error", data={"reason": reason}
    )


def _parse_error() -> JSONParseError:
    return JSONParseError(code=-32700, message="Invalid JSON payload")


def _internal_error(code: AgentRunErrorCode) -> InternalError:
    """Build the outward failure of one run: a stable code and a fixed detail."""
    return InternalError(
        code=-32603,
        message="Internal error",
        data={"code": str(code), "detail": _RUN_ERROR_DETAILS.get(code, _UNEXPECTED_DETAIL)},
    )


def _unexpected_error() -> InternalError:
    """Build the failure answering anything this endpoint did not anticipate."""
    return InternalError(
        code=-32603,
        message="Internal error",
        data={"code": str(AgentRunErrorCode.PROVIDER_UNAVAILABLE), "detail": _UNEXPECTED_DETAIL},
    )


def _decode_envelope(body: bytes) -> _RpcEnvelope:
    """Decode the JSON-RPC envelope.

    Raises:
        _RpcFault: ``-32700`` when the body is not JSON, ``-32600`` when it is
            JSON of an unusable shape.
    """
    try:
        return _ENVELOPE_DECODER.decode(body)
    except msgspec.ValidationError as exc:
        raise _RpcFault(_invalid_request_error(str(exc))) from exc
    except msgspec.DecodeError as exc:
        raise _RpcFault(_parse_error()) from exc


def _require_method(envelope: _RpcEnvelope) -> str:
    """Return the requested method, refusing anything that is not JSON-RPC 2.0.

    Validated after the id has been read, so the refusal still echoes it.

    Raises:
        _RpcFault: ``-32600`` when the envelope is not a JSON-RPC 2.0 request.
    """
    if envelope.jsonrpc != _JSONRPC_VERSION or not envelope.method:
        raise _RpcFault(_invalid_request_error("'jsonrpc' must be '2.0' and 'method' is required"))
    return envelope.method


def _text_of(part: object) -> str | None:
    if not isinstance(part, Mapping) or part.get("kind") != "text":
        return None
    text = part.get("text")
    return text if isinstance(text, str) else None


def _extract_prompt(params: Mapping[str, Any] | None, *, max_prompt_bytes: int) -> str:
    """Read the caller prompt out of the message parts.

    Raises:
        _RpcFault: ``-32602`` when no text part is present or the concatenated
            text exceeds ``ai.max_prompt_bytes``.
    """
    message = params.get("message") if params is not None else None
    parts = message.get("parts") if isinstance(message, Mapping) else None
    texts = [text for part in parts or () if (text := _text_of(part)) is not None]
    if not texts:
        raise _RpcFault(_invalid_params_error("'params.message.parts' must carry a text part"))
    prompt = "".join(texts)
    if len(prompt.encode("utf-8")) > max_prompt_bytes:
        raise _RpcFault(
            _invalid_params_error(
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
    return _DATA_PREFIX + ENCODER.encode(_rpc_response(request_id, event)) + _FRAME_SUFFIX


def _failure_event(exc: BaseException) -> ErrorEvent:
    """Turn a post-first-byte failure into the stream's terminal error event."""
    if isinstance(exc, AgentRunError):
        return ErrorEvent(code=exc.code, message=str(exc))
    return ErrorEvent(
        code=AgentRunErrorCode.PROVIDER_UNAVAILABLE, message="the agent run failed unexpectedly"
    )


async def _next_frame(frames: AsyncIterator[bytes]) -> bytes | None:
    try:
        return await anext(frames)
    except StopAsyncIteration:
        return None


async def _drain(pending: asyncio.Future[bytes | None] | None) -> None:
    """Cancel the awaited frame and wait for it, so nothing outlives the stream."""
    if pending is None:
        return
    pending.cancel()
    try:
        await pending
    except (Exception, asyncio.CancelledError):
        # The consumer is already gone: neither the cancellation just requested
        # nor a late failure has anybody left to be reported to.
        return


async def _with_heartbeats(
    frames: AsyncIterator[bytes], *, heartbeat_ms: int
) -> AsyncIterator[bytes]:
    """Emit a comment frame for every silence longer than *heartbeat_ms*.

    Mirrors the race in :func:`loom.ai.fastapi.streaming.stream_sse` — which
    cannot be reused because it is bound to the HTTP frame encoding, while an
    A2A frame is a JSON-RPC envelope and one agent event may project to two of
    them. The awaited frame is shielded from the timeout and drained on exit,
    so a disconnected client leaves no run burning tokens (FR-033).

    Args:
        frames: Frames to relay, in order; exhaustion ends the stream.
        heartbeat_ms: Silence after which a comment frame is emitted.

    Yields:
        The relayed frames, interleaved with heartbeat comment frames.
    """
    iterator = frames.__aiter__()
    beat = heartbeat_ms / 1000
    pending: asyncio.Future[bytes | None] | None = None
    try:
        while True:
            if pending is None:
                pending = asyncio.ensure_future(_next_frame(iterator))
            try:
                async with asyncio.timeout(beat):
                    frame = await asyncio.shield(pending)
            except TimeoutError:
                yield HEARTBEAT_FRAME
                continue
            pending = None
            if frame is None:
                return
            yield frame
    finally:
        await _drain(pending)


def _make_send_handler(
    runtime: AgentRuntime,
    config: AiConfig,
    *,
    prefix: str,
    observability_runtime: ObservabilityRuntime,
) -> _Handler:
    """Build the ``message/send`` handler: one run, one terminal task."""

    async def send_message(call: _Call) -> Response:
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
            return _error_response(call.request_id, _internal_error(exc.code))
        task = _completed_task(uuid4().hex, uuid4().hex, result.output)
        return AgentJSONResponse(content=_rpc_response(call.request_id, task))

    return send_message


def _run_frames(
    runtime: AgentRuntime, call: _Call, prompt: str, *, task_id: str, context_id: str
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
            for projected in projector.project(_failure_event(exc)):
                yield _sse_frame(call.request_id, projected)

    return _frames()


def _make_stream_handler(
    runtime: AgentRuntime,
    config: AiConfig,
    *,
    prefix: str,
    observability_runtime: ObservabilityRuntime,
) -> _Handler:
    """Build the ``message/stream`` handler: the run's events, as SSE."""

    async def stream_message(call: _Call) -> Response:
        name = call.agent.name
        prompt = _extract_prompt(call.params, max_prompt_bytes=config.max_prompt_bytes)
        task_id, context_id = uuid4().hex, uuid4().hex

        async def _framed() -> AsyncIterator[bytes]:
            with _always_closed(
                observability_runtime.span(
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
                async for frame in _with_heartbeats(frames, heartbeat_ms=_HEARTBEAT_MS):
                    yield frame

        return StreamingResponse(_framed(), media_type=_MEDIA_TYPE_SSE)

    return stream_message


def _make_unsupported_handler(method: str) -> _Handler:
    """Build the handler answering one advertised-absent method with ``-32004``."""

    async def refuse(call: _Call) -> Response:
        return _error_response(call.request_id, _unsupported_error(method))

    return refuse


def _make_handlers(
    runtime: AgentRuntime,
    config: AiConfig,
    *,
    prefix: str,
    observability_runtime: ObservabilityRuntime,
) -> Mapping[str, _Handler]:
    """Build the method dispatch table, shared by every published agent."""
    handlers: dict[str, _Handler] = {
        method: _make_unsupported_handler(method) for method in _UNSUPPORTED_METHODS
    }
    handlers[_SEND] = _make_send_handler(
        runtime, config, prefix=prefix, observability_runtime=observability_runtime
    )
    handlers[_STREAM] = _make_stream_handler(
        runtime, config, prefix=prefix, observability_runtime=observability_runtime
    )
    return handlers


def _make_rpc_handler(
    agent: _PublishedAgent, handlers: Mapping[str, _Handler], *, max_prompt_bytes: int
) -> Callable[[Request], Awaitable[Response]]:
    """Build the JSON-RPC endpoint of one published agent."""
    body_cap = max_prompt_bytes + _BODY_OVERHEAD_BYTES

    async def serve_rpc(request: Request) -> Response:
        request_id: int | str | None = None
        try:
            identity = _require_caller(agent.name, agent.endpoint)
            body = await _read_body_capped(request, max_bytes=body_cap)
            envelope = _decode_envelope(body)
            request_id = envelope.id
            method = _require_method(envelope)
            handler = handlers.get(method)
            if handler is None:
                return _error_response(request_id, _method_not_found_error(method))
            return await handler(_Call(agent, request_id, envelope.params, identity))
        except _AgentHttpError as exc:
            return exc.response()
        except _RpcFault as exc:
            return _error_response(request_id, exc.error)
        except Exception:
            # An unanticipated failure carries file paths, DSNs and credential
            # references in its text; the caller gets none of it.
            _logger.exception("Unhandled error in the a2a endpoint of agent %r", agent.name)
            return _error_response(request_id, _unexpected_error())

    return serve_rpc


def _make_card_handler(card: bytes) -> Callable[[], Awaitable[Response]]:
    """Build the handler serving one agent's card, encoded once at bind time."""

    async def serve_card() -> Response:
        return Response(content=card, media_type=_MEDIA_TYPE_JSON)

    return serve_card


def _published_agents(
    *,
    config: AiConfig,
    plans: Mapping[str, AgentPlan],
    runtime: AgentRuntime,
    mechanism: str | None,
    prefix: str,
) -> tuple[_PublishedAgent, ...]:
    """Select the exposed agents that really exist, warning about the ones that do not.

    ``expose`` is authoritative and never empty (``A2A_EXPOSE_EMPTY``): an empty
    list means no agent, never all of them (FR-041a).
    """
    a2a = config.a2a
    assert a2a is not None  # noqa: S101 - guarded by the caller's early return
    published: list[_PublishedAgent] = []
    for name in a2a.expose:
        plan = plans.get(name)
        if plan is None or not runtime.has_agent(name):
            _logger.warning(
                "Agent %s is listed in 'ai.a2a.expose' but no compiled agent has that name: "
                "it is not published over A2A",
                name,
            )
            continue
        card = ENCODER.encode(build_agent_card(plan, a2a, mechanism=mechanism, prefix=prefix))
        published.append(
            _PublishedAgent(
                name=name,
                card=card,
                max_steps=plan.policies.max_iterations,
                endpoint=_active_endpoint(config.endpoints.get(name)),
            )
        )
    return tuple(published)


def _active_endpoint(endpoint: AgentEndpointConfig | None) -> AgentEndpointConfig | None:
    """Return the HTTP opt-in only when it is actually in force.

    ``allow_anonymous`` lives in the HTTP stanza, whose mount is a double
    opt-in — ``enabled`` **and** a named ``auth`` (FR-029a). A stanza failing
    either grants nothing, so A2A must not inherit its anonymity opt-out:
    otherwise switching the HTTP surface off while leaving
    ``allow_anonymous: true`` behind would keep anonymous invocation alive on
    the one surface published to the internet.
    """
    if endpoint is None or not endpoint.enabled or not endpoint.auth.strip():
        return None
    return endpoint


def _allows_anonymous(agent: _PublishedAgent) -> bool:
    """Report whether this agent accepts an unverified external caller."""
    return agent.endpoint is not None and agent.endpoint.allow_anonymous


def _require_usable_authenticator(
    agent: _PublishedAgent, authenticator: Authenticator | None
) -> None:
    """Refuse to publish an agent whose invocation route could never be used.

    Mirrors :func:`loom.ai.fastapi.endpoints._require_usable_authenticator`:
    without an authenticator every invocation answers 401 forever, which is a
    dead route advertised by a live card, not a security posture.

    Raises:
        ConfigError: When no authenticator can verify callers and the agent
            declares no explicit ``allow_anonymous`` opt-out.
    """
    if authenticator is not None or _allows_anonymous(agent):
        return
    raise ConfigError(
        f"Agent {agent.name!r} is published in 'ai.a2a.expose' but the application configures "
        "no authentication, so every A2A invocation would be refused. Add the "
        "'app.rest.auth.jwt' section, pass create_app(authenticator=...), or opt into "
        f"anonymous callers with 'ai.endpoints.{agent.name}' enabled, its 'auth' named and "
        "'allow_anonymous: true'."
    )


def _verify_exclusions(
    exclude_paths: Sequence[str], card_paths: frozenset[str], *, prefix: str, agents_prefix: str
) -> None:
    """Refuse to start when an exclusion opens an invocation path (FR-041b).

    Exclusions are matched as exact strings by
    :class:`~loom.rest.auth.middleware.AuthenticationMiddleware`, so a
    hand-written ``/a2a`` would not merely be useless — combined with a router
    that serves ``/a2a`` itself it opens the whole invocation surface.

    Raises:
        AgentCompilationError: With ``AUTH_EXCLUSION_OVERLAPS_AGENTS``, naming
            every offending exclusion at once.
    """
    guarded = (prefix, agents_prefix)
    offending = [
        path
        # Deduplicated in order: the mounted list and the argument usually
        # repeat each other, and the failure must name each path once.
        for path in dict.fromkeys(exclude_paths)
        if path not in card_paths
        and any(path == root or path.startswith(f"{root}/") for root in guarded)
    ]
    if offending:
        raise AgentCompilationError([auth_exclusion_overlaps_agents(offending)])


def _is_auth_middleware(entry: Middleware) -> bool:
    """Report whether one recorded middleware entry is the authentication one.

    ``Middleware.cls`` is typed as a generic ASGI factory protocol, so the
    identity test needs the cast to be expressible at all.
    """
    return cast(object, entry.cls) is AuthenticationMiddleware


def _auth_middleware_entries(app: FastAPI) -> list[Middleware]:
    return [entry for entry in app.user_middleware if _is_auth_middleware(entry)]


def _declared_exclusions(entry: Middleware) -> tuple[str, ...]:
    """Read one middleware entry's exclusion list.

    Raises:
        ConfigError: When the recorded ``exclude_paths`` argument is not a list
            of paths, since neither the guard nor the card registration could
            then be trusted.
    """
    declared = entry.kwargs.get("exclude_paths", ())
    if not isinstance(declared, Sequence) or isinstance(declared, str):
        raise ConfigError(
            "The mounted 'AuthenticationMiddleware' declares an unrecognisable "
            "'exclude_paths' argument, so the A2A agent card paths cannot be excluded "
            "from authentication."
        )
    return tuple(str(path) for path in declared)


def _mounted_exclusions(app: FastAPI) -> tuple[str, ...]:
    """Return the authentication exclusions the deployment actually runs with.

    The authoritative list is the one mounted on the middleware, not the one
    the caller of :func:`bind_a2a_endpoints` chose to repeat in its argument —
    which defaults to empty. Reading it here is what makes the FR-041b guard
    fail closed.
    """
    return tuple(
        path for entry in _auth_middleware_entries(app) for path in _declared_exclusions(entry)
    )


def _register_card_exclusions(app: FastAPI, card_paths: Sequence[str]) -> None:
    """Add the card paths to the authentication middleware's exclusion list.

    The middleware is mounted by ``create_app`` *before* the routes exist, so
    the exclusion cannot be passed at construction time; the ASGI stack is not
    built yet, which is what makes extending the recorded keyword argument
    sound. Doing this here — rather than in ``create_app`` — keeps the card the
    *only* path this feature can ever open.

    Args:
        app: Application whose authentication middleware is amended.
        card_paths: Paths to serve without credentials.

    Raises:
        ConfigError: When the ASGI stack is already built, or when no
            recognisable :class:`AuthenticationMiddleware` entry is mounted
            even though the deployment configured an authenticator.
    """
    if app.middleware_stack is not None:
        raise ConfigError(
            "The A2A surface must be bound before the ASGI middleware stack is built: "
            "the agent card paths can no longer be excluded from authentication."
        )
    entries = _auth_middleware_entries(app)
    if not entries:
        raise ConfigError(
            "The application configures an authenticator but no 'AuthenticationMiddleware' is "
            "mounted, so the A2A agent card paths cannot be excluded from authentication. "
            "Mount the middleware through create_app() before binding the A2A endpoints."
        )
    for entry in entries:
        entry.kwargs["exclude_paths"] = (*_declared_exclusions(entry), *card_paths)


def _announce_publication(
    agent: _PublishedAgent, *, base_url: str, mechanism: str | None, prefix: str
) -> None:
    """Emit the startup WARNING naming exactly what is now reachable from outside."""
    allow_anonymous = _allows_anonymous(agent)
    _logger.warning(
        "A2A agent published to the public internet: url=%s%s/%s card=%s agent=%s auth=%s "
        "allow_anonymous=%s. %s",
        base_url.rstrip("/"),
        prefix,
        agent.name,
        card_path(agent.name, prefix=prefix),
        agent.name,
        mechanism or "none",
        allow_anonymous,
        _identity_notice(allow_anonymous),
    )


def _identity_notice(allow_anonymous: bool) -> str:
    """State plainly which identity an external A2A caller's run executes as."""
    if allow_anonymous:
        return (
            "allow_anonymous is set, so external callers are NOT authenticated: every run "
            "spends model tokens on behalf of an unidentified stranger, and only "
            "'max_concurrent_runs' and 'run_timeout_ms' bound that cost"
        )
    return (
        "the card is served anonymously; every invocation requires a verified caller, and "
        "each capability call then runs as that identity"
    )


def bind_a2a_endpoints(
    app: FastAPI,
    *,
    runtime: AgentRuntime,
    config: AiConfig,
    plans: Sequence[AgentPlan],
    authenticator: Authenticator | None = None,
    exclude_paths: Sequence[str] = (),
    observability_runtime: ObservabilityRuntime | None = None,
    prefix: str = DEFAULT_A2A_PREFIX,
    agents_prefix: str = _DEFAULT_AGENTS_PREFIX,
) -> None:
    """Publish the agents named in ``ai.a2a.expose`` over A2A.

    With no ``ai.a2a`` section nothing is mounted and nothing is logged
    (FR-041). Each published agent gets one anonymous card route and one
    authenticated JSON-RPC route serving ``message/send`` and ``message/stream``;
    every other A2A method answers a named JSON-RPC error, as its card says.

    Args:
        app: FastAPI application to mount the routes on.
        runtime: Entered runtime serving the agents.
        config: Parsed ``ai:`` section.
        plans: Compiled plans the runtime was built from; the card is projected
            from them, which is why they are passed explicitly instead of read
            back out of the runtime.
        authenticator: Mechanism authenticating callers; its name selects the
            security scheme the card advertises.
        exclude_paths: Extra authentication exclusions to validate against the
            invocation surface (FR-041b). The exclusions mounted on the
            application's :class:`AuthenticationMiddleware` are read directly
            and always validated; this argument only adds to them.
        observability_runtime: Runtime emitting one span per run.
        prefix: Path prefix the A2A surface is mounted under.
        agents_prefix: Path prefix the HTTP agent surface is mounted under,
            guarded by the same exclusion check.

    Raises:
        AgentCompilationError: With ``AUTH_EXCLUSION_OVERLAPS_AGENTS`` when an
            exclusion covers an invocation path.
        ConfigError: When an agent is published without a usable authenticator
            and without an explicit anonymous opt-out, or when the card paths
            cannot be excluded from authentication.

    Example::

        bind_a2a_endpoints(app, runtime=runtime, config=ai_config, plans=plans,
                           authenticator=authenticator, exclude_paths=("/health",))
    """
    if config.a2a is None:
        return
    mechanism = authenticator.name if authenticator is not None else None
    agents = _published_agents(
        config=config,
        plans={plan.name: plan for plan in plans},
        runtime=runtime,
        mechanism=mechanism,
        prefix=prefix,
    )
    if not agents:
        return
    for agent in agents:
        _require_usable_authenticator(agent, authenticator)
    cards = [card_path(agent.name, prefix=prefix) for agent in agents]
    _verify_exclusions(
        (*exclude_paths, *_mounted_exclusions(app)),
        frozenset(cards),
        prefix=prefix,
        agents_prefix=agents_prefix,
    )
    if authenticator is not None:
        _register_card_exclusions(app, cards)
    handlers = _make_handlers(
        runtime,
        config,
        prefix=prefix,
        observability_runtime=observability_runtime or ObservabilityRuntime.noop(),
    )
    for agent, path in zip(agents, cards, strict=True):
        app.add_api_route(path, _make_card_handler(agent.card), methods=["GET"])
        app.add_api_route(
            f"{prefix}/{agent.name}",
            _make_rpc_handler(agent, handlers, max_prompt_bytes=config.max_prompt_bytes),
            methods=["POST"],
        )
        _announce_publication(
            agent, base_url=config.a2a.base_url, mechanism=mechanism, prefix=prefix
        )
