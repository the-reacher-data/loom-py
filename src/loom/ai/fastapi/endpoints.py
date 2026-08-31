"""HTTP surface of the agent runtime: ``/run``, ``/stream`` and ``/health``.

Mirrors :func:`loom.rest.fastapi.sql.bind_sql_endpoints`. Mounting is a double
opt-in — an agent needs ``enabled`` **and** a named ``auth`` in ``ai.endpoints``
— every mount is announced with a WARNING carrying its security state, and an
agent that opts in without a usable authenticator aborts start-up instead of
serving anonymously by accident.

Two request-path rules come straight from the contract
(``specs/001-ai-agent-layer/contracts/http-sse.md``):

* Authentication is evaluated **before** existence, so an anonymous probe for
  an unknown agent gets ``401`` and the surface cannot be used to enumerate the
  agents an application runs (FR-029b).
* The body is capped **while it is read**. The declared ``Content-Length`` is a
  fast path for honest clients, never the cap (FR-033a).
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from typing import Final

import msgspec
from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse

from loom.ai.abc import ErrorEvent
from loom.ai.config import AgentEndpointConfig, AiConfig
from loom.ai.errors import AgentRunErrorCode
from loom.ai.fastapi.response import AgentJSONResponse
from loom.ai.fastapi.streaming import encode_sse_event, stream_sse
from loom.ai.runtime import AgentRunError, AgentRuntime
from loom.core.config.errors import ConfigError
from loom.core.identity import Identity, current_identity
from loom.core.model import LoomFrozenStruct
from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.tracing import get_trace_id
from loom.rest.auth.abc import Authenticator

_logger = logging.getLogger(__name__)

# Fixed headroom on top of ``max_prompt_bytes`` for the JSON envelope around
# the ``prompt`` field (key name, quoting, escapes). The total body cap is
# ``max_prompt_bytes + _BODY_OVERHEAD_BYTES``; anything larger is refused
# before buffering beyond the cap.
_BODY_OVERHEAD_BYTES: Final[int] = 64 * 1024

# Silence after which the SSE surface emits a comment frame. Well under the
# 30 s idle timeout of the common reverse proxies, and not configurable: it is
# a property of the transport, not of an agent.
_HEARTBEAT_MS: Final[int] = 15_000

_MEDIA_TYPE_SSE: Final[str] = "text/event-stream"

# Published mapping of run-error codes to HTTP statuses (contract table). Codes
# absent from the table are deliberate 500s: an unmapped outcome is a defect in
# this table, not something to guess a status for.
_STATUS_BY_CODE: Mapping[AgentRunErrorCode, int] = {
    AgentRunErrorCode.OUTPUT_SCHEMA_VIOLATION: 422,
    AgentRunErrorCode.MAX_ITERATIONS_EXCEEDED: 422,
    AgentRunErrorCode.PROVIDER_UNAVAILABLE: 503,
    AgentRunErrorCode.PROVIDER_RATE_LIMITED: 503,
    AgentRunErrorCode.TOOL_UNAVAILABLE: 503,
    AgentRunErrorCode.TOOL_TIMEOUT: 504,
    AgentRunErrorCode.RUN_TIMEOUT: 504,
    AgentRunErrorCode.TOO_MANY_RUNS: 429,
    AgentRunErrorCode.UNAUTHORIZED: 403,
}


class _AgentRunRequest(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Body accepted by ``/run`` and ``/stream``: one prompt, nothing else."""

    prompt: str


_REQUEST_DECODER = msgspec.json.Decoder(_AgentRunRequest)


class _AgentHttpError(Exception):
    """A failure that is still a status code because no byte was sent yet.

    Args:
        status_code: HTTP status to answer with.
        code: Stable machine-readable code carried in the body.
        message: Human-readable description, safe to return to the caller.
    """

    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message

    def response(self) -> Response:
        """Return the flat ``{"code", "message"}`` body of this failure."""
        return _error_response(self.status_code, self.code, self.message)


def _error_response(status_code: int, code: str, message: str) -> Response:
    """Build the flat error body every agent endpoint answers failures with."""
    return AgentJSONResponse(content={"code": code, "message": message}, status_code=status_code)


def _run_error_response(error: AgentRunError) -> Response:
    """Map a run-error code onto its published HTTP status."""
    return _error_response(_STATUS_BY_CODE.get(error.code, 500), str(error.code), str(error))


def _declared_content_length(request: Request) -> int | None:
    """Return the Content-Length header as an int, or ``None`` when unusable."""
    header = request.headers.get("content-length")
    if header is None:
        return None
    try:
        return int(header)
    except ValueError:
        return None


def _prompt_too_large(max_bytes: int) -> _AgentHttpError:
    return _AgentHttpError(
        413,
        "PROMPT_TOO_LARGE",
        f"Request body exceeds the maximum accepted size ({max_bytes} bytes)",
    )


async def _read_body_capped(request: Request, *, max_bytes: int) -> bytes:
    """Read the request body without ever buffering more than *max_bytes*.

    The Content-Length check is only a fast path for honest clients; the capped
    stream read is the authoritative defense — it covers chunked bodies and
    lying headers too, aborting as soon as the cap is exceeded.

    Args:
        request: Incoming request whose body is read.
        max_bytes: Maximum number of bytes ever buffered.

    Returns:
        The body bytes.

    Raises:
        _AgentHttpError: 413 ``PROMPT_TOO_LARGE`` when the cap is hit.
    """
    declared = _declared_content_length(request)
    if declared is not None and declared > max_bytes:
        raise _prompt_too_large(max_bytes)
    received = 0
    chunks: list[bytes] = []
    async for chunk in request.stream():
        received += len(chunk)
        if received > max_bytes:
            raise _prompt_too_large(max_bytes)
        chunks.append(chunk)
    return b"".join(chunks)


async def _read_prompt(request: Request, *, max_prompt_bytes: int) -> str:
    """Read and validate the prompt of one invocation.

    Raises:
        _AgentHttpError: 413 when the body or the prompt exceeds its cap, 422
            when the body is not the documented ``{"prompt": ...}`` shape.
    """
    body = await _read_body_capped(request, max_bytes=max_prompt_bytes + _BODY_OVERHEAD_BYTES)
    try:
        parsed = _REQUEST_DECODER.decode(body)
    except msgspec.DecodeError as exc:
        raise _AgentHttpError(422, "INVALID_REQUEST", str(exc)) from exc
    if len(parsed.prompt.encode("utf-8")) > max_prompt_bytes:
        raise _prompt_too_large(max_prompt_bytes)
    return parsed.prompt


def _require_caller(name: str, endpoint: AgentEndpointConfig | None) -> Identity:
    """Return the verified caller, refusing anonymous ones before existence.

    An unknown agent has no ``allow_anonymous`` opt-out, so an anonymous probe
    is refused with 401 whatever the name — which is what stops the surface
    from being used as an agent directory (FR-029b).

    Raises:
        _AgentHttpError: 401 for an anonymous caller without an opt-out.
    """
    identity = current_identity()
    if identity.is_authenticated or (endpoint is not None and endpoint.allow_anonymous):
        return identity
    raise _AgentHttpError(401, "UNAUTHORIZED", f"agent {name!r} requires a verified caller")


def _require_agent(
    name: str, exposed: Mapping[str, AgentEndpointConfig], runtime: AgentRuntime
) -> None:
    """Refuse an agent that is not compiled or not exposed over HTTP.

    Raises:
        _AgentHttpError: 404 ``AGENT_NOT_FOUND``.
    """
    if name in exposed and runtime.has_agent(name):
        return
    raise _AgentHttpError(404, "AGENT_NOT_FOUND", f"no agent named {name!r} is exposed")


def _make_run_handler(
    runtime: AgentRuntime,
    config: AiConfig,
    exposed: Mapping[str, AgentEndpointConfig],
    *,
    path: str,
    observability_runtime: ObservabilityRuntime,
) -> Callable[[Request, str], object]:
    """Build the handler serving one complete run."""

    async def run_agent(request: Request, name: str) -> Response:
        try:
            identity = _require_caller(name, exposed.get(name))
            _require_agent(name, exposed, runtime)
            prompt = await _read_prompt(request, max_prompt_bytes=config.max_prompt_bytes)
            with observability_runtime.span(
                Scope.AGENT,
                "agent_run",
                trace_id=get_trace_id(),
                route=path,
                method="POST",
                status_code=200,
                agent=name,
                subject=identity.subject,
                mechanism=identity.mechanism,
            ):
                result = await runtime.run(name, prompt, identity=identity)
            return AgentJSONResponse(content=result)
        except _AgentHttpError as exc:
            return exc.response()
        except AgentRunError as exc:
            return _run_error_response(exc)
        except Exception:
            _logger.exception("Unhandled error in the run endpoint of agent %r", name)
            return _error_response(500, "INTERNAL_ERROR", "An unexpected error occurred")

    return run_agent


@contextmanager
def _always_closed(span: AbstractContextManager[None]) -> Iterator[None]:
    """Enter *span* and close it whatever ends the body, including a disconnect.

    :meth:`~loom.core.observability.runtime.ObservabilityRuntime.span` emits its
    terminal event for a normal exit or an ``Exception`` only. A client that
    walks away from an SSE stream ends the generator serving it with
    ``CancelledError`` or ``GeneratorExit`` — neither is an ``Exception`` — so
    without this adapter every abandoned stream would leave a span that emitted
    ``START`` and nothing else. A disconnect closes the span as a normal end:
    the run was not the thing that failed.

    Args:
        span: Span context manager to enter, and to close exactly once.

    Yields:
        ``None``, with the span open.
    """
    span.__enter__()
    try:
        yield
    except Exception as exc:
        span.__exit__(type(exc), exc, exc.__traceback__)
        raise
    except BaseException:
        span.__exit__(None, None, None)
        raise
    else:
        span.__exit__(None, None, None)


def _stream_frames(
    runtime: AgentRuntime,
    name: str,
    prompt: str,
    identity: Identity,
    *,
    path: str,
    observability_runtime: ObservabilityRuntime,
) -> AsyncIterator[bytes]:
    """Drive one run inside the response's own task and encode it as SSE.

    The run's single span is opened inside the generator, not around the
    handler: the handler returns as soon as the response exists, while the run
    lasts for as long as the frames are pulled. Entering it here makes the span
    open on the first frame and close on generator exit — including the
    cancellation of a disconnected client, which :func:`_always_closed`
    turns into a terminal event.

    The span carries no ``status_code``: the status line of a stream is
    committed to 200 before the run produces anything, so the field would be a
    constant that reads as "the run succeeded" even when the terminal frame is
    an ``error`` one or the client left mid-run.
    """

    async def _frames() -> AsyncIterator[bytes]:
        with _always_closed(
            observability_runtime.span(
                Scope.AGENT,
                "agent_run",
                trace_id=get_trace_id(),
                route=path,
                method="POST",
                agent=name,
                subject=identity.subject,
                mechanism=identity.mechanism,
            )
        ):
            try:
                async with runtime.run_stream(name, prompt, identity=identity) as events:
                    async for frame in stream_sse(events, heartbeat_ms=_HEARTBEAT_MS):
                        yield frame
            except AgentRunError as exc:
                # Admission failures surface once the response exists, so they
                # can only travel in-band, as this stream's terminal frame.
                yield encode_sse_event(ErrorEvent(code=exc.code, message=str(exc)))

    return _frames()


def _make_stream_handler(
    runtime: AgentRuntime,
    config: AiConfig,
    exposed: Mapping[str, AgentEndpointConfig],
    *,
    path: str,
    observability_runtime: ObservabilityRuntime,
) -> Callable[[Request, str], object]:
    """Build the handler serving one run as server-sent events."""

    async def stream_agent(request: Request, name: str) -> Response:
        try:
            identity = _require_caller(name, exposed.get(name))
            _require_agent(name, exposed, runtime)
            prompt = await _read_prompt(request, max_prompt_bytes=config.max_prompt_bytes)
        except _AgentHttpError as exc:
            return exc.response()
        return StreamingResponse(
            _stream_frames(
                runtime,
                name,
                prompt,
                identity,
                path=path,
                observability_runtime=observability_runtime,
            ),
            media_type=_MEDIA_TYPE_SSE,
        )

    return stream_agent


def _make_health_handler(
    runtime: AgentRuntime,
    exposed: Mapping[str, AgentEndpointConfig],
) -> Callable[[str], object]:
    """Build the handler serving the cached health of one agent."""

    async def agent_health(name: str) -> Response:
        try:
            _require_agent(name, exposed, runtime)
        except _AgentHttpError as exc:
            return exc.response()
        health = await runtime.health(name)
        payload: dict[str, object] = {"status": health.status}
        if health.detail is not None:
            payload["detail"] = health.detail
        # Dependency identifiers are internal topology: an anonymous scrape
        # gets the aggregate only (FR-029c).
        if current_identity().is_authenticated:
            payload["checks"] = dict(health.checks)
        status_code = 503 if health.status == "unavailable" else 200
        return AgentJSONResponse(content=payload, status_code=status_code)

    return agent_health


def _exposed_agents(
    config: AiConfig, authenticator: Authenticator | None
) -> Mapping[str, AgentEndpointConfig]:
    """Select the agents that opted into HTTP, refusing unusable opt-ins.

    Raises:
        ConfigError: When an agent opts in but no authentication mechanism can
            verify its callers and it does not declare ``allow_anonymous``.
    """
    exposed: dict[str, AgentEndpointConfig] = {}
    for name, endpoint in config.endpoints.items():
        if not endpoint.enabled or not endpoint.auth.strip():
            continue
        _require_usable_authenticator(name, endpoint, authenticator)
        exposed[name] = endpoint
    return exposed


def _require_usable_authenticator(
    name: str, endpoint: AgentEndpointConfig, authenticator: Authenticator | None
) -> None:
    if authenticator is not None or endpoint.allow_anonymous:
        return
    raise ConfigError(
        f"Agent {name!r}: 'ai.endpoints.{name}.auth' requires a verified caller but the "
        "application configures no authentication. Add the 'app.rest.auth.jwt' section, "
        "pass create_app(authenticator=...), or set 'allow_anonymous: true' explicitly."
    )


def _announce_mount(
    runtime: AgentRuntime,
    name: str,
    endpoint: AgentEndpointConfig,
    *,
    prefix: str,
) -> None:
    """Emit the startup WARNING carrying the security state of one mount."""
    if not runtime.has_agent(name):
        _logger.warning(
            "Agent endpoint configured for agent=%s but no compiled agent has that name: "
            "its routes answer 404",
            name,
        )
        return
    _logger.warning(
        "Agent endpoints mounted: path=%s/%s/{run,stream,health} agent=%s auth=%s "
        "allow_anonymous=%s capabilities=%s. %s",
        prefix,
        name,
        name,
        endpoint.auth,
        endpoint.allow_anonymous,
        ",".join(runtime.capability_kinds(name)) or "none",
        _identity_notice(endpoint),
    )


def _identity_notice(endpoint: AgentEndpointConfig) -> str:
    """State plainly which identity the capability calls of this mount run as.

    ``allow_anonymous`` is not a relaxation of the caller check on top of an
    otherwise verified identity: it removes the identity altogether, so the
    reassuring sentence of an authenticated mount would be false next to it.
    """
    if endpoint.allow_anonymous:
        return (
            "allow_anonymous is set, so callers are NOT authenticated: every capability "
            "call runs with no verified identity, and every run spends model tokens on "
            "behalf of an unidentified caller — only 'max_concurrent_runs' and "
            "'run_timeout_ms' bound that cost, there is no rate limit"
        )
    return (
        "'auth' only authenticates the caller; every capability call then runs as that "
        "verified identity"
    )


def bind_agent_endpoints(
    app: FastAPI,
    *,
    runtime: AgentRuntime,
    config: AiConfig,
    authenticator: Authenticator | None = None,
    observability_runtime: ObservabilityRuntime | None = None,
    prefix: str = "/agents",
) -> None:
    """Mount ``/run``, ``/stream`` and ``/health`` for every opted-in agent.

    Only agents present in ``ai.endpoints`` with ``enabled`` **and** a named
    ``auth`` are reachable; every other compiled agent exposes no HTTP surface
    at all. Each mount is announced with a WARNING carrying its security state.

    Args:
        app: FastAPI application to mount the routes on.
        runtime: Entered runtime serving the agents.
        config: Parsed ``ai:`` section.
        authenticator: Mechanism authenticating callers of the application.
            ``None`` is only acceptable for agents declaring
            ``allow_anonymous``.
        observability_runtime: Runtime emitting one span per run, over both
            ``/run`` and ``/stream``. This surface is the single owner of that
            span: :class:`~loom.ai.runtime.AgentRuntime` emits none.
        prefix: Path prefix the routes are mounted under.

    Raises:
        ConfigError: When an agent opts into HTTP without a usable
            authenticator and without ``allow_anonymous``.

    Example::

        bind_agent_endpoints(app, runtime=runtime, config=ai_config,
                             authenticator=authenticator)
    """
    exposed = _exposed_agents(config, authenticator)
    if not exposed:
        return
    observability = (
        observability_runtime if observability_runtime is not None else ObservabilityRuntime.noop()
    )
    run_path = f"{prefix}/{{name}}/run"
    app.add_api_route(
        run_path,
        _make_run_handler(
            runtime, config, exposed, path=run_path, observability_runtime=observability
        ),
        methods=["POST"],
    )
    stream_path = f"{prefix}/{{name}}/stream"
    app.add_api_route(
        stream_path,
        _make_stream_handler(
            runtime, config, exposed, path=stream_path, observability_runtime=observability
        ),
        methods=["POST"],
    )
    app.add_api_route(
        f"{prefix}/{{name}}/health",
        _make_health_handler(runtime, exposed),
        methods=["GET"],
    )
    for name, endpoint in exposed.items():
        _announce_mount(runtime, name, endpoint, prefix=prefix)
