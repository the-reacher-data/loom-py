"""Transport primitives shared by the HTTP and the A2A agent surfaces.

Both surfaces put the same run behind two different wire protocols, so both
need the same request-path rules: the body cap that is applied *while* the body
is read, the caller check evaluated before existence, the span that closes on a
disconnect, and the heartbeat race that keeps a silent stream alive without
letting a timeout tear down the run it is protecting.

This module owns those rules once. It is deliberately protocol-agnostic:
:func:`with_heartbeats` relays already-encoded ``bytes``, so the HTTP surface
can feed it SSE event frames and the A2A surface JSON-RPC envelopes, and
neither has to reimplement the race. Everything above the frame — which frames
exist, what they are named, how many an agent event projects to — stays with
the transport that defines it.

Nothing here renders a response: the two surfaces encode their own bodies, so
this module imports Starlette's ``Request`` and nothing else from either.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import Final

from starlette.requests import Request

from loom.ai.abc import ErrorEvent
from loom.ai.config import AgentEndpointConfig
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.core.identity import Identity, current_identity

BODY_OVERHEAD_BYTES: Final[int] = 64 * 1024
"""Headroom over ``max_prompt_bytes`` for the JSON envelope around the prompt.

The total body cap is ``max_prompt_bytes + BODY_OVERHEAD_BYTES``; anything
larger is refused before buffering beyond the cap.
"""

HEARTBEAT_MS: Final[int] = 15_000
"""Silence after which a streaming surface emits one comment frame.

Well under the 30 s idle timeout of the common reverse proxies, and not
configurable: it is a property of the transport, not of an agent.
"""

HEARTBEAT_FRAME: Final[bytes] = b": ping\n\n"
"""SSE comment frame keeping a silent stream alive; clients ignore it."""

_UNEXPECTED_FAILURE: Final[str] = "the agent run failed unexpectedly"


class TransportError(Exception):
    """A failure that is still a status code because no byte was sent yet.

    Rendering is left to the caller: this module stays free of any response
    type, so it never has to import a transport's encoder and the import graph
    keeps pointing one way — from a surface into the shared rules, never back.

    Args:
        status_code: HTTP status to answer with.
        code: Stable machine-readable code carried in the body.
        message: Human-readable description, safe to return to the caller.

    Attributes:
        status_code: HTTP status to answer with.
        code: Stable machine-readable code carried in the body.
        message: Human-readable description, safe to return to the caller.
    """

    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


def _declared_content_length(request: Request) -> int | None:
    """Return the Content-Length header as an int, or ``None`` when unusable."""
    header = request.headers.get("content-length")
    if header is None:
        return None
    try:
        return int(header)
    except ValueError:
        return None


def _prompt_too_large(max_bytes: int) -> TransportError:
    return TransportError(
        413,
        "PROMPT_TOO_LARGE",
        f"Request body exceeds the maximum accepted size ({max_bytes} bytes)",
    )


async def read_body_capped(request: Request, *, max_bytes: int) -> bytes:
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
        TransportError: 413 ``PROMPT_TOO_LARGE`` when the cap is hit.

    Example::

        body = await read_body_capped(request, max_bytes=cap)
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


def require_caller(name: str, endpoint: AgentEndpointConfig | None) -> Identity:
    """Return the verified caller, refusing anonymous ones before existence.

    An unknown agent has no ``allow_anonymous`` opt-out, so an anonymous probe
    is refused with 401 whatever the name — which is what stops an agent
    surface from being used as an agent directory (FR-029b).

    Args:
        name: Agent the caller addressed.
        endpoint: Endpoint configuration of that agent, or ``None`` when the
            agent is unknown to this surface.

    Returns:
        The verified caller, or the anonymous identity when the agent declares
        ``allow_anonymous``.

    Raises:
        TransportError: 401 for an anonymous caller without an opt-out.

    Example::

        identity = require_caller("analyst", exposed.get("analyst"))
    """
    identity = current_identity()
    if identity.is_authenticated or (endpoint is not None and endpoint.allow_anonymous):
        return identity
    raise TransportError(401, "UNAUTHORIZED", f"agent {name!r} requires a verified caller")


@contextmanager
def always_closed(span: AbstractContextManager[None]) -> Iterator[None]:
    """Enter *span* and close it whatever ends the body, including a disconnect.

    :meth:`~loom.core.observability.runtime.ObservabilityRuntime.span` emits its
    terminal event for a normal exit or an ``Exception`` only. A client that
    walks away from a stream ends the generator serving it with
    ``CancelledError`` or ``GeneratorExit`` — neither is an ``Exception`` — so
    without this adapter every abandoned stream would leave a span that emitted
    ``START`` and nothing else. A disconnect closes the span as a normal end:
    the run was not the thing that failed.

    Args:
        span: Span context manager to enter, and to close exactly once.

    Yields:
        ``None``, with the span open.

    Example::

        with always_closed(observability.span(Scope.AGENT, "agent_run")):
            ...
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


def failure_event(exc: BaseException) -> ErrorEvent:
    """Turn a post-first-byte failure into a stream's terminal error event.

    The test is ``isinstance``, not an exact class match: a deployment that
    subclasses :class:`~loom.ai.errors.AgentRunError` to carry extra context
    still owns a stable code, and dropping it would answer a truthful
    ``TOOL_TIMEOUT`` as an invented ``PROVIDER_UNAVAILABLE``.

    Args:
        exc: Failure raised once the status line was already committed.

    Returns:
        The ``error`` event carrying the failure's code, or the catch-all
        ``PROVIDER_UNAVAILABLE`` with a fixed message for anything else.

    Example::

        event = failure_event(AgentRunError(AgentRunErrorCode.RUN_TIMEOUT, "late"))
    """
    if isinstance(exc, AgentRunError):
        return ErrorEvent(code=exc.code, message=str(exc))
    return ErrorEvent(code=AgentRunErrorCode.PROVIDER_UNAVAILABLE, message=_UNEXPECTED_FAILURE)


async def _next_frame(frames: AsyncIterator[bytes]) -> bytes | None:
    """Await the next frame, reporting exhaustion as ``None``."""
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


async def with_heartbeats(
    frames: AsyncIterator[bytes], *, heartbeat_ms: int
) -> AsyncIterator[bytes]:
    """Relay encoded frames, emitting a comment frame during long silences.

    Every silence longer than *heartbeat_ms* produces one
    :data:`HEARTBEAT_FRAME` and the race starts again. The awaited frame is
    shielded from the heartbeat — a timeout must not tear down the run it is
    keeping alive — and is cancelled and drained when the consumer goes away,
    so no work outlives it (FR-033).

    The race lives in this generator rather than in a background task feeding a
    queue: a background task survives the disconnect cancellation and keeps a
    run burning tokens with nobody listening.

    Args:
        frames: Already-encoded frames to relay, in order; exhaustion ends the
            stream.
        heartbeat_ms: Silence after which a comment frame is emitted.

    Yields:
        The relayed frames, interleaved with heartbeat comment frames.

    Example::

        async for frame in with_heartbeats(encoded, heartbeat_ms=HEARTBEAT_MS):
            ...
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
