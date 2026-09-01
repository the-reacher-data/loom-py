"""HTTP surface of the agent runtime (T086-T095, T098).

Drives ``bind_agent_endpoints`` over a live :class:`AgentRuntime` backed by
scripted engines, asserting the wire contract in
``specs/001-ai-agent-layer/contracts/http-sse.md``: double opt-in mounting,
401 before 404, the body cap enforced while reading, the run-error status
table, the SSE surface, the redacted health projection, and the import
containment that keeps ``loom.ai`` out of an application with no ``ai:``
section.
"""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import sys
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from fastapi import FastAPI

from loom.ai.a2a.card import card_path
from loom.ai.a2a.server import bind_a2a_endpoints
from loom.ai.abc import AgentEvent, ErrorEvent, FinalEvent, TextDeltaEvent
from loom.ai.compiler._plan import AgentPlan
from loom.ai.config import A2AConfig, AgentEndpointConfig
from loom.ai.errors import AgentCompilationError, AgentErrorCode, AgentRunErrorCode
from loom.ai.fastapi.endpoints import bind_agent_endpoints
from loom.ai.runtime import AgentRuntime
from loom.core.config.errors import ConfigError
from loom.core.di import LoomContainer
from loom.core.identity import Identity, reset_identity, set_identity
from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.rest.auth.middleware import AuthenticationMiddleware
from tests.integration.ai.conftest import (
    DEFAULT_OUTPUT,
    DEFAULT_USAGE,
    CountingEngineProvider,
    RecordingMcpSession,
    ScriptedEngine,
    StubDepsFactory,
    StubMcpClient,
    make_ai_config,
    make_endpoint,
    make_mcp_capability,
    make_mcp_servers,
    make_plan,
    mcp_client_factory,
)

_AGENT = "analyst"
_PREFIX = "/agents"
_MCP_SERVER = "tools"
_SRC = Path(__file__).resolve().parents[3] / "src"


class StubAuthenticator:
    """Authentication mechanism standing in for any configured one."""

    name = "stub"
    provides_roles = True

    async def authenticate(self, credentials: object) -> Identity | None:
        """Authenticate every caller as the same fixed subject."""
        del credentials
        return Identity(subject="stub-user", mechanism=self.name)


class _RecordingObserver:
    """Lifecycle observer keeping every event a span emitted, in order."""

    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []

    def on_event(self, event: LifecycleEvent) -> None:
        """Record one lifecycle event."""
        self.events.append(event)


class _IdentityMiddleware:
    """Publishes a fixed identity for the duration of one request."""

    def __init__(self, app: Any, *, identity: Identity | None) -> None:
        self._app = app
        self._identity = identity

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        """Set the identity around the downstream ASGI call."""
        if scope["type"] != "http" or self._identity is None:
            await self._app(scope, receive, send)
            return
        token = set_identity(self._identity)
        try:
            await self._app(scope, receive, send)
        finally:
            reset_identity(token)


@asynccontextmanager
async def _serving(
    *,
    deps: StubDepsFactory,
    container: LoomContainer,
    plans: Sequence[AgentPlan] | None = None,
    engines: Mapping[str, ScriptedEngine] | None = None,
    endpoints: Mapping[str, AgentEndpointConfig] | None = None,
    identity: Identity | None = None,
    authenticator: object | None = None,
    max_prompt_bytes: int = 65536,
    health_cache_ttl_ms: int = 20,
    observability_runtime: ObservabilityRuntime | None = None,
) -> AsyncIterator[tuple[FastAPI, httpx.AsyncClient]]:
    """Serve an entered runtime over an in-process ASGI client."""
    config = make_ai_config(
        endpoints=dict(endpoints if endpoints is not None else {_AGENT: make_endpoint()}),
        mcp_servers=make_mcp_servers(_MCP_SERVER),
        max_prompt_bytes=max_prompt_bytes,
        health_cache_ttl_ms=health_cache_ttl_ms,
    )
    runtime = AgentRuntime(
        plans=list(plans if plans is not None else (make_plan(_AGENT),)),
        config=config,
        engine_provider=CountingEngineProvider(engines=engines),  # type: ignore[arg-type]
        deps=deps,
        container=container,
        mcp_client_factory=mcp_client_factory(
            {_MCP_SERVER: StubMcpClient(label="mcp", session=RecordingMcpSession(), log=[])}
        ),  # type: ignore[arg-type]
    )
    async with runtime:
        app = FastAPI()
        bind_agent_endpoints(
            app,
            runtime=runtime,
            config=config,
            authenticator=authenticator if authenticator is not None else StubAuthenticator(),  # type: ignore[arg-type]
            observability_runtime=observability_runtime,
            prefix=_PREFIX,
        )
        app.add_middleware(_IdentityMiddleware, identity=identity)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://agents.test") as client:
            yield app, client


def _route_paths(app: FastAPI) -> set[str]:
    """Return every path the application mounted."""
    return {str(getattr(route, "path", "")) for route in app.routes}


def _sse_names(payload: str) -> list[str]:
    """Return the event names of an SSE payload, ignoring comment frames."""
    return [line[len("event: ") :] for line in payload.splitlines() if line.startswith("event: ")]


async def _asgi_post(
    app: FastAPI,
    path: str,
    chunks: Sequence[bytes],
    headers: Sequence[tuple[bytes, bytes]],
) -> int:
    """POST raw body chunks over ASGI and return the response status."""
    messages: list[dict[str, Any]] = [
        {"type": "http.request", "body": chunk, "more_body": True} for chunk in chunks
    ]
    messages.append({"type": "http.request", "body": b"", "more_body": False})
    consumed = 0
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        nonlocal consumed
        message = messages[min(consumed, len(messages) - 1)]
        consumed += 1
        return message

    async def send(message: Any) -> None:
        sent.append(dict(message))

    scope: dict[str, Any] = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": list(headers),
        "client": ("testclient", 50000),
        "server": ("agents.test", 80),
    }
    await app(scope, receive, send)
    return int(next(m["status"] for m in sent if m["type"] == "http.response.start"))


class TestMontaje:
    """Double opt-in: only ``enabled`` agents with a named ``auth`` mount (T095)."""

    async def test_no_monta_ruta_cuando_el_agente_no_esta_en_endpoints(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """An agent absent from ``ai.endpoints`` exposes no route at all."""
        async with _serving(
            deps=deps,
            container=container,
            plans=(make_plan(_AGENT), make_plan("hidden")),
            identity=identity,
        ) as (app, _client):
            assert f"{_PREFIX}/hidden/run" not in _route_paths(app)

    async def test_responde_404_cuando_el_agente_no_esta_en_endpoints(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """A compiled but unexposed agent is unreachable for a verified caller."""
        async with _serving(
            deps=deps,
            container=container,
            plans=(make_plan(_AGENT), make_plan("hidden")),
            identity=identity,
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/hidden/run", json={"prompt": "p"})

            assert response.status_code == 404

    async def test_no_monta_ruta_cuando_el_agente_esta_deshabilitado(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """``enabled: false`` is an opt-out even with a named authentication."""
        async with _serving(
            deps=deps,
            container=container,
            endpoints={_AGENT: make_endpoint(enabled=False)},
            identity=identity,
        ) as (app, _client):
            assert f"{_PREFIX}/{_AGENT}/run" not in _route_paths(app)

    async def test_falla_al_arrancar_cuando_no_hay_authenticator_utilizable(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """Opting in without an authenticator and without anonymous aborts start-up."""
        config = make_ai_config(endpoints={_AGENT: make_endpoint(allow_anonymous=False)})
        runtime = AgentRuntime(
            plans=[make_plan(_AGENT)],
            config=config,
            engine_provider=CountingEngineProvider(),  # type: ignore[arg-type]
            deps=deps,
            container=container,
        )

        async with runtime:
            with pytest.raises(ConfigError):
                bind_agent_endpoints(
                    FastAPI(), runtime=runtime, config=config, authenticator=None, prefix=_PREFIX
                )

    async def test_avisa_con_warning_cuando_monta_un_agente(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Every mount announces its security state at WARNING level."""
        with caplog.at_level(logging.WARNING):
            async with _serving(deps=deps, container=container) as (_app, _client):
                pass

        assert _AGENT in caplog.text

    @staticmethod
    async def _mount_warning(
        *,
        deps: StubDepsFactory,
        container: LoomContainer,
        caplog: pytest.LogCaptureFixture,
        allow_anonymous: bool,
    ) -> str:
        """Return the mount WARNING emitted for one endpoint configuration."""
        endpoints = {_AGENT: make_endpoint(allow_anonymous=allow_anonymous)}
        with caplog.at_level(logging.WARNING):
            async with _serving(deps=deps, container=container, endpoints=endpoints) as (
                _app,
                _client,
            ):
                pass
        return caplog.text

    async def test_no_afirma_identidad_verificada_cuando_el_montaje_es_anonimo(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """``allow_anonymous`` removes the identity: the warning must not claim one."""
        text = await self._mount_warning(
            deps=deps, container=container, caplog=caplog, allow_anonymous=True
        )

        assert "runs as that verified identity" not in text
        assert "NOT authenticated" in text

    async def test_avisa_del_gasto_sin_identidad_cuando_el_montaje_es_anonimo(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """An anonymous mount spends model tokens for an unidentified caller: say so."""
        text = await self._mount_warning(
            deps=deps, container=container, caplog=caplog, allow_anonymous=True
        )

        assert "no verified identity" in text
        assert "max_concurrent_runs" in text
        assert "rate limit" in text

    async def test_afirma_identidad_verificada_cuando_el_montaje_exige_auth(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """An authenticated mount keeps the sentence that is true for it."""
        text = await self._mount_warning(
            deps=deps, container=container, caplog=caplog, allow_anonymous=False
        )

        assert "runs as that verified identity" in text


class TestAutenticacionAntesDeExistencia:
    """401 precedes 404 so the surface cannot enumerate agents (T089)."""

    async def test_responde_401_cuando_el_agente_no_existe_y_el_llamador_es_anonimo(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """An anonymous probe for an unknown agent gets 401, never 404 (FR-029b)."""
        async with _serving(deps=deps, container=container, identity=None) as (_app, client):
            response = await client.post(f"{_PREFIX}/does-not-exist/run", json={"prompt": "p"})

            assert response.status_code == 401

    async def test_responde_401_cuando_el_agente_existe_y_el_llamador_es_anonimo(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """An anonymous caller is refused before the agent is even resolved."""
        async with _serving(deps=deps, container=container, identity=None) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p"})

            assert response.status_code == 401


class TestRun:
    """``POST /run`` returns the decoded output and its usage (T087)."""

    async def test_responde_200_cuando_la_identidad_es_valida(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """A verified caller drives the agent to completion."""
        async with _serving(deps=deps, container=container, identity=identity) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p"})

            assert response.status_code == 200

    async def test_devuelve_output_y_usage_cuando_la_identidad_es_valida(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """The body carries the decoded output plus the run's usage, encoded once."""
        async with _serving(deps=deps, container=container, identity=identity) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p"})

            assert response.json() == {
                "output": dict(DEFAULT_OUTPUT),
                "usage": {
                    "input_tokens": DEFAULT_USAGE.input_tokens,
                    "output_tokens": DEFAULT_USAGE.output_tokens,
                    "requests": DEFAULT_USAGE.requests,
                    "duration_ms": DEFAULT_USAGE.duration_ms,
                },
            }


class TestTopeDeCuerpo:
    """The prompt cap is enforced while reading, never from the header (T088)."""

    async def test_responde_413_cuando_el_cuerpo_supera_el_tope(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """An oversized honest body is rejected with 413."""
        async with _serving(
            deps=deps, container=container, identity=identity, max_prompt_bytes=32
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/run", json={"prompt": "x" * 200_000})

            assert response.status_code == 413

    async def test_codifica_prompt_too_large_cuando_el_cuerpo_supera_el_tope(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """The 413 body carries the stable ``PROMPT_TOO_LARGE`` code."""
        async with _serving(
            deps=deps, container=container, identity=identity, max_prompt_bytes=32
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/run", json={"prompt": "x" * 200_000})

            assert response.json()["code"] == "PROMPT_TOO_LARGE"

    async def test_responde_413_cuando_el_content_length_miente(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """A body larger than its declared length is still rejected while reading."""
        chunk = b'{"prompt":"' + b"x" * 100_000 + b'"}'
        async with _serving(
            deps=deps, container=container, identity=identity, max_prompt_bytes=32
        ) as (app, _client):
            status = await _asgi_post(
                app,
                f"{_PREFIX}/{_AGENT}/run",
                [chunk, chunk, chunk],
                [
                    (b"host", b"agents.test"),
                    (b"content-type", b"application/json"),
                    (b"content-length", b"12"),
                ],
            )

            assert status == 413


class TestMapeoDeErrores:
    """Run-error codes map to the published status codes (contract table)."""

    @pytest.mark.parametrize(
        ("code", "status"),
        [
            (AgentRunErrorCode.OUTPUT_SCHEMA_VIOLATION, 422),
            (AgentRunErrorCode.PROVIDER_UNAVAILABLE, 503),
            (AgentRunErrorCode.TOOL_UNAVAILABLE, 503),
            (AgentRunErrorCode.TOOL_TIMEOUT, 504),
            (AgentRunErrorCode.RUN_TIMEOUT, 504),
            (AgentRunErrorCode.TOO_MANY_RUNS, 429),
            (AgentRunErrorCode.UNAUTHORIZED, 403),
        ],
    )
    async def test_mapea_el_status_cuando_la_ejecucion_falla(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
        code: AgentRunErrorCode,
        status: int,
    ) -> None:
        """Each run-error code produces its documented HTTP status."""
        engine = ScriptedEngine(script=(ErrorEvent(code=code, message="scripted"),))
        async with _serving(
            deps=deps, container=container, identity=identity, engines={_AGENT: engine}
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p"})

            assert response.status_code == status


class TestStream:
    """``POST /stream`` is SSE, with pre-stream failures as status codes (T090)."""

    @staticmethod
    def _script() -> tuple[AgentEvent, ...]:
        return (
            TextDeltaEvent(text="Demand rose "),
            FinalEvent(output={"answer": "42"}, usage=DEFAULT_USAGE),
        )

    async def test_responde_text_event_stream_cuando_la_identidad_es_valida(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """The streaming endpoint advertises the SSE media type."""
        engine = ScriptedEngine(script=self._script())
        async with _serving(
            deps=deps, container=container, identity=identity, engines={_AGENT: engine}
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/stream", json={"prompt": "p"})

            assert response.headers["content-type"].startswith("text/event-stream")

    async def test_emite_las_tramas_en_orden_cuando_la_identidad_es_valida(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """Frames arrive in script order and end in the single terminal event."""
        engine = ScriptedEngine(script=self._script())
        async with _serving(
            deps=deps, container=container, identity=identity, engines={_AGENT: engine}
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/stream", json={"prompt": "p"})

            assert _sse_names(response.text) == ["text_delta", "final"]

    async def test_responde_401_cuando_falla_antes_del_primer_byte(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A pre-stream failure is a status code, never an SSE frame."""
        async with _serving(deps=deps, container=container, identity=None) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/stream", json={"prompt": "p"})

            assert response.status_code == 401

    async def test_emite_error_en_banda_cuando_falla_tras_el_primer_byte(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """A failure after the first byte travels in-band as an ``error`` frame."""
        engine = ScriptedEngine(
            script=(
                TextDeltaEvent(text="Demand rose "),
                ErrorEvent(code=AgentRunErrorCode.PROVIDER_RATE_LIMITED, message="rate limited"),
            )
        )
        async with _serving(
            deps=deps, container=container, identity=identity, engines={_AGENT: engine}
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/stream", json={"prompt": "p"})

            assert _sse_names(response.text) == ["text_delta", "error"]


class TestTrazaDelStream:
    """``POST /stream`` is attributed: exactly one agent span, never one per delta."""

    @staticmethod
    def _script() -> tuple[AgentEvent, ...]:
        return (
            TextDeltaEvent(text="Demand "),
            TextDeltaEvent(text="rose "),
            TextDeltaEvent(text="12%"),
            FinalEvent(output=DEFAULT_OUTPUT, usage=DEFAULT_USAGE),
        )

    @staticmethod
    async def _stream_with_recorder(
        *,
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
    ) -> list[LifecycleEvent]:
        """Drive one full ``/stream`` request and return the events it emitted."""
        recorder = _RecordingObserver()
        engine = ScriptedEngine(script=TestTrazaDelStream._script())
        async with _serving(
            deps=deps,
            container=container,
            identity=identity,
            engines={_AGENT: engine},
            observability_runtime=ObservabilityRuntime([recorder]),
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/stream", json={"prompt": "p"})
            assert _sse_names(response.text) == [
                "text_delta",
                "text_delta",
                "text_delta",
                "final",
            ]
        return recorder.events

    async def test_emite_un_unico_span_de_agente_cuando_atiende_un_stream(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """One run is one span: three deltas do not become three spans."""
        events = await self._stream_with_recorder(deps=deps, container=container, identity=identity)

        starts = [
            event
            for event in events
            if event.scope is Scope.AGENT and event.kind is EventKind.START
        ]
        assert len(starts) == 1

    async def test_cierra_el_span_cuando_acaba_el_stream(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """The span closes when the generator is exhausted, not when the handler returns."""
        events = await self._stream_with_recorder(deps=deps, container=container, identity=identity)

        ends = [
            event for event in events if event.scope is Scope.AGENT and event.kind is EventKind.END
        ]
        assert len(ends) == 1

    @staticmethod
    async def _abandon_stream_after_first_frame(app: FastAPI, path: str) -> None:
        """Drive one ``/stream`` request over ASGI and disconnect mid-run.

        ``httpx.ASGITransport`` buffers the whole response, so it cannot express
        a client that walks away; the raw ASGI call can. The disconnect is
        published only once a frame reached the wire, so the span is provably
        open when the stream is abandoned.
        """
        frame_sent = asyncio.Event()
        pending: list[dict[str, Any]] = [
            {"type": "http.request", "body": b'{"prompt": "p"}', "more_body": False}
        ]

        async def receive() -> dict[str, Any]:
            if pending:
                return pending.pop(0)
            await frame_sent.wait()
            return {"type": "http.disconnect"}

        async def send(message: Any) -> None:
            if message["type"] == "http.response.body" and message.get("body"):
                frame_sent.set()

        scope: dict[str, Any] = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "root_path": "",
            "headers": [(b"content-type", b"application/json")],
            "client": ("testclient", 50000),
            "server": ("agents.test", 80),
        }
        await app(scope, receive, send)

    async def test_cierra_el_span_cuando_el_cliente_abandona_el_stream(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """A disconnect terminates the span: an abandoned stream leaks no open span."""
        recorder = _RecordingObserver()
        # The second delta never arrives, so the run is still in flight — and
        # its span still open — when the client disconnects after the first.
        engine = ScriptedEngine(
            script=(
                TextDeltaEvent(text="Demand "),
                TextDeltaEvent(text="rose "),
                FinalEvent(output=DEFAULT_OUTPUT, usage=DEFAULT_USAGE),
            ),
            delays_ms=(0, 60_000, 0),
        )
        async with _serving(
            deps=deps,
            container=container,
            identity=identity,
            engines={_AGENT: engine},
            observability_runtime=ObservabilityRuntime([recorder]),
        ) as (app, _client):
            await self._abandon_stream_after_first_frame(app, f"{_PREFIX}/{_AGENT}/stream")

        agent_kinds = [event.kind for event in recorder.events if event.scope is Scope.AGENT]
        assert agent_kinds[0] is EventKind.START
        assert agent_kinds[-1] in (EventKind.END, EventKind.ERROR)

    async def test_atribuye_el_sujeto_cuando_atiende_un_stream(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """The span carries who ran what, over which route: an audit needs all of it."""
        events = await self._stream_with_recorder(deps=deps, container=container, identity=identity)

        start = next(
            event
            for event in events
            if event.scope is Scope.AGENT and event.kind is EventKind.START
        )
        assert start.meta["subject"] == identity.subject
        assert start.meta["agent"] == _AGENT
        assert start.meta["mechanism"] == identity.mechanism
        assert start.meta["route"] == f"{_PREFIX}/{{name}}/stream"
        assert start.meta["method"] == "POST"


class TestHealth:
    """``GET /health`` is cached, redacted for anonymous callers (T093/T094)."""

    @staticmethod
    async def _wait_for_status(
        client: httpx.AsyncClient, path: str, expected: str, *, budget_s: float = 0.5
    ) -> httpx.Response:
        """Poll the cached health until the background probe reports ``expected``."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + budget_s
        response = await client.get(path)
        while loop.time() < deadline and response.json().get("status") != expected:
            await asyncio.sleep(0.005)
            response = await client.get(path)
        return response

    async def test_omite_checks_cuando_el_llamador_es_anonimo(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """An anonymous caller gets the aggregate only (FR-029c)."""
        plan = make_plan(_AGENT, capabilities=(make_mcp_capability(_MCP_SERVER),))
        async with _serving(deps=deps, container=container, plans=(plan,), identity=None) as (
            _app,
            client,
        ):
            response = await client.get(f"{_PREFIX}/{_AGENT}/health")

            assert "checks" not in response.json()

    async def test_no_revela_dependencias_cuando_el_llamador_es_anonimo(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """No dependency identifier appears anywhere in the anonymous body."""
        plan = make_plan(_AGENT, capabilities=(make_mcp_capability(_MCP_SERVER),))
        async with _serving(deps=deps, container=container, plans=(plan,), identity=None) as (
            _app,
            client,
        ):
            body = (await client.get(f"{_PREFIX}/{_AGENT}/health")).text

            assert "tools.internal" not in body and "mcp:" not in body

    async def test_devuelve_checks_cuando_el_llamador_esta_autenticado(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """A verified caller gets the per-dependency breakdown."""
        plan = make_plan(_AGENT, capabilities=(make_mcp_capability(_MCP_SERVER),))
        async with _serving(deps=deps, container=container, plans=(plan,), identity=identity) as (
            _app,
            client,
        ):
            response = await self._wait_for_status(client, f"{_PREFIX}/{_AGENT}/health", "ok")

            assert "checks" in response.json()

    async def test_responde_503_cuando_el_agente_esta_no_disponible(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """An ``unavailable`` aggregate is reported with a 503 status."""
        engine = ScriptedEngine(health_status="unavailable")
        async with _serving(
            deps=deps, container=container, identity=identity, engines={_AGENT: engine}
        ) as (_app, client):
            response = await self._wait_for_status(
                client, f"{_PREFIX}/{_AGENT}/health", "unavailable"
            )

            assert response.status_code == 503

    async def test_reporta_probing_cuando_la_primera_sonda_no_ha_terminado(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """Before the first probe completes the status is ``degraded``/``probing``."""
        engine = ScriptedEngine(health_gate=asyncio.Event())
        async with _serving(
            deps=deps,
            container=container,
            identity=identity,
            engines={_AGENT: engine},
            health_cache_ttl_ms=5000,
        ) as (_app, client):
            body = (await client.get(f"{_PREFIX}/{_AGENT}/health")).json()

            assert (body["status"], body["detail"]) == ("degraded", "probing")

    async def test_no_bloquea_cuando_la_primera_sonda_no_ha_terminado(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """The scrape never waits on the probe: it answers from the cache."""
        engine = ScriptedEngine(health_gate=asyncio.Event())
        loop = asyncio.get_running_loop()
        async with _serving(
            deps=deps,
            container=container,
            identity=identity,
            engines={_AGENT: engine},
            health_cache_ttl_ms=5000,
        ) as (_app, client):
            started = loop.time()
            await client.get(f"{_PREFIX}/{_AGENT}/health")
            elapsed = loop.time() - started

            assert elapsed < 0.050, f"the health scrape blocked for {elapsed:.3f}s"


_APP_MODULE = "loom_ai_containment_fixture_app"

_APP_SOURCE = '''\
"""Minimal discoverable app used by the ``loom.ai`` import-containment test."""

from __future__ import annotations

from typing import Any

from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface, RestRoute


class ContainmentRecord(BaseModel):
    __tablename__ = "ai_containment_records_fixture"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=50)


class ContainmentPingUseCase(UseCase[ContainmentRecord, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "pong"


class ContainmentPingInterface(RestInterface[str]):
    prefix = "/containment-ping"
    routes = (RestRoute(use_case=ContainmentPingUseCase, method="GET", path="/"),)
'''

_CONTAINMENT_SCRIPT = """
import sys

from loom.rest.fastapi.auto import create_app, describe_fastapi_app

app = create_app(sys.argv[1])
description = describe_fastapi_app(app)
if "agents" in description:
    raise SystemExit("an app with no ai: section described an 'agents' section")
leaked = sorted(name for name in sys.modules if name == "loom.ai" or name.startswith("loom.ai."))
if leaked:
    raise SystemExit("loom.ai leaked into an app with no ai: section: " + ", ".join(leaked))
"""


def test_no_importa_loom_ai_cuando_la_config_no_tiene_seccion_ai(tmp_path: Path) -> None:
    """Neither building nor describing an app without ``ai:`` imports ``loom.ai`` (T098, SC-013).

    Covers both routes into the pillar — ``create_app`` and the
    post-construction ``describe_fastapi_app`` — and runs in a clean
    interpreter so modules imported by unrelated suites inside the pytest
    process cannot mask the leak.
    """
    (tmp_path / f"{_APP_MODULE}.py").write_text(_APP_SOURCE, encoding="utf-8")
    config = {
        "app": {
            "name": "ai-containment-demo",
            "code_path": ".",
            "discovery": {
                "mode": "interfaces",
                "interfaces": {"modules": [_APP_MODULE], "warn_recommended": False},
            },
        },
        "database": {"url": "sqlite+aiosqlite:///"},
    }
    config_path = tmp_path / "app.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(_SRC), str(tmp_path)])}

    result = subprocess.run(
        [sys.executable, "-c", _CONTAINMENT_SCRIPT, str(config_path)],
        capture_output=True,
        text=True,
        check=False,
        env=env,
        cwd=str(tmp_path),
    )

    assert result.returncode == 0, result.stderr


_A2A_PREFIX = "/a2a"
_A2A_BASE_URL = "https://api.example.com"


class TestExclusionesDeAutenticacion:
    """Only the card may be excluded from authentication (T141, FR-041b)."""

    @staticmethod
    async def _bind(
        *,
        deps: StubDepsFactory,
        container: LoomContainer,
        exclude_paths: Sequence[str],
    ) -> None:
        """Bind the A2A surface of one agent with the given exclusion list."""
        config = make_ai_config(
            endpoints={_AGENT: make_endpoint()},
            a2a=A2AConfig(base_url=_A2A_BASE_URL, expose=(_AGENT,)),
        )
        plans = [make_plan(_AGENT)]
        runtime = AgentRuntime(
            plans=plans,
            config=config,
            engine_provider=CountingEngineProvider(),  # type: ignore[arg-type]
            deps=deps,
            container=container,
        )
        async with runtime:
            app = FastAPI()
            app.add_middleware(
                AuthenticationMiddleware,
                authenticator=StubAuthenticator(),  # type: ignore[arg-type]
                exclude_paths=tuple(exclude_paths),
            )
            bind_a2a_endpoints(
                app,
                runtime=runtime,
                config=config,
                plans=plans,
                authenticator=StubAuthenticator(),  # type: ignore[arg-type]
                exclude_paths=tuple(exclude_paths),
                prefix=_A2A_PREFIX,
            )

    @pytest.mark.parametrize(
        "excluded",
        [_A2A_PREFIX, f"{_A2A_PREFIX}/{_AGENT}", _PREFIX, f"{_PREFIX}/{_AGENT}/run"],
    )
    async def test_falla_al_arrancar_cuando_la_exclusion_cubre_una_invocacion(
        self, deps: StubDepsFactory, container: LoomContainer, excluded: str
    ) -> None:
        """Any exclusion under the A2A or agents prefix other than a card aborts start-up."""
        with pytest.raises(AgentCompilationError) as raised:
            await self._bind(deps=deps, container=container, exclude_paths=(excluded,))

        assert raised.value.issues[0].code is AgentErrorCode.AUTH_EXCLUSION_OVERLAPS_AGENTS
        assert excluded in raised.value.issues[0].message

    async def test_arranca_cuando_la_unica_exclusion_es_la_card(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The card path is the one exclusion this surface accepts."""
        await self._bind(
            deps=deps,
            container=container,
            exclude_paths=(card_path(_AGENT, prefix=_A2A_PREFIX),),
        )

    async def test_arranca_cuando_la_exclusion_no_toca_los_agentes(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """Exclusions outside both prefixes are none of this surface's business."""
        await self._bind(deps=deps, container=container, exclude_paths=("/health", "/docs"))
