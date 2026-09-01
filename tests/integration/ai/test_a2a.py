"""Inbound A2A surface: card, JSON-RPC methods and streaming parity (T135, T136).

Drives ``bind_a2a_endpoints`` over a live :class:`AgentRuntime` backed by
scripted engines, asserting ``specs/001-ai-agent-layer/contracts/a2a.md``:
the published card validates against the A2A SDK, the streamed events are the
same union the HTTP surface projects (FR-039a), the capability wiring never
reaches the wire (FR-030a), unsupported methods answer a named JSON-RPC error,
and only the card is anonymous (FR-041b).

The application is wired with the real
:class:`~loom.rest.auth.middleware.AuthenticationMiddleware`, so the exclusion
``bind_a2a_endpoints`` registers is exercised end to end instead of simulated.
Nothing here touches the network: engines replay a script and every request
goes through an in-process ASGI transport.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import Any, cast

import httpx
import pytest
from a2a.compat.v0_3.types import AgentCard
from fastapi import FastAPI

from loom.ai.a2a.card import card_path
from loom.ai.a2a.server import bind_a2a_endpoints
from loom.ai.abc import (
    AgentEvent,
    FinalEvent,
    HealthStatus,
    TextDeltaEvent,
    ToolCallEvent,
    ToolResultEvent,
)
from loom.ai.compiler._plan import AgentPlan
from loom.ai.config import A2AConfig, AgentEndpointConfig
from loom.ai.errors import AgentCompilationError, AgentErrorCode, AgentRunErrorCode
from loom.ai.fastapi.endpoints import bind_agent_endpoints
from loom.ai.runtime import AgentRuntime
from loom.core.config.errors import ConfigError
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from loom.rest.auth.abc import RequestCredentials
from loom.rest.auth.middleware import AuthenticationMiddleware
from tests.integration.ai.conftest import (
    DEFAULT_OUTPUT,
    DEFAULT_USAGE,
    CountingEngineProvider,
    ScriptedEngine,
    StubDepsFactory,
    error_script,
    make_ai_config,
    make_endpoint,
    make_plan,
)

_AGENT = "analyst"
_PREFIX = "/a2a"
_AGENTS_PREFIX = "/agents"
_BASE_URL = "https://api.example.com"
_TOKEN = "let-me-in"

# Distinctive strings the redaction assertions look for: none of them may ever
# reach an external caller (FR-030a).
_SECRET_TOOL = "reporting_warehouse_query"
_SECRET_ARGUMENT = "SELECT margin FROM internal_pricing"
_SECRET_SUMMARY = "17 rows from internal_pricing"

# Failure texts an external caller must never see: the first is the shape
# ``AgentRunError`` carries (capability key plus budget), the second the shape
# an unhandled exception carries (file path and DSN).
_FAILING_CODE = AgentRunErrorCode.TOOL_TIMEOUT
_RUN_CANARY = "tool 'usecase_pricing.recalculate' exceeded tool_timeout_ms (5000)"
_CATCH_ALL_CANARY = "/srv/loom/secrets.yaml: clickhouse://root:hunter2@analytics.internal"


class ExplodingEngine:
    """Engine double whose stream fails with an exception nothing anticipates.

    Its message carries the shapes an unhandled exception typically leaks, so
    the catch-all's redaction is observable end to end rather than asserted on
    a stub error string.
    """

    def run_stream(self, prompt: str, *, identity: Identity) -> Any:
        """Return a stream whose entry raises before any event exists."""
        del prompt, identity
        return _FailingStream()

    async def run(self, prompt: str, *, identity: Identity) -> object:
        """Fail the same way the stream does; the runtime drives the stream."""
        del prompt, identity
        raise RuntimeError(_CATCH_ALL_CANARY)

    async def health(self) -> HealthStatus:
        """Report a healthy engine: the failure is per run, not per probe."""
        return HealthStatus(status="ok")


class _FailingStream:
    """Engine session whose entry raises, like a broken provider connection."""

    async def __aenter__(self) -> AsyncIterator[AgentEvent]:
        raise RuntimeError(_CATCH_ALL_CANARY)

    async def __aexit__(self, *exc_info: object) -> None:
        return None


class HeaderAuthenticator:
    """Authenticator accepting exactly one bearer token, refusing everything else."""

    name = "jwt"
    provides_roles = True

    async def authenticate(self, credentials: RequestCredentials) -> Identity | None:
        """Return the fixed caller when the expected bearer token is present."""
        if credentials.header("authorization") != f"Bearer {_TOKEN}":
            return None
        return Identity(subject="stub-user", roles=("analyst",), mechanism=self.name)


def tool_script() -> tuple[AgentEvent, ...]:
    """Return a run exercising every member of the event union."""
    return (
        TextDeltaEvent(text="Demand rose "),
        ToolCallEvent(tool=_SECRET_TOOL, call_id="call-1", arguments={"query": _SECRET_ARGUMENT}),
        ToolResultEvent(call_id="call-1", ok=True, summary=_SECRET_SUMMARY),
        FinalEvent(output=DEFAULT_OUTPUT, usage=DEFAULT_USAGE),
    )


@asynccontextmanager
async def _serving(
    *,
    deps: StubDepsFactory,
    container: LoomContainer,
    plans: Sequence[AgentPlan] | None = None,
    engines: Mapping[str, ScriptedEngine] | None = None,
    endpoints: Mapping[str, AgentEndpointConfig] | None = None,
    a2a: A2AConfig | None = None,
    mounted_exclusions: Sequence[str] = (),
    declared_exclusions: Sequence[str] = (),
    max_prompt_bytes: int = 65536,
    with_http_endpoints: bool = False,
) -> AsyncIterator[tuple[FastAPI, httpx.AsyncClient]]:
    """Serve an entered runtime behind the real authentication middleware.

    ``mounted_exclusions`` are the exclusions the deployment really mounts on
    the middleware; ``declared_exclusions`` are the ones the caller repeats in
    the ``exclude_paths`` argument of ``bind_a2a_endpoints``. They are separate
    on purpose: the guard must fail on the mounted list alone.
    """
    plans = list(plans if plans is not None else (make_plan(_AGENT),))
    config = make_ai_config(
        endpoints=dict(endpoints if endpoints is not None else {_AGENT: make_endpoint()}),
        a2a=a2a if a2a is not None else A2AConfig(base_url=_BASE_URL, expose=(_AGENT,)),
        max_prompt_bytes=max_prompt_bytes,
    )
    runtime = AgentRuntime(
        plans=plans,
        config=config,
        engine_provider=CountingEngineProvider(engines=engines),  # type: ignore[arg-type]
        deps=deps,
        container=container,
    )
    authenticator = HeaderAuthenticator()
    async with runtime:
        app = FastAPI()
        app.add_middleware(
            AuthenticationMiddleware,
            authenticator=authenticator,  # type: ignore[arg-type]
            exclude_paths=tuple(mounted_exclusions),
        )
        if with_http_endpoints:
            bind_agent_endpoints(
                app,
                runtime=runtime,
                config=config,
                authenticator=authenticator,  # type: ignore[arg-type]
                prefix=_AGENTS_PREFIX,
            )
        bind_a2a_endpoints(
            app,
            runtime=runtime,
            config=config,
            plans=plans,
            authenticator=authenticator,  # type: ignore[arg-type]
            exclude_paths=tuple(declared_exclusions),
            prefix=_PREFIX,
        )
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://agents.test") as client:
            yield app, client


def _runtime(
    *,
    deps: StubDepsFactory,
    container: LoomContainer,
    config: Any,
    plans: Sequence[AgentPlan],
) -> AgentRuntime:
    """Build a runtime over scripted engines for one configuration."""
    return AgentRuntime(
        plans=list(plans),
        config=config,
        engine_provider=CountingEngineProvider(),  # type: ignore[arg-type]
        deps=deps,
        container=container,
    )


async def _bind_a2a(
    *,
    deps: StubDepsFactory,
    container: LoomContainer,
    mounted_exclusions: Sequence[str] = (),
    agents_prefix: str,
) -> None:
    """Bind the A2A surface of one agent under a chosen HTTP agents prefix."""
    config = make_ai_config(
        endpoints={_AGENT: make_endpoint()},
        a2a=A2AConfig(base_url=_BASE_URL, expose=(_AGENT,)),
    )
    plans = [make_plan(_AGENT)]
    authenticator = HeaderAuthenticator()
    runtime = _runtime(deps=deps, container=container, config=config, plans=plans)
    async with runtime:
        app = FastAPI()
        app.add_middleware(
            AuthenticationMiddleware,
            authenticator=authenticator,  # type: ignore[arg-type]
            exclude_paths=tuple(mounted_exclusions),
        )
        bind_a2a_endpoints(
            app,
            runtime=runtime,
            config=config,
            plans=plans,
            authenticator=authenticator,  # type: ignore[arg-type]
            prefix=_PREFIX,
            agents_prefix=agents_prefix,
        )


@asynccontextmanager
async def _serving_anonymously(
    *,
    deps: StubDepsFactory,
    container: LoomContainer,
    endpoints: Mapping[str, AgentEndpointConfig],
) -> AsyncIterator[httpx.AsyncClient]:
    """Serve the A2A surface of an application configuring no authentication.

    No authenticator means no middleware either, which is the only wiring where
    an anonymous caller reaches the endpoint at all: with one mounted, the
    middleware refuses the request before any agent code runs.
    """
    config = make_ai_config(
        endpoints=dict(endpoints),
        a2a=A2AConfig(base_url=_BASE_URL, expose=(_AGENT,)),
    )
    plans = [make_plan(_AGENT)]
    runtime = _runtime(deps=deps, container=container, config=config, plans=plans)
    async with runtime:
        app = FastAPI()
        bind_a2a_endpoints(app, runtime=runtime, config=config, plans=plans, prefix=_PREFIX)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://agents.test") as client:
            yield client


def _rpc(method: str, *, prompt: str = "hola", request_id: int | str = 7) -> dict[str, Any]:
    """Build one JSON-RPC request body carrying a single text part."""
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
        "params": {
            "message": {
                "role": "user",
                "kind": "message",
                "messageId": "m-1",
                "parts": [{"kind": "text", "text": prompt}],
            }
        },
    }


def _auth() -> dict[str, str]:
    return {"authorization": f"Bearer {_TOKEN}"}


def _route_paths(app: FastAPI) -> set[str]:
    return {str(getattr(route, "path", "")) for route in app.routes}


def _stream_results(payload: str) -> list[dict[str, Any]]:
    """Return the ``result`` object of every JSON-RPC frame in an SSE payload."""
    return [
        json.loads(line[len("data: ") :])["result"]
        for line in payload.splitlines()
        if line.startswith("data: ")
    ]


def _sse_names(payload: str) -> list[str]:
    """Return the event names of an HTTP SSE payload, ignoring comment frames."""
    return [line[len("event: ") :] for line in payload.splitlines() if line.startswith("event: ")]


class TestCard:
    """The published card is a valid A2A card and says nothing about the wiring."""

    async def test_valida_contra_el_sdk_cuando_publica_la_card(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """T135: the served card validates against the A2A SDK's own model.

        ``a2a.compat.v0_3.types.AgentCard`` is the pydantic model of the SDK's
        JSON representation, validating the exact camelCase document served on
        the wire.  ``a2a.utils.parse_agent_card`` is deliberately not used: it
        goes through protobuf and rewrites legacy 0.3 fields, so a card could
        "pass" only because the parser edited it first.
        """
        async with _serving(deps=deps, container=container) as (_app, client):
            response = await client.get(card_path(_AGENT, prefix=_PREFIX))

        card = AgentCard.model_validate(response.json())
        assert card.name == _AGENT
        assert card.url == f"{_BASE_URL}{_PREFIX}/{_AGENT}"
        assert card.capabilities.streaming is True

    async def test_no_publica_el_cableado_cuando_publica_la_card(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """Instructions and capability wiring never appear in the card."""
        async with _serving(deps=deps, container=container) as (_app, client):
            response = await client.get(card_path(_AGENT, prefix=_PREFIX))

        assert "answer" not in response.text
        assert "fake-model" not in response.text

    async def test_sirve_la_card_sin_credenciales_cuando_hay_autenticacion(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The card path is the sole authentication exclusion (FR-041b)."""
        async with _serving(deps=deps, container=container) as (_app, client):
            response = await client.get(card_path(_AGENT, prefix=_PREFIX))

        assert response.status_code == 200

    async def test_responde_401_cuando_invocan_sin_credenciales(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The invocation path is never excluded, whatever the card allows."""
        async with _serving(deps=deps, container=container) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}", json=_rpc("message/send"))

        assert response.status_code == 401


class TestOpcionalidad:
    """With no ``ai.a2a`` section nothing at all is published (T149, FR-041)."""

    async def test_no_monta_rutas_cuando_no_hay_seccion_a2a(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """No section means no card route and no invocation route."""
        config = make_ai_config(endpoints={_AGENT: make_endpoint()})
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
            bind_a2a_endpoints(app, runtime=runtime, config=config, plans=plans)

        assert not [path for path in _route_paths(app) if path.startswith(_PREFIX)]

    async def test_no_publica_un_agente_sin_plan_compilado(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A name in ``expose`` with no compiled agent is not published."""
        a2a = A2AConfig(base_url=_BASE_URL, expose=(_AGENT, "ghost"))
        async with _serving(deps=deps, container=container, a2a=a2a) as (app, _client):
            paths = _route_paths(app)

        assert f"{_PREFIX}/ghost" not in paths
        assert f"{_PREFIX}/{_AGENT}" in paths


class TestMetodos:
    """``message/send`` runs the agent; every other method answers explicitly."""

    async def test_devuelve_una_tarea_terminal_cuando_envia_un_mensaje(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """``message/send`` answers a task already in a terminal state."""
        async with _serving(deps=deps, container=container) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("message/send"), headers=_auth()
            )

        body = response.json()
        assert body["id"] == 7
        assert body["result"]["status"]["state"] == "completed"
        assert body["result"]["artifacts"][0]["parts"][0]["data"] == DEFAULT_OUTPUT

    @pytest.mark.parametrize(
        "method",
        [
            "tasks/get",
            "tasks/list",
            "tasks/cancel",
            "tasks/resubscribe",
            "tasks/pushNotificationConfig/set",
        ],
    )
    async def test_responde_error_explicito_cuando_el_metodo_no_esta_soportado(
        self, deps: StubDepsFactory, container: LoomContainer, method: str
    ) -> None:
        """Unsupported methods are HTTP 200 with a ``-32004`` naming the method."""
        async with _serving(deps=deps, container=container) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}", json=_rpc(method), headers=_auth())

        body = response.json()
        assert response.status_code == 200
        assert body["error"]["code"] == -32004
        assert body["error"]["data"]["method"] == method
        assert body["id"] == 7

    async def test_responde_method_not_found_cuando_el_metodo_es_desconocido(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """An unknown method is ``-32601``, echoing the request id."""
        async with _serving(deps=deps, container=container) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("agent/teleport"), headers=_auth()
            )

        assert response.json()["error"]["code"] == -32601

    async def test_responde_parse_error_cuando_el_cuerpo_no_es_json(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """An unreadable body is ``-32700``."""
        async with _serving(deps=deps, container=container) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", content=b"{not json", headers=_auth()
            )

        assert response.json()["error"]["code"] == -32700

    async def test_responde_invalid_request_cuando_falta_el_metodo(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A JSON body that is not a JSON-RPC request is ``-32600``."""
        async with _serving(deps=deps, container=container) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json={"jsonrpc": "2.0", "id": 3}, headers=_auth()
            )

        body = response.json()
        assert body["error"]["code"] == -32600
        assert body["id"] == 3

    async def test_responde_invalid_params_cuando_no_hay_parte_de_texto(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A message with no text part is ``-32602``."""
        request = _rpc("message/send")
        request["params"]["message"]["parts"] = [{"kind": "data", "data": {"a": 1}}]
        async with _serving(deps=deps, container=container) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}", json=request, headers=_auth())

        assert response.json()["error"]["code"] == -32602


class TestStreaming:
    """``message/stream`` projects the same union the HTTP surface projects."""

    async def test_emite_la_tarea_inicial_y_los_eventos_proyectados(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The stream opens with the task and closes on a final status update."""
        engines = {_AGENT: ScriptedEngine(script=tool_script())}
        async with _serving(deps=deps, container=container, engines=engines) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("message/stream"), headers=_auth()
            )

        results = _stream_results(response.text)
        assert [result["kind"] for result in results] == [
            "task",
            "artifact-update",
            "status-update",
            "status-update",
            "artifact-update",
            "status-update",
        ]
        assert results[-1]["final"] is True

    async def test_no_publica_la_capability_cuando_el_run_usa_una_herramienta(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """Neither the capability key, its arguments nor the summary reach the wire."""
        engines = {_AGENT: ScriptedEngine(script=tool_script())}
        async with _serving(deps=deps, container=container, engines=engines) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("message/stream"), headers=_auth()
            )

        assert _SECRET_TOOL not in response.text
        assert _SECRET_ARGUMENT not in response.text
        assert _SECRET_SUMMARY not in response.text
        assert "call-1" not in response.text

    async def test_publica_solo_el_ordinal_opaco_cuando_el_run_usa_una_herramienta(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A tool call publishes ``step n/m`` and nothing else."""
        engines = {_AGENT: ScriptedEngine(script=tool_script())}
        async with _serving(deps=deps, container=container, engines=engines) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("message/stream"), headers=_auth()
            )

        working = _stream_results(response.text)[2]
        assert working["status"]["message"]["parts"] == [{"kind": "text", "text": "step 1/8"}]

    async def test_proyecta_los_mismos_eventos_que_la_superficie_http(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """T136: both surfaces are projections of one union (FR-039a).

        The same scripted run is served over ``/agents/{name}/stream`` and over
        ``message/stream``; the events correspond one for one, with the single
        documented difference that ``final`` projects to two A2A events.
        """
        engines = {_AGENT: ScriptedEngine(script=tool_script())}
        async with _serving(
            deps=deps, container=container, engines=engines, with_http_endpoints=True
        ) as (_app, client):
            http = await client.post(
                f"{_AGENTS_PREFIX}/{_AGENT}/stream", json={"prompt": "hola"}, headers=_auth()
            )
            a2a = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("message/stream"), headers=_auth()
            )

        # The leading frame is the A2A task envelope, which has no HTTP twin.
        projected = [result["kind"] for result in _stream_results(a2a.text)[1:]]
        assert _sse_names(http.text) == ["text_delta", "tool_call", "tool_result", "final"]
        assert projected == [
            "artifact-update",
            "status-update",
            "status-update",
            "artifact-update",
            "status-update",
        ]


class TestRedaccionDeFallos:
    """A failed run publishes its stable code and nothing else (T151 B1, B2)."""

    async def test_no_devuelve_el_mensaje_del_fallo_cuando_el_run_falla(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """``message/send`` never echoes ``AgentRunError.message``.

        The failure text names the capability key and its budget — the very
        wiring the card and the stream redact — so publishing it on the other
        method of the same endpoint would be a leak beside a guarantee.
        """
        engines = {_AGENT: ScriptedEngine(script=error_script(_FAILING_CODE, _RUN_CANARY))}
        async with _serving(deps=deps, container=container, engines=engines) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("message/send"), headers=_auth()
            )

        assert _RUN_CANARY not in response.text
        assert "usecase_pricing" not in response.text

    async def test_devuelve_el_codigo_estable_cuando_el_run_falla(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The caller still gets the code, plus a fixed catalogue detail."""
        engines = {_AGENT: ScriptedEngine(script=error_script(_FAILING_CODE, _RUN_CANARY))}
        async with _serving(deps=deps, container=container, engines=engines) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("message/send"), headers=_auth()
            )

        error = response.json()["error"]
        assert error["code"] == -32603
        assert error["data"] == {
            "code": str(_FAILING_CODE),
            "detail": "a capability call exceeded its time limit",
        }

    async def test_no_devuelve_el_texto_de_la_excepcion_cuando_el_fallo_es_inesperado(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The catch-all answers a fixed message, like the HTTP surface does.

        An unanticipated exception carries file paths, DSNs and credential
        references in its text; none of it may reach an external caller.
        """
        engines = cast("Mapping[str, ScriptedEngine]", {_AGENT: ExplodingEngine()})
        async with _serving(deps=deps, container=container, engines=engines) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("message/send"), headers=_auth()
            )

        assert _CATCH_ALL_CANARY not in response.text
        assert response.json()["error"]["data"]["detail"] == "An unexpected error occurred"


class TestExclusionesEfectivas:
    """The FR-041b guard reads the exclusions actually mounted (T151 B3, M1)."""

    async def test_falla_al_arrancar_cuando_la_exclusion_montada_abre_la_invocacion(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A mounted exclusion aborts start-up even if the caller never declares it.

        ``exclude_paths`` defaults to empty, so validating only the argument
        would let a deployment open the invocation path by simply not repeating
        it here.
        """
        excluded = f"{_PREFIX}/{_AGENT}"
        with pytest.raises(AgentCompilationError) as raised:
            async with _serving(deps=deps, container=container, mounted_exclusions=(excluded,)) as (
                _app,
                _client,
            ):
                pass  # pragma: no cover - binding fails before the body runs

        issue = raised.value.issues[0]
        assert issue.code is AgentErrorCode.AUTH_EXCLUSION_OVERLAPS_AGENTS
        assert excluded in issue.message

    async def test_falla_al_arrancar_cuando_la_exclusion_cubre_otro_prefijo_de_agentes(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The HTTP prefix under guard is the one the deployment really mounts."""
        excluded = "/bots/analyst/run"
        with pytest.raises(AgentCompilationError):
            await _bind_a2a(
                deps=deps,
                container=container,
                mounted_exclusions=(excluded,),
                agents_prefix="/bots",
            )

    async def test_arranca_cuando_la_exclusion_cubre_el_prefijo_por_defecto_no_usado(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """With the agents mounted elsewhere, ``/agents`` is nobody's invocation path."""
        await _bind_a2a(
            deps=deps,
            container=container,
            mounted_exclusions=("/agents/analyst/run",),
            agents_prefix="/bots",
        )


class TestOptOutAnonimo:
    """Anonymity is granted by an active stanza only (T151 I1, I2)."""

    async def test_invoca_sin_credenciales_cuando_el_stanza_activo_lo_permite(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """An active stanza with ``allow_anonymous`` serves an unauthenticated caller."""
        endpoints = {_AGENT: make_endpoint(allow_anonymous=True)}
        async with _serving_anonymously(
            deps=deps, container=container, endpoints=endpoints
        ) as client:
            response = await client.post(f"{_PREFIX}/{_AGENT}", json=_rpc("message/send"))

        assert response.json()["result"]["status"]["state"] == "completed"

    async def test_falla_al_arrancar_cuando_el_stanza_desactivado_declara_anonimo(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A disabled HTTP stanza grants nothing, so its opt-out is not inherited.

        Otherwise switching the HTTP surface off while leaving
        ``allow_anonymous: true`` behind would keep anonymous invocation alive
        on the one surface published to the internet.
        """
        endpoints = {_AGENT: make_endpoint(enabled=False, allow_anonymous=True)}
        with pytest.raises(ConfigError):
            async with _serving_anonymously(
                deps=deps, container=container, endpoints=endpoints
            ) as _client:
                pass  # pragma: no cover - binding fails before the body runs

    async def test_falla_al_arrancar_cuando_publica_sin_authenticator_utilizable(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """Publishing with no authenticator and no opt-out is a dead route, not a posture."""
        endpoints = {_AGENT: make_endpoint(allow_anonymous=False)}
        with pytest.raises(ConfigError) as raised:
            async with _serving_anonymously(
                deps=deps, container=container, endpoints=endpoints
            ) as _client:
                pass  # pragma: no cover - binding fails before the body runs

        assert _AGENT in str(raised.value)


class TestAnuncioDePublicacion:
    """Start-up says exactly which agents are reachable from outside."""

    @staticmethod
    async def _publication_warning(
        *,
        deps: StubDepsFactory,
        container: LoomContainer,
        caplog: pytest.LogCaptureFixture,
        endpoints: Mapping[str, AgentEndpointConfig] | None = None,
    ) -> str:
        """Return the WARNING text emitted while publishing one agent."""
        with caplog.at_level(logging.WARNING):
            async with _serving(deps=deps, container=container, endpoints=endpoints) as (
                _app,
                _client,
            ):
                pass
        return caplog.text

    async def test_nombra_al_agente_cuando_lo_publica(
        self, deps: StubDepsFactory, container: LoomContainer, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The announcement names the agent and the URL a stranger reaches."""
        text = await self._publication_warning(deps=deps, container=container, caplog=caplog)

        assert _AGENT in text
        assert f"{_BASE_URL}{_PREFIX}/{_AGENT}" in text
        assert card_path(_AGENT, prefix=_PREFIX) in text

    async def test_no_anuncia_anonimato_cuando_el_stanza_esta_desactivado(
        self, deps: StubDepsFactory, container: LoomContainer, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A disabled stanza is announced as what it is: not an anonymous mount."""
        endpoints = {_AGENT: make_endpoint(enabled=False, allow_anonymous=True)}
        text = await self._publication_warning(
            deps=deps, container=container, caplog=caplog, endpoints=endpoints
        )

        assert "allow_anonymous=False" in text
        assert "NOT authenticated" not in text


class TestLimiteDeCuerpo:
    """The prompt cap applies to the JSON-RPC route too (FR-033a)."""

    async def test_rechaza_el_prompt_cuando_supera_el_maximo(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A prompt over ``ai.max_prompt_bytes`` is ``-32602``, never a run."""
        request = _rpc("message/send", prompt="x" * 2048)
        async with _serving(deps=deps, container=container, max_prompt_bytes=1024) as (
            _app,
            client,
        ):
            response = await client.post(f"{_PREFIX}/{_AGENT}", json=request, headers=_auth())

        assert response.json()["error"]["code"] == -32602

    async def test_rechaza_el_cuerpo_cuando_supera_el_tope(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A body far over the envelope cap is refused with 413 before it is buffered."""
        body = b'{"jsonrpc":"2.0","id":7,"method":"message/send","padding":"' + b"x" * 200_000
        async with _serving(deps=deps, container=container, max_prompt_bytes=1024) as (
            _app,
            client,
        ):
            response = await client.post(f"{_PREFIX}/{_AGENT}", content=body, headers=_auth())

        assert response.status_code == 413
        assert response.json()["code"] == "PROMPT_TOO_LARGE"
