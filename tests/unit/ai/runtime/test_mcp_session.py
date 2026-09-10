"""La sesion declara si necesita serializacion; el runtime deja de decidirlo.

``mcp_session_for`` es el unico punto que decide si envuelve una sesion en
``SharedMcpSession``: lo hace por defecto, y deja la sesion tal cual cuando
esta declara :class:`~loom.ai.abc.ConcurrentMcpSession`. Estos tests miden lo
que importa -- que varias llamadas concurrentes sobre una sesion declarada
corren de verdad en paralelo, no solo que el candado desaparece -- y fijan lo
contrario: una sesion sin declarar sigue serializando. Cubren tambien la
cancelacion en ambas formas, que no se comporta igual (B1): ``SharedMcpSession``
sigue drenando la llamada cancelada antes de soltar su candado, para que sus
vecinas en la misma sesion sigan siendo utilizables; una sesion declarada
concurrente no sostiene ningun candado que proteger, asi que cancelarla vuelve
al llamante de inmediato en vez de drenar.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from types import CoroutineType
from typing import Any

from loom.ai.abc import ConcurrentMcpSession, McpSession, McpToolCallResult, McpToolInfo
from loom.ai.runtime._mcp import SharedMcpSession, mcp_session_for

_CALL_DELAY = 0.05
"""One call's own duration; long enough that N serial calls are measurably
slower than N concurrent ones, short enough to keep the suite fast."""


class _DelayedSession:
    """A plain ``McpSession`` double: declares nothing, so it keeps queuing."""

    def __init__(self, *, delay: float = _CALL_DELAY) -> None:
        self._delay = delay
        self.finished: list[str] = []

    async def list_tools(self) -> tuple[McpToolInfo, ...]:
        return ()

    async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> McpToolCallResult:
        await asyncio.sleep(self._delay)
        self.finished.append(name)
        return McpToolCallResult(ok=True, structured=None)


class _CountingSession:
    """Counts every ``call_tool`` invocation -- i.e. every coroutine built.

    ``call_tool`` is a plain function returning a coroutine, not an
    ``async def`` itself: calling it builds and returns the coroutine
    object synchronously, without running any of its body, so the counter
    must go up in ``call_tool`` itself rather than in the coroutine it
    hands back. Bumping it inside an ``async def`` body would only go up
    once the coroutine is awaited -- exactly what must not happen for a
    caller still queued behind :class:`SharedMcpSession`'s lock, and
    exactly the bug a doubly-anticipatory coroutine would hide.
    """

    def __init__(self) -> None:
        self.built = 0

    async def list_tools(self) -> tuple[McpToolInfo, ...]:
        return ()

    def call_tool(
        self, name: str, arguments: Mapping[str, Any]
    ) -> CoroutineType[Any, Any, McpToolCallResult]:
        self.built += 1
        return self._run()

    async def _run(self) -> McpToolCallResult:
        await asyncio.sleep(_CALL_DELAY)
        return McpToolCallResult(ok=True, structured=None)


class _DelayedConcurrentSession(_DelayedSession, ConcurrentMcpSession):
    """The same double, declaring itself already safe for concurrent calls.

    Inherits :meth:`_DelayedSession.call_tool` unchanged: a session declaring
    :class:`~loom.ai.abc.ConcurrentMcpSession` must **not** shield a call from
    its own caller's cancellation (B1) — there is no shared frame left to
    desynchronise, so nothing here should drain.
    """


async def _call_many(session: McpSession, count: int) -> float:
    start = time.monotonic()
    await asyncio.gather(*(session.call_tool(f"tool{i}", {}) for i in range(count)))
    return time.monotonic() - start


class TestLaDeclaracionDeConcurrencia:
    """``mcp_session_for`` lee la declaracion de la sesion, no decide por ella."""

    async def test_una_sesion_que_declara_concurrencia_queda_sin_envolver(self) -> None:
        session = _DelayedConcurrentSession()
        assert mcp_session_for(session, label="crm") is session

    async def test_una_sesion_que_no_declara_nada_se_envuelve_en_shared(self) -> None:
        session = _DelayedSession()
        wrapped = mcp_session_for(session, label="crm")
        assert isinstance(wrapped, SharedMcpSession)
        assert wrapped is not session


class TestElParalelismoDeVerdad:
    """El criterio de exito no es que el candado desaparezca: es que N llamadas
    concurrentes sobre una concesion corran de verdad en paralelo. El lado
    "declarada" de ese criterio (O3) no lo mide un doble sin candado propio
    -- ``_DelayedConcurrentSession`` corre en paralelo por construccion, y
    nada de produccion puede ponerlo en rojo -- sino el camino real, que
    atraviesa ``mcp_session_for`` y ``_ToolsetSession``
    (``tests/unit/ai/engines/test_pydantic_ai_mcp_concurrency.py``)."""

    async def test_una_sesion_sin_declarar_sigue_serializando(self) -> None:
        session = _DelayedSession()
        wrapped = mcp_session_for(session, label="crm")
        elapsed = await _call_many(wrapped, 8)
        # Ocho llamadas en fila se parecen a ocho, no a una.
        assert elapsed > _CALL_DELAY * 6


class TestElDrenadoAlCancelar:
    """Una llamada cancelada a mitad de ``SharedMcpSession`` no debe
    desincronizar a sus vecinas: se drena antes de soltar el candado. Una
    sesion declarada concurrente no sostiene ningun candado que proteger, asi
    que su cancelacion vuelve al llamante de inmediato en lugar de drenar
    (B1)."""

    async def test_shared_mcp_session_drena_la_llamada_cancelada(self) -> None:
        session = _DelayedSession(delay=0.1)
        shared = SharedMcpSession(session, label="crm")
        task = asyncio.create_task(shared.call_tool("victim", {}))
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("expected the cancelled call to re-raise")
        # Drenada antes de soltar el candado: ya deberia constar como terminada.
        assert session.finished == ["victim"]
        # Y el candado sigue usable para la siguiente vecina.
        await shared.call_tool("neighbour", {})
        assert session.finished == ["victim", "neighbour"]

    async def test_sesion_concurrente_no_drena_al_cancelar(self) -> None:
        session = _DelayedConcurrentSession(delay=0.1)
        task = asyncio.create_task(session.call_tool("victim", {}))
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("expected the cancelled call to re-raise")
        # Orden, no reloj: sin candado que sostener, la cancelacion ya
        # regreso al llamante -- y sin escudo, la llamada abandonada nunca
        # llega a completarse.
        assert session.finished == []

    async def test_una_vecina_concurrente_no_se_bloquea_cuando_cancelan_a_la_otra(self) -> None:
        session = _DelayedConcurrentSession(delay=0.1)
        start = time.monotonic()
        victim = asyncio.create_task(session.call_tool("victim", {}))
        neighbour = asyncio.create_task(session.call_tool("neighbour", {}))
        await asyncio.sleep(0.01)
        victim.cancel()
        await neighbour
        elapsed = time.monotonic() - start
        # La vecina corria en paralelo desde el principio: termina alrededor de
        # su propio delay, no del doble -- que es lo que tardaria si hubiese
        # quedado en cola detras del drenado de la cancelada.
        assert elapsed < 0.15
        try:
            await victim
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("expected the cancelled call to re-raise")


class TestLaCorutinaSeConstruyeConElCandadoYaTomado:
    """Un llamante cancelado mientras espera el turno no debe dejar una
    corutina propia sin ejecutar nunca (H6): ``SharedMcpSession`` solo
    construye la llamada del siguiente turno una vez que ya tiene el
    candado."""

    async def test_el_llamante_en_cola_no_construye_su_corutina_si_lo_cancelan(self) -> None:
        session = _CountingSession()
        shared = SharedMcpSession(session, label="crm")
        holder = asyncio.create_task(shared.call_tool("holder", {}))
        await asyncio.sleep(0)
        queued = asyncio.create_task(shared.call_tool("queued", {}))
        await asyncio.sleep(0)
        # ``queued`` sigue esperando el candado: cancelarlo aqui no debe
        # haber construido ya su propia corutina de ``call_tool``.
        queued.cancel()
        try:
            await queued
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("expected the queued call to re-raise")
        await holder
        # Solo la que sostuvo el candado llego a construir su corutina.
        assert session.built == 1
