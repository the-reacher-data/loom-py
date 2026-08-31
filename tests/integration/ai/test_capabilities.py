"""Governed capabilities (US5): containment, identity and fail-closed SQL roles.

Every test drives the **real** pydantic-ai adapter over a scripted
``FunctionModel``: no network, no credential, no provider key. The model is the
only double on the engine side; the application side (the invoker, the SQL
service) is a recorder registered in a real :class:`~loom.core.di.LoomContainer`,
so an assertion about "what the capability actually did" is an assertion about
the call that reached the application, never about prose.

Covers T119 (containment, FR-042/SC-010), T120 (identity, FR-043/FR-045),
T123 (no path to the shared ``default_role``, FR-043a), the result bounds of
FR-046b, and the capability kinds the adapter announces.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import msgspec
import pytest
from pydantic_ai import ToolReturn
from pydantic_ai.exceptions import (
    ApprovalRequired,
    CallDeferred,
    ModelHTTPError,
    ModelRetry,
)
from pydantic_ai.messages import (
    FunctionToolResultEvent,
    ModelMessage,
    ModelResponse,
    RetryPromptPart,
    ToolCallPart,
    ToolReturnPart,
)
from pydantic_ai.models import Model
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.toolsets import AbstractToolset, FunctionToolset

from loom.ai.abc import AgentEngine, ToolResultEvent
from loom.ai.compiler._plan import (
    CompiledMcpCapability,
    CompiledPythonCapability,
    CompiledSqlCapability,
    CompiledUsecaseCapability,
)
from loom.ai.declarative import PolicySpec
from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider, _capabilities
from loom.ai.engines.pydantic_ai._events import translate
from loom.ai.errors import AgentCompilationError, AgentRunErrorCode
from loom.ai.runtime import AgentRunError
from loom.core.di import LoomContainer
from loom.core.engine.compilable import Compilable
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.errors import Forbidden
from loom.core.identity import ANONYMOUS, Identity, current_identity
from loom.core.sql.abc import SqlColumn, SqlQueryResult
from loom.core.sql.config import SqlConfig, SqlConnectionConfig
from loom.core.sql.service import SqlQueryService
from loom.core.use_case.invoker import ApplicationInvoker, EntityInvoker
from loom.core.use_case.use_case import UseCase
from tests.integration.ai.conftest import make_plan, make_policies

CANARY = "SECRET-CANARY-9931"
"""Token planted in every row payload; it must never reach the model."""

CONNECTION = "reporting"
ALLOWED_ROLE = "role_reporting_reader"
SHARED_DEFAULT_ROLE = "role_shared_default"
UNRELATED_ROLE = "role_unrelated"

ANALYST = Identity(subject="user-1", roles=(ALLOWED_ROLE,), mechanism="test")
"""Authenticated caller holding exactly one allowlisted role."""

ROLELESS = Identity(subject="user-2", roles=(UNRELATED_ROLE,), mechanism="test")
"""Authenticated caller holding no allowlisted role."""


# ---------------------------------------------------------------------------
# Granted and ungranted operations
# ---------------------------------------------------------------------------


class CreateProductUseCase(UseCase[object, str]):
    """Granted operation: creates a product."""

    async def execute(self, name: str) -> str:
        """Return a confirmation for ``name``."""
        return f"created {name}"


class GetProductUseCase(UseCase[object, str]):
    """Granted operation: reads one product."""

    async def execute(self, product_id: str) -> str:
        """Return a description of ``product_id``."""
        return f"product {product_id}"


class DeleteProductUseCase(UseCase[object, str]):
    """Registered in the application but **not** granted to the agent."""

    async def execute(self, product_id: str) -> str:
        """Delete ``product_id``; the agent must never reach this."""
        return f"deleted {product_id}"


def _compile_use_cases() -> None:
    compiler = UseCaseCompiler()
    for use_case in (CreateProductUseCase, GetProductUseCase, DeleteProductUseCase):
        compiler.compile(use_case)


_compile_use_cases()

GRANTED_TOOLS = ("usecase_product_create", "usecase_product_get")
"""Tool names the two granted keys derive (design D2: ``product:create`` →
``usecase_product_create``)."""

UNGRANTED_TOOL = "usecase_product_delete"
"""Name the ungranted key *would* derive; no tool may carry it."""

SQL_TOOL = f"sql_{CONNECTION}"
"""Name of the single tool a ``sql`` capability publishes (design D3)."""


def usecase_capability() -> CompiledUsecaseCapability:
    """Grant exactly two of the three registered operations."""
    return CompiledUsecaseCapability(
        keys=("product:create", "product:get"),
        use_cases=(CreateProductUseCase, GetProductUseCase),
    )


def bound_connection(*, max_sql_bytes: int = 262144) -> SqlConnectionConfig:
    """A read-only connection that *does* carry a shared ``default_role``.

    The shared role is set on purpose: it is the exact regression FR-043a
    forbids. Without it, a capability reaching ``execute(roles=())`` would
    succeed silently and the role tests would not bite.
    """
    return SqlConnectionConfig(
        backend="clickhouse",
        url="clickhouse://reports.internal:8123/reporting",
        allowed_roles=(ALLOWED_ROLE,),
        default_role=SHARED_DEFAULT_ROLE,
        readonly=True,
        max_sql_bytes=max_sql_bytes,
    )


def sql_capability(
    *,
    max_rows: int = 1000,
    max_result_bytes: int = 1_000_000,
    max_sql_bytes: int = 262144,
    connection: str = CONNECTION,
) -> CompiledSqlCapability:
    """Grant the bound connection under explicit bounds (FR-046b)."""
    return CompiledSqlCapability(
        connection=connection,
        config=bound_connection(max_sql_bytes=max_sql_bytes),
        max_rows=max_rows,
        max_result_bytes=max_result_bytes,
    )


# ---------------------------------------------------------------------------
# Application-side recorders
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RecordedInvocation:
    """One use-case invocation as the application observed it.

    Attributes:
        use_case: Type the capability asked the invoker to run.
        params: Primitive parameters handed over.
        payload: Command payload handed over, if any.
        identity: Ambient identity *inside* the invocation (FR-043).
    """

    use_case: type[Compilable]
    params: Mapping[str, Any]
    payload: Mapping[str, Any] | None
    identity: Identity


class RecordingInvoker:
    """``ApplicationInvoker`` double recording every invocation and its caller.

    Args:
        result: Value every invocation returns.
        failure: Raised *after* the invocation is recorded, so a test can tell
            an operation that already happened from one that never ran.
    """

    def __init__(self, result: str = "done", *, failure: Exception | None = None) -> None:
        self.result = result
        self.failure = failure
        self.calls: list[RecordedInvocation] = []

    async def invoke(
        self,
        use_case: type[Compilable],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> str:
        """Record the call, the ambient identity, and return the fixed result."""
        self.calls.append(
            RecordedInvocation(
                use_case=use_case,
                params=dict(params or {}),
                payload=dict(payload) if payload is not None else None,
                identity=current_identity(),
            )
        )
        if self.failure is not None:
            raise self.failure
        return self.result

    async def invoke_name(
        self,
        key: str,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> str:
        """Not used: the capability holds resolved types, never keys."""
        raise AssertionError(f"the capability must not resolve '{key}' by name")

    def entity(self, model: type[Any]) -> EntityInvoker:
        """Not used by any capability."""
        raise AssertionError("the capability must not use the entity facade")


@dataclass(frozen=True)
class RecordedQuery:
    """One ``SqlQueryService.execute`` call as the service observed it.

    Attributes:
        sql: Statement the capability sent.
        connection: Connection name the capability named.
        roles: Roles the capability passed; ``None`` when it passed none.
        limit: Row limit the capability passed.
    """

    sql: str
    connection: str
    roles: tuple[str, ...] | None
    limit: int | None


def canary_result(rows: int) -> SqlQueryResult:
    """Build a result whose every row carries :data:`CANARY`."""
    return SqlQueryResult(
        columns=(SqlColumn(name="secret", type="String"),),
        rows=tuple((f"{CANARY}-{index}",) for index in range(rows)),
        row_count=rows,
        limit=rows,
        offset=0,
        has_more=False,
        elapsed_ms=1.0,
    )


class RecordingSqlQueryService(SqlQueryService):
    """``SqlQueryService`` double recording every ``execute`` call.

    It subclasses the real service so it resolves under the same container key
    the application registers, and it never touches an executor.

    Args:
        result: Result every call returns.
    """

    def __init__(self, result: SqlQueryResult) -> None:
        super().__init__(
            executors={}, config=SqlConfig(connections={CONNECTION: bound_connection()})
        )
        self.result = result
        self.calls: list[RecordedQuery] = []

    async def execute(
        self,
        sql: str,
        *,
        connection: str,
        roles: Sequence[str] | None = None,
        parameters: Mapping[str, Any] | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> SqlQueryResult:
        """Record the call — roles included, verbatim — and return the result."""
        del parameters, offset
        self.calls.append(
            RecordedQuery(
                sql=sql,
                connection=connection,
                roles=None if roles is None else tuple(roles),
                limit=limit,
            )
        )
        return self.result


# ---------------------------------------------------------------------------
# Engine-side scripting
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CapabilityDeps:
    """Dependency bundle satisfying the capability boundary contract (D1).

    Attributes:
        identity: Verified caller of this invocation.
        container: Application container the capability resolves from.
    """

    identity: Identity
    container: LoomContainer


class CapabilityDepsFactory:
    """Per-invocation factory producing a well-formed :class:`CapabilityDeps`."""

    def build(self, identity: Identity, container: LoomContainer) -> object:
        """Return the bundle carrying the caller and the container."""
        return CapabilityDeps(identity=identity, container=container)


class IdentitylessDepsFactory:
    """Factory producing a bundle that carries no identity at all.

    Models the deployment that wires a dependency bundle of its own: the
    capability boundary must fail closed rather than substitute ``ANONYMOUS``.
    """

    def build(self, identity: Identity, container: LoomContainer) -> object:
        """Return a bundle deliberately missing the contract's attributes."""
        del identity, container
        return object()


class ScriptedToolModel:
    """A model that issues a fixed sequence of tool calls, then answers.

    It records the tool surface it was offered and every tool return part it
    was handed, which is how a test observes containment, what a refusal
    actually shows the model, and which event the stream would carry.

    Args:
        calls: ``(tool name, arguments)`` pairs to issue, in order.
        answer: Structured answer emitted once the script is exhausted.
    """

    def __init__(
        self,
        *,
        calls: Sequence[tuple[str, Mapping[str, Any]]] = (),
        answer: Mapping[str, Any] | None = None,
    ) -> None:
        self.calls = tuple(calls)
        self.offered_tools: tuple[str, ...] = ()
        self.tool_returns: list[str] = []
        self.tool_parts: list[ToolReturnPart] = []
        self._answer = msgspec.json.encode(dict(answer or {"answer": "42"})).decode()
        self._step = 0
        self._seen: set[str] = set()

    def as_model(self) -> Model:
        """Return the ``FunctionModel`` the engine will drive."""
        return FunctionModel(self._respond)

    def _respond(self, messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        self.offered_tools = tuple(tool.name for tool in info.function_tools)
        self._collect(messages)
        if self._step < len(self.calls):
            name, arguments = self.calls[self._step]
            self._step += 1
            call_id = f"call-{self._step}"
            return ModelResponse(
                parts=[ToolCallPart(tool_name=name, args=dict(arguments), tool_call_id=call_id)]
            )
        return ModelResponse(
            parts=[ToolCallPart(tool_name=info.output_tools[0].name, args=self._answer)]
        )

    def _collect(self, messages: Sequence[ModelMessage]) -> None:
        for message in messages:
            for part in getattr(message, "parts", ()):
                if isinstance(part, ToolReturnPart) and part.tool_call_id not in self._seen:
                    self._seen.add(part.tool_call_id)
                    self.tool_returns.append(str(part.content))
                    self.tool_parts.append(part)

    @property
    def shown(self) -> str:
        """Everything the model was ever shown as a tool result, concatenated."""
        return "\n".join(self.tool_returns)


def build_engine(
    *,
    capabilities: Sequence[Any],
    model: ScriptedToolModel,
    container: LoomContainer,
    deps: object,
    policies: PolicySpec | None = None,
) -> AgentEngine:
    """Build the real adapter for a plan carrying ``capabilities``."""
    provider = PydanticAIEngineProvider(model_resolver=lambda target: model.as_model())
    plan = make_plan("analyst", capabilities=tuple(capabilities), policies=policies)
    return provider.create_engine(plan, deps=deps, container=container)  # type: ignore[arg-type]


@pytest.fixture
def invoker() -> RecordingInvoker:
    """Application invoker recording every use-case call and its caller."""
    return RecordingInvoker()


@pytest.fixture
def sql_service() -> RecordingSqlQueryService:
    """SQL service recording every ``execute`` call, roles included."""
    return RecordingSqlQueryService(canary_result(3))


@pytest.fixture
def app_container(
    invoker: RecordingInvoker, sql_service: RecordingSqlQueryService
) -> LoomContainer:
    """Container holding the recorders under the keys the application uses."""
    container = LoomContainer()
    container.register_instance(ApplicationInvoker, invoker)
    container.register_instance(SqlQueryService, sql_service)
    return container


# ---------------------------------------------------------------------------
# T119 — containment (FR-042, SC-010)
# ---------------------------------------------------------------------------


class TestContainment:
    async def test_solo_se_ofrecen_las_operaciones_concedidas_cuando_el_plan_las_declara(
        self, app_container: LoomContainer
    ) -> None:
        """Two granted keys publish two tools; the third key publishes none."""
        model = ScriptedToolModel()
        engine = build_engine(
            capabilities=(usecase_capability(),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        await engine.run("hello", identity=ANALYST)

        assert sorted(model.offered_tools) == sorted(GRANTED_TOOLS)

    async def test_la_operacion_no_concedida_no_se_invoca_cuando_el_modelo_la_pide(
        self, app_container: LoomContainer, invoker: RecordingInvoker
    ) -> None:
        """The ungranted key is unreachable even when the model names it.

        Naming a tool that was never published is model misbehaviour, not a
        capability refusal: pydantic-ai has no such tool to dispatch to, so the
        run ends in ``OUTPUT_SCHEMA_VIOLATION``.  What containment asserts is
        the other half — the ungranted operation is never invoked on any path.
        """
        model = ScriptedToolModel(calls=((UNGRANTED_TOOL, {"product_id": "p-1"}),))
        engine = build_engine(
            capabilities=(usecase_capability(),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        with pytest.raises(AgentRunError) as failure:
            await engine.run("hello", identity=ANALYST)

        assert failure.value.code is AgentRunErrorCode.OUTPUT_SCHEMA_VIOLATION
        assert invoker.calls == []


# ---------------------------------------------------------------------------
# T120 — identity (FR-043, FR-045)
# ---------------------------------------------------------------------------


class TestIdentity:
    async def test_la_capacidad_corre_bajo_la_identidad_del_llamante_cuando_se_invoca(
        self, app_container: LoomContainer, invoker: RecordingInvoker
    ) -> None:
        """The invocation observes the caller, never ``ANONYMOUS``."""
        model = ScriptedToolModel(calls=(("usecase_product_create", {"name": "widget"}),))
        engine = build_engine(
            capabilities=(usecase_capability(),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        await engine.run("hello", identity=ANALYST)

        assert [call.identity for call in invoker.calls] == [ANALYST]

    async def test_no_se_invoca_ninguna_operacion_cuando_el_llamante_es_anonimo(
        self, app_container: LoomContainer, invoker: RecordingInvoker
    ) -> None:
        """``usecase`` refuses an unauthenticated caller too (ruling R3).

        The compiler already rejects an anonymous-opt-out agent holding a
        ``usecase`` grant, so an ``ANONYMOUS`` identity arriving here means the
        transport failed to propagate a caller.  It fails closed instead of
        running the operation as nobody.
        """
        model = ScriptedToolModel(calls=(("usecase_product_create", {"name": "widget"}),))
        engine = build_engine(
            capabilities=(usecase_capability(),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        with pytest.raises(AgentRunError) as failure:
            await engine.run("hello", identity=ANONYMOUS)

        assert failure.value.code is AgentRunErrorCode.UNAUTHORIZED
        assert invoker.calls == []

    async def test_se_rechaza_cuando_el_bundle_de_deps_no_lleva_identidad(
        self, app_container: LoomContainer
    ) -> None:
        """A bundle without the contract's attributes fails closed (design D1)."""
        model = ScriptedToolModel(calls=(("usecase_product_create", {"name": "widget"}),))
        engine = build_engine(
            capabilities=(usecase_capability(),),
            model=model,
            container=app_container,
            deps=IdentitylessDepsFactory(),
        )

        with pytest.raises(AgentRunError) as failure:
            await engine.run("hello", identity=ANALYST)

        assert failure.value.code is AgentRunErrorCode.UNAUTHORIZED

    async def test_no_se_invoca_ninguna_operacion_cuando_el_bundle_no_lleva_identidad(
        self, app_container: LoomContainer, invoker: RecordingInvoker
    ) -> None:
        """The refusal happens before the application is ever touched."""
        model = ScriptedToolModel(calls=(("usecase_product_create", {"name": "widget"}),))
        engine = build_engine(
            capabilities=(usecase_capability(),),
            model=model,
            container=app_container,
            deps=IdentitylessDepsFactory(),
        )

        with pytest.raises(AgentRunError) as failure:
            await engine.run("hello", identity=ANALYST)

        assert (failure.value.code, invoker.calls) == (AgentRunErrorCode.UNAUTHORIZED, [])


# ---------------------------------------------------------------------------
# T123 — no path reaches the shared ``default_role`` (FR-043a)
# ---------------------------------------------------------------------------


class TestSqlRolesAreBoundToTheCaller:
    async def test_la_consulta_llega_con_los_roles_resueltos_cuando_el_llamante_esta_autenticado(
        self, app_container: LoomContainer, sql_service: RecordingSqlQueryService
    ) -> None:
        """The roles reaching the service are the caller's resolved, non-empty tuple."""
        model = ScriptedToolModel(calls=((SQL_TOOL, {"sql": "SELECT 1"}),))
        engine = build_engine(
            capabilities=(sql_capability(),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        await engine.run("hello", identity=ANALYST)

        assert [call.roles for call in sql_service.calls] == [(ALLOWED_ROLE,)]

    async def test_ninguna_consulta_llega_con_roles_vacios_cuando_el_agente_consulta_dos_veces(
        self, app_container: LoomContainer, sql_service: RecordingSqlQueryService
    ) -> None:
        """No recorded call carries ``None`` or ``()``: the shared default is unreachable."""
        model = ScriptedToolModel(
            calls=((SQL_TOOL, {"sql": "SELECT 1"}), (SQL_TOOL, {"sql": "SELECT 2"}))
        )
        engine = build_engine(
            capabilities=(sql_capability(),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        await engine.run("hello", identity=ANALYST)

        unbound = [call for call in sql_service.calls if not call.roles]
        assert (len(sql_service.calls), unbound) == (2, [])

    async def test_la_consulta_no_llega_al_servicio_cuando_el_llamante_es_anonimo(
        self, app_container: LoomContainer, sql_service: RecordingSqlQueryService
    ) -> None:
        """An anonymous caller is refused before ``execute`` is ever called."""
        model = ScriptedToolModel(calls=((SQL_TOOL, {"sql": "SELECT 1"}),))
        engine = build_engine(
            capabilities=(sql_capability(),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        with pytest.raises(AgentRunError) as failure:
            await engine.run("hello", identity=ANONYMOUS)

        assert (failure.value.code, sql_service.calls) == (AgentRunErrorCode.UNAUTHORIZED, [])

    async def test_la_consulta_no_llega_al_servicio_cuando_el_llamante_no_tiene_rol_permitido(
        self, app_container: LoomContainer, sql_service: RecordingSqlQueryService
    ) -> None:
        """A caller holding no allowlisted role never reaches the service."""
        model = ScriptedToolModel(calls=((SQL_TOOL, {"sql": "SELECT 1"}),))
        engine = build_engine(
            capabilities=(sql_capability(),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        with pytest.raises(AgentRunError) as failure:
            await engine.run("hello", identity=ROLELESS)

        assert (failure.value.code, sql_service.calls) == (AgentRunErrorCode.UNAUTHORIZED, [])


# ---------------------------------------------------------------------------
# FR-046b — bounds enforced before the model's context
# ---------------------------------------------------------------------------


class TestResultBounds:
    async def test_el_modelo_no_ve_ninguna_fila_cuando_el_resultado_supera_max_rows(
        self, app_container: LoomContainer
    ) -> None:
        """A result above ``max_rows`` is refused, carrying no row data."""
        service = RecordingSqlQueryService(canary_result(5))
        app_container.register_instance(SqlQueryService, service)
        model = ScriptedToolModel(calls=((SQL_TOOL, {"sql": "SELECT secret FROM t"}),))
        engine = build_engine(
            capabilities=(sql_capability(max_rows=2),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        await engine.run("hello", identity=ANALYST)

        assert (bool(model.tool_returns), CANARY in model.shown) == (True, False)

    async def test_el_modelo_no_ve_ninguna_fila_cuando_el_resultado_supera_max_result_bytes(
        self, app_container: LoomContainer
    ) -> None:
        """A result above ``max_result_bytes`` is refused, carrying no row data."""
        service = RecordingSqlQueryService(canary_result(3))
        app_container.register_instance(SqlQueryService, service)
        model = ScriptedToolModel(calls=((SQL_TOOL, {"sql": "SELECT secret FROM t"}),))
        engine = build_engine(
            capabilities=(sql_capability(max_rows=1000, max_result_bytes=16),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        await engine.run("hello", identity=ANALYST)

        assert (bool(model.tool_returns), CANARY in model.shown) == (True, False)


# ---------------------------------------------------------------------------
# Design D10 — the kinds this phase honours
# ---------------------------------------------------------------------------


class TestSupportedCapabilityKinds:
    def test_los_kinds_soportados_son_los_de_esta_fase_cuando_se_consulta_el_provider(
        self,
    ) -> None:
        """``a2a`` stays out: it is the next phase, not this one."""
        kinds = PydanticAIEngineProvider().supported_capability_kinds()

        assert kinds == frozenset({"usecase", "sql", "mcp", "skills", "python"})


# ---------------------------------------------------------------------------
# Contained application failures (security review, FIX 1 and FIX 2)
# ---------------------------------------------------------------------------


BACKEND_CANARY = "clickhouse://reports.internal:8123 relation public.salaries missing"
"""Backend detail planted in a failure; no byte of it may reach the model."""

REPLAYING_POLICIES = make_policies(retries=2)
"""The real default: a retriable failure replays the whole run twice more."""


class FlakyToolModel(ScriptedToolModel):
    """Calls one tool, then fails the run with a retriable provider outage.

    The failure is raised *after* the tool has been dispatched, which is the
    shape that makes a replay dangerous: the application operation has already
    happened when the engine decides whether to try again.
    """

    def __init__(self) -> None:
        super().__init__(calls=(("usecase_product_create", {"name": "widget"}),))
        self.responses = 0

    def _respond(self, messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        """Issue the scripted call on the first turn, then fail every later one."""
        self.responses += 1
        if self.responses > 1:
            raise ModelHTTPError(status_code=503, model_name="scripted")
        return super()._respond(messages, info)


class TestCapabilityRunsAreNotReplayed:
    async def test_la_operacion_se_ejecuta_una_sola_vez_cuando_el_proveedor_falla(
        self, app_container: LoomContainer, invoker: RecordingInvoker
    ) -> None:
        """A retriable provider outage does not replay a capability run.

        The mutation already happened when the provider failed, and nothing
        about a granted use case is idempotent or keyed, so replaying the run
        would execute it a second and a third time under the default
        ``retries=2``.  A capability-bearing agent surfaces the failure instead.
        """
        model = FlakyToolModel()
        engine = build_engine(
            capabilities=(usecase_capability(),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
            policies=REPLAYING_POLICIES,
        )

        with pytest.raises(AgentRunError):
            await engine.run("hello", identity=ANALYST)

        assert len(invoker.calls) == 1

    async def test_el_agente_sin_capacidades_conserva_sus_reintentos(
        self, app_container: LoomContainer
    ) -> None:
        """A pure-language agent has no side effect to duplicate, so it retries."""
        model = FlakyToolModel()
        engine = build_engine(
            capabilities=(),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
            policies=REPLAYING_POLICIES,
        )

        with pytest.raises(AgentRunError):
            await engine.run("hello", identity=ANALYST)

        assert model.responses > 1


class TestContainedApplicationFailures:
    async def test_la_operacion_mutante_se_ejecuta_una_sola_vez_cuando_la_aplicacion_falla(
        self, app_container: LoomContainer
    ) -> None:
        """A failing mutation is never replayed by the retry policy.

        A raw exception leaving the tool would be classified
        ``PROVIDER_UNAVAILABLE`` — retriable — and the engine would replay the
        whole run, invoking the already-executed mutation again. The invoker
        must therefore record exactly one call.
        """
        invoker = RecordingInvoker(failure=RuntimeError(BACKEND_CANARY))
        app_container.register_instance(ApplicationInvoker, invoker)
        model = ScriptedToolModel(calls=(("usecase_product_create", {"name": "widget"}),))
        engine = build_engine(
            capabilities=(usecase_capability(),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
            policies=REPLAYING_POLICIES,
        )

        await engine.run("hello", identity=ANALYST)

        assert len(invoker.calls) == 1

    async def test_el_modelo_ve_un_rechazo_generico_cuando_la_aplicacion_falla(
        self, app_container: LoomContainer
    ) -> None:
        """The refusal names the tool and a generic cause, never the backend."""
        app_container.register_instance(
            ApplicationInvoker, RecordingInvoker(failure=RuntimeError(BACKEND_CANARY))
        )
        model = ScriptedToolModel(calls=(("usecase_product_create", {"name": "widget"}),))
        engine = build_engine(
            capabilities=(usecase_capability(),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        await engine.run("hello", identity=ANALYST)

        assert ("refused" in model.shown, BACKEND_CANARY in model.shown) == (True, False)

    async def test_el_fallo_sql_no_muestra_el_detalle_del_driver_cuando_el_servicio_falla(
        self, app_container: LoomContainer
    ) -> None:
        """A driver failure is contained exactly like an application one."""

        class FailingSqlQueryService(RecordingSqlQueryService):
            async def execute(self, sql: str, **kwargs: Any) -> SqlQueryResult:
                """Fail the way a driver does, with a detailed message."""
                raise RuntimeError(BACKEND_CANARY)

        app_container.register_instance(SqlQueryService, FailingSqlQueryService(canary_result(1)))
        model = ScriptedToolModel(calls=((SQL_TOOL, {"sql": "SELECT 1"}),))
        engine = build_engine(
            capabilities=(sql_capability(),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        await engine.run("hello", identity=ANALYST)

        assert ("refused" in model.shown, BACKEND_CANARY in model.shown) == (True, False)

    async def test_la_denegacion_de_la_aplicacion_no_reejecuta_la_operacion_cuando_se_rechaza(
        self, app_container: LoomContainer
    ) -> None:
        """A ``Forbidden`` is ``UNAUTHORIZED``: an authorisation class, never retried."""
        invoker = RecordingInvoker(failure=Forbidden("product:create is not granted"))
        app_container.register_instance(ApplicationInvoker, invoker)
        model = ScriptedToolModel(calls=(("usecase_product_create", {"name": "widget"}),))
        engine = build_engine(
            capabilities=(usecase_capability(),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
            policies=REPLAYING_POLICIES,
        )

        with pytest.raises(AgentRunError) as failure:
            await engine.run("hello", identity=ANALYST)

        assert (failure.value.code, len(invoker.calls)) == (AgentRunErrorCode.UNAUTHORIZED, 1)

    async def test_la_denegacion_no_muestra_el_mensaje_de_la_aplicacion_cuando_se_rechaza(
        self, app_container: LoomContainer
    ) -> None:
        """The coded error carries loom's own wording, not the application's."""
        app_container.register_instance(
            ApplicationInvoker, RecordingInvoker(failure=Forbidden(BACKEND_CANARY))
        )
        model = ScriptedToolModel(calls=(("usecase_product_create", {"name": "widget"}),))
        engine = build_engine(
            capabilities=(usecase_capability(),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        with pytest.raises(AgentRunError) as failure:
            await engine.run("hello", identity=ANALYST)

        assert BACKEND_CANARY not in str(failure.value)


# ---------------------------------------------------------------------------
# Foreign toolsets sit behind the same boundary (security review, FIX 3)
# ---------------------------------------------------------------------------


def python_capability(calls: list[str]) -> CompiledPythonCapability:
    """Build a ``python`` grant whose single tool records every dispatch."""

    def ping() -> str:
        """Answer a fixed token, recording that the tool body actually ran."""
        calls.append("ping")
        return "pong"

    def factory(container: LoomContainer) -> AbstractToolset[Any]:
        del container
        return FunctionToolset([ping])

    return CompiledPythonCapability(
        factory_ref="tests.integration.ai.test_capabilities:factory", factory=factory
    )


def stub_mcp_server(calls: list[str]) -> Callable[[CompiledMcpCapability], AbstractToolset[Any]]:
    """Stand in for the MCP client, which is an optional dependency.

    The client is not installed in the test environment, so the remote toolset
    is replaced by a local one carrying the same tool surface: what is under
    test is loom's boundary around it, not the client.
    """

    def remote_ping() -> str:
        """Answer as a remote tool would, recording that it was reached."""
        calls.append("remote_ping")
        return "pong"

    def _server(capability: CompiledMcpCapability) -> AbstractToolset[Any]:
        del capability
        return FunctionToolset([remote_ping])

    return _server


def signalling_capability(signal: Exception) -> CompiledPythonCapability:
    """Build a ``python`` grant whose tool raises one engine control signal."""

    def ping() -> str:
        """Raise the engine signal instead of answering."""
        raise signal

    def factory(container: LoomContainer) -> AbstractToolset[Any]:
        del container
        return FunctionToolset([ping])

    return CompiledPythonCapability(
        factory_ref="tests.integration.ai.test_capabilities:factory", factory=factory
    )


def dictating_capability() -> CompiledPythonCapability:
    """Build a ``python`` grant whose tool tries to dictate its own summary."""

    def ping() -> ToolReturn:
        """Return a value carrying loom's own reserved metadata key."""
        return ToolReturn(
            return_value="pong",
            metadata={"loom": {"shape": "rows", "n": 999}, "own": "kept"},
        )

    def factory(container: LoomContainer) -> AbstractToolset[Any]:
        del container
        return FunctionToolset([ping])

    return CompiledPythonCapability(
        factory_ref="tests.integration.ai.test_capabilities:factory", factory=factory
    )


class RetryCapturingModel(ScriptedToolModel):
    """Scripted model that also records the retry prompts it was handed.

    ``ModelRetry`` reaches the model as a ``RetryPromptPart``, not as a tool
    return, so the base recorder does not see it.
    """

    def __init__(self) -> None:
        super().__init__(calls=(("ping", {}), ("ping", {})))
        self.retry_prompts: list[str] = []

    def _collect(self, messages: Sequence[ModelMessage]) -> None:
        """Record tool returns as usual, plus every retry prompt."""
        super()._collect(messages)
        for message in messages:
            for part in getattr(message, "parts", ()):
                if isinstance(part, RetryPromptPart):
                    self.retry_prompts.append(str(part.content))


class TestEngineSignalsSurviveTheGuard:
    async def test_el_modelo_recibe_la_guia_cuando_la_herramienta_pide_reintento(
        self, app_container: LoomContainer
    ) -> None:
        """``ModelRetry`` is control flow, not a failure.

        It carries the guidance the tool wrote for the model. Swallowing it into
        the generic refusal would lose that text and report a healthy protocol
        exchange as a failure.
        """
        model = RetryCapturingModel()
        engine = build_engine(
            capabilities=(signalling_capability(ModelRetry("bad argument, try X")),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
            policies=make_policies(retries=2),
        )

        await engine.run("hello", identity=ANALYST)

        assert any("bad argument, try X" in prompt for prompt in model.retry_prompts)
        assert "refused" not in model.shown

    @pytest.mark.parametrize("signal", [CallDeferred(), ApprovalRequired()])
    async def test_el_protocolo_del_motor_no_se_convierte_en_rechazo(
        self, app_container: LoomContainer, signal: Exception
    ) -> None:
        """Deferred and approval drive a protocol the engine is waiting on.

        This agent declares no deferred output type, so the engine itself
        rejects the protocol — which is the proof the signal was never
        swallowed: had the guard caught it, the model would have been shown a
        generic refusal and the run would have completed normally.
        """
        model = ScriptedToolModel(calls=(("ping", {}),))
        engine = build_engine(
            capabilities=(signalling_capability(signal),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        with pytest.raises(AgentRunError) as failure:
            await engine.run("hello", identity=ANALYST)

        assert "deferred" in str(failure.value).lower()
        assert "refused" not in model.shown


class TestForeignToolsetsCannotDictateTheirSummary:
    async def test_el_toolset_ajeno_no_dicta_el_resumen_de_su_propio_evento(
        self, app_container: LoomContainer
    ) -> None:
        """The reserved ``loom`` metadata key is stripped from a foreign return.

        Left writable, an MCP server or a third-party toolset could label its
        own call ``999 rows`` — the one thing FR-030b says the tool never
        produces. Its own metadata keys survive.
        """
        model = ScriptedToolModel(calls=(("ping", {}),))
        engine = build_engine(
            capabilities=(dictating_capability(),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        await engine.run("hello", identity=ANALYST)

        part = model.tool_parts[0]
        metadata = part.metadata or {}
        assert "loom" not in metadata
        assert metadata.get("own") == "kept"

        event = translate(FunctionToolResultEvent(part))
        assert isinstance(event, ToolResultEvent)
        assert event.summary == "ok"


class TestForeignToolsetsAreGuarded:
    async def test_la_herramienta_python_no_se_ejecuta_cuando_el_llamante_es_anonimo(
        self, app_container: LoomContainer
    ) -> None:
        """A ``python`` toolset is first-party code: it runs as the caller or not at all."""
        calls: list[str] = []
        model = ScriptedToolModel(calls=(("ping", {}),))
        engine = build_engine(
            capabilities=(python_capability(calls),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        with pytest.raises(AgentRunError) as failure:
            await engine.run("hello", identity=ANONYMOUS)

        assert (failure.value.code, calls) == (AgentRunErrorCode.UNAUTHORIZED, [])

    async def test_la_herramienta_python_se_ejecuta_cuando_el_llamante_esta_autenticado(
        self, app_container: LoomContainer
    ) -> None:
        """The guard bounds the call without breaking normal dispatch."""
        calls: list[str] = []
        model = ScriptedToolModel(calls=(("ping", {}),))
        engine = build_engine(
            capabilities=(python_capability(calls),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        await engine.run("hello", identity=ANALYST)

        assert calls == ["ping"]

    async def test_el_servidor_mcp_no_se_alcanza_cuando_el_llamante_es_anonimo(
        self, app_container: LoomContainer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An unauthenticated run must not reach a remote MCP server."""
        calls: list[str] = []
        monkeypatch.setattr(_capabilities, "_mcp_server", stub_mcp_server(calls))
        model = ScriptedToolModel(calls=(("remote_ping", {}),))
        engine = build_engine(
            capabilities=(CompiledMcpCapability(url="https://tools.internal/mcp"),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        with pytest.raises(AgentRunError) as failure:
            await engine.run("hello", identity=ANONYMOUS)

        assert (failure.value.code, calls) == (AgentRunErrorCode.UNAUTHORIZED, [])

    async def test_el_servidor_mcp_se_alcanza_cuando_el_llamante_esta_autenticado(
        self, app_container: LoomContainer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The guard leaves an authenticated MCP call working."""
        calls: list[str] = []
        monkeypatch.setattr(_capabilities, "_mcp_server", stub_mcp_server(calls))
        model = ScriptedToolModel(calls=(("remote_ping", {}),))
        engine = build_engine(
            capabilities=(CompiledMcpCapability(url="https://tools.internal/mcp"),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        await engine.run("hello", identity=ANALYST)

        assert calls == ["remote_ping"]


# ---------------------------------------------------------------------------
# Statement and row bounds (security review, FIX 5 and FIX 6)
# ---------------------------------------------------------------------------


class TestStatementBound:
    async def test_la_consulta_no_llega_al_servicio_cuando_supera_max_sql_bytes(
        self, app_container: LoomContainer, sql_service: RecordingSqlQueryService
    ) -> None:
        """``max_sql_bytes`` binds the model-authored statement, as REST binds the request."""
        model = ScriptedToolModel(calls=((SQL_TOOL, {"sql": "SELECT " + "x" * 200}),))
        engine = build_engine(
            capabilities=(sql_capability(max_sql_bytes=32),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        await engine.run("hello", identity=ANALYST)

        assert (sql_service.calls, "max_sql_bytes" in model.shown) == ([], True)


class TestRowBoundCountsTheRowsHandedOver:
    async def test_el_modelo_no_ve_ninguna_fila_cuando_row_count_contradice_a_rows(
        self, app_container: LoomContainer
    ) -> None:
        """The bound counts ``rows``, not the sibling ``row_count`` an executor computes."""
        rows = canary_result(5)
        understated = msgspec.structs.replace(rows, row_count=0)
        app_container.register_instance(SqlQueryService, RecordingSqlQueryService(understated))
        model = ScriptedToolModel(calls=((SQL_TOOL, {"sql": "SELECT secret FROM t"}),))
        engine = build_engine(
            capabilities=(sql_capability(max_rows=2),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        await engine.run("hello", identity=ANALYST)

        assert (CANARY in model.shown, "max_rows" in model.shown) == (False, True)


# ---------------------------------------------------------------------------
# Published tool names (security review, FIX 7)
# ---------------------------------------------------------------------------


class TestPublishedToolNames:
    def test_el_plan_no_compila_cuando_dos_capacidades_derivan_el_mismo_nombre(
        self, app_container: LoomContainer
    ) -> None:
        """Collisions are rejected across capabilities, not only within one."""
        colliding = (
            CompiledUsecaseCapability(keys=("product:create",), use_cases=(CreateProductUseCase,)),
            CompiledUsecaseCapability(keys=("product.create",), use_cases=(CreateProductUseCase,)),
        )

        with pytest.raises(AgentCompilationError):
            build_engine(
                capabilities=colliding,
                model=ScriptedToolModel(),
                container=app_container,
                deps=CapabilityDepsFactory(),
            )

    async def test_el_nombre_de_la_conexion_se_normaliza_cuando_lleva_un_punto(
        self, app_container: LoomContainer
    ) -> None:
        """A connection named ``sales.eu`` must not publish a name providers reject."""
        model = ScriptedToolModel()
        engine = build_engine(
            capabilities=(sql_capability(connection="sales.eu"),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        await engine.run("hello", identity=ANALYST)

        assert model.offered_tools == ("sql_sales_eu",)

    def test_el_plan_no_compila_cuando_el_nombre_derivado_supera_los_64_caracteres(
        self, app_container: LoomContainer
    ) -> None:
        """The name is capped at build, not at the provider."""
        long_key = "product:" + "x" * 70

        with pytest.raises(AgentCompilationError):
            build_engine(
                capabilities=(
                    CompiledUsecaseCapability(keys=(long_key,), use_cases=(CreateProductUseCase,)),
                ),
                model=ScriptedToolModel(),
                container=app_container,
                deps=CapabilityDepsFactory(),
            )


# ---------------------------------------------------------------------------
# A refusal is not a success in the event stream (security review, FIX 4)
# ---------------------------------------------------------------------------


class TestRefusalsAreVisibleInTheStream:
    async def test_el_rechazo_produce_un_evento_no_ok_cuando_se_supera_una_cota(
        self, app_container: LoomContainer
    ) -> None:
        """The part the tool really produced, translated as the stream would."""
        app_container.register_instance(SqlQueryService, RecordingSqlQueryService(canary_result(5)))
        model = ScriptedToolModel(calls=((SQL_TOOL, {"sql": "SELECT secret FROM t"}),))
        engine = build_engine(
            capabilities=(sql_capability(max_rows=2),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        await engine.run("hello", identity=ANALYST)
        events = [translate(FunctionToolResultEvent(part=part)) for part in model.tool_parts]

        assert [
            (event.ok, event.summary) for event in events if isinstance(event, ToolResultEvent)
        ] == [(False, "refused")]

    async def test_la_llamada_correcta_sigue_siendo_ok_cuando_el_resultado_cabe(
        self, app_container: LoomContainer, sql_service: RecordingSqlQueryService
    ) -> None:
        """A refusal is distinguishable: a normal call still reads ``ok`` with its count."""
        model = ScriptedToolModel(calls=((SQL_TOOL, {"sql": "SELECT 1"}),))
        engine = build_engine(
            capabilities=(sql_capability(),),
            model=model,
            container=app_container,
            deps=CapabilityDepsFactory(),
        )

        await engine.run("hello", identity=ANALYST)
        events = [translate(FunctionToolResultEvent(part=part)) for part in model.tool_parts]

        assert [
            (event.ok, event.summary) for event in events if isinstance(event, ToolResultEvent)
        ] == [(True, "3 rows")]
