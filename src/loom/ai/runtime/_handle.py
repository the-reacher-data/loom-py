"""The concrete :class:`~loom.ai.abc.AgentHandle` an ``Agent()`` marker resolves to.

Built once per execution, by :func:`agent_marker_resolver`, and handed to the
executor through :meth:`~loom.core.engine.executor.RuntimeExecutor.bind_agent_resolver`
— never constructed by application code. Running the handle reuses
:meth:`AgentRuntime.run` unchanged, so the same concurrency admission and the
same policies a transport-driven run goes through apply here too; nothing in
this module opens a parallel path around them.

Three run modes (T304): ``run(prompt)`` decodes into the artefact's own
declared output; ``run(prompt, expect=X)`` decodes this call only into ``X``,
skipping the artefact's own output check, which is compiled against the
declared schema and cannot be asked to validate another one; ``run_text``
decodes nothing at all. Both overrides are refused before the model is
called when the artefact's output hook declares the ``output`` field — see
:meth:`AgentRuntime.output_hook_shape_bound` — because that hook's command is
compiled against the declared shape and cannot be handed another one. A hook
declaring only conversation-bookkeeping fields is unaffected and keeps
running with every mode.

``mcp()`` and ``sql()`` (T301/T303) return the artefact's own granted views,
built by :mod:`~loom.ai.runtime._grants`: the same filtered session and the
same bounded connection the model's own capabilities run over, never a
second one.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from loom.ai.abc import AgentAnswer, AgentHandle, McpHandle, SqlGrantHandle
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.runtime._grants import AgentGrants, McpGrantView, SqlGrantView
from loom.ai.runtime._lifecycle import AgentRuntime
from loom.core.identity import Identity
from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.observability.span import LoomSpan
from loom.core.sql.service import SqlQueryService


class _BoundAgentHandle:
    """One named agent, bound to the caller of a single use-case execution.

    Args:
        name: Agent name as this deployment's runtime knows it.
        runtime: Live runtime the handle runs through.
        identity: Verified caller of the execution that resolved this
            handle. Never read from anywhere else, and never reassignable —
            an ``AgentHandle`` closes over one identity for its whole life.
        observability: Runtime the handle's own span opens on, or ``None`` to
            open no span. ``None`` when no observability runtime is
            registered, matching every other optional-observability
            collaborator in this codebase (e.g.
            :class:`~loom.core.sql.caller_bound.CallerBoundSql`).
        sql_query_service: The application's single, already-resolved query
            service, handed to every :class:`~loom.ai.runtime._grants.SqlGrantView`
            this handle builds — this handle never reaches into the runtime
            for it, and the runtime holds no container to reach into.
    """

    def __init__(
        self,
        *,
        name: str,
        runtime: AgentRuntime,
        identity: Identity,
        observability: ObservabilityRuntime | None,
        sql_query_service: SqlQueryService,
    ) -> None:
        self._name = name
        self._runtime = runtime
        self._identity = identity
        self._observability = observability
        self._sql_query_service = sql_query_service

    async def run(
        self,
        prompt: str,
        *,
        expect: type[Any] | None = None,
        conversation_id: str | None = None,
    ) -> AgentAnswer[Any]:
        """Run the agent once, decoded into ``expect`` or its declared output.

        Args:
            prompt: Prompt for this run.
            expect: When given, decode this run only into this type instead
                of the artefact's declared output; the artefact's own output
                check does not run for this call.
            conversation_id: Identifier of the conversation this run
                continues; ``None`` runs single-shot.

        Returns:
            The decoded answer, this run's own usage and its interaction id.

        Raises:
            AgentRunError: With ``UNAUTHORIZED`` when this handle's identity
                is anonymous — checked before the model is called; with
                ``AGENT_RUN_SHAPE_WITH_HOOK`` when ``expect`` is given and the
                artefact's output hook declares the ``output`` field — also
                checked before the model is called; or with whatever code the
                run itself failed with.
        """
        self._require_authenticated()
        if expect is not None:
            self._require_shape_allowed()
        span = self._open_span()
        try:
            result = await self._runtime.run(
                self._name,
                prompt,
                identity=self._identity,
                conversation_id=conversation_id,
                output_type=expect,
            )
        except AgentRunError as exc:
            self._close_span_on_failure(span, exc)
            raise
        self._close_span_on_success(span, result.interaction_id)
        return AgentAnswer(
            output=result.output,
            usage=result.usage,
            interaction_id=result.interaction_id,
        )

    async def run_text(
        self, prompt: str, *, conversation_id: str | None = None
    ) -> AgentAnswer[str]:
        """Run the agent for open prose, with no declared or overridden shape.

        Args:
            prompt: Prompt for this run.
            conversation_id: Identifier of the conversation this run
                continues; ``None`` runs single-shot.

        Returns:
            The model's own prose, this run's usage and its interaction id.

        Raises:
            AgentRunError: With ``UNAUTHORIZED`` or ``AGENT_RUN_SHAPE_WITH_HOOK``,
                for the same reasons :meth:`run` raises them with ``expect``.
        """
        answer = await self.run(prompt, expect=str, conversation_id=conversation_id)
        return answer

    def mcp(self, server: str) -> McpHandle:
        """Return this artefact's own filtered view of one ``mcp`` grant.

        Raises:
            AgentRunError: With ``MCP_GRANT_UNKNOWN`` when the artefact
                declares no ``mcp`` grant on that server name.
        """
        grants = self._runtime.grants(self._name)
        grant = grants.mcp.get(server)
        if grant is None:
            raise self._mcp_grant_unknown(server, grants)
        return McpGrantView(
            agent=self._name,
            capability=grant.capability,
            session=grant.session,
            catalogue=grant.catalogue,
            timeout_s=grants.tool_timeout_s,
            identity=self._identity,
            observability=self._observability,
        )

    def sql(self, connection: str) -> SqlGrantHandle:
        """Return this artefact's own bounded view of one ``sql`` grant.

        Raises:
            AgentRunError: With ``SQL_GRANT_UNKNOWN`` when the artefact
                declares no ``sql`` grant on that connection name.
        """
        grants = self._runtime.grants(self._name)
        capability = grants.sql.get(connection)
        if capability is None:
            raise self._sql_grant_unknown(connection, grants)
        return SqlGrantView(
            agent=self._name,
            capability=capability,
            sql_query_service=self._sql_query_service,
            identity=self._identity,
            observability=self._observability,
        )

    def grants(self) -> tuple[str, ...]:
        """Return every grant name reachable through :meth:`mcp` and :meth:`sql`."""
        return self._runtime.grants(self._name).names

    def _mcp_grant_unknown(self, server: str, grants: AgentGrants) -> AgentRunError:
        granted = grants.mcp_names
        if granted:
            detail = f"mcp servers this agent grants: {', '.join(granted)}"
        else:
            detail = (
                f"agent {self._name!r} grants no mcp server; add one to its "
                "artefact's 'mcp' capability"
            )
        return AgentRunError(
            AgentRunErrorCode.MCP_GRANT_UNKNOWN,
            f"agent {self._name!r} grants no mcp server named {server!r}; {detail}",
        )

    def _sql_grant_unknown(self, connection: str, grants: AgentGrants) -> AgentRunError:
        granted = tuple(grants.sql)
        if granted:
            detail = f"sql connections this agent grants: {', '.join(granted)}"
        else:
            detail = (
                f"agent {self._name!r} grants no sql connection; add one to its "
                "artefact's 'sql' capability"
            )
        return AgentRunError(
            AgentRunErrorCode.SQL_GRANT_UNKNOWN,
            f"agent {self._name!r} grants no sql connection named {connection!r}; {detail}",
        )

    def _require_authenticated(self) -> None:
        """Refuse an anonymous caller before any network call (design R3).

        Mirrors :func:`loom.ai.engines.pydantic_ai._guards.require_authenticated`:
        the same refusal, for the same reason, on the path a use case opens
        instead of the path a tool call opens.
        """
        if not self._identity.is_authenticated:
            raise AgentRunError(
                AgentRunErrorCode.UNAUTHORIZED,
                f"agent {self._name!r} requires an authenticated caller",
            )

    def _require_shape_allowed(self) -> None:
        """Refuse a per-run shape when the output hook's command needs the declared one (T304)."""
        if self._runtime.grants(self._name).output_shape_bound:
            raise AgentRunError(
                AgentRunErrorCode.AGENT_RUN_SHAPE_WITH_HOOK,
                f"agent {self._name!r} declares an output hook that reads the run's "
                "output, so this run cannot use a per-run shape; call run(prompt) "
                "for the artefact's own declared output instead",
            )

    def _open_span(self) -> LoomSpan | None:
        """Open this run's own span; the transport opens none for this path.

        ``AgentRuntime`` itself emits no span (its own docstring states the
        transport owns that), and a marker-driven run has no transport at
        all — so, unlike the HTTP and A2A surfaces, this is the only place
        that can open one. Uses :meth:`~ObservabilityRuntime.open_span`
        rather than the lexical-scope :meth:`~ObservabilityRuntime.span`
        because the interaction id, annotated on close, is only known once
        the run — awaited further down in :meth:`run` — has returned.
        """
        if self._observability is None:
            return None
        return self._observability.open_span(
            Scope.AGENT,
            "agent_run",
            agent=self._name,
            subject=self._identity.subject,
            mechanism=self._identity.mechanism,
        )

    @staticmethod
    def _close_span_on_success(span: LoomSpan | None, interaction_id: str | None) -> None:
        if span is None:
            return
        if interaction_id is not None:
            span.annotate({"interaction_id": interaction_id})
        span.end()

    @staticmethod
    def _close_span_on_failure(span: LoomSpan | None, exc: AgentRunError) -> None:
        if span is None:
            return
        if exc.interaction_id is not None:
            span.annotate({"interaction_id": exc.interaction_id})
        span.fail(exc)


def agent_marker_resolver(
    runtime: AgentRuntime,
    *,
    sql_query_service: SqlQueryService,
    observability: ObservabilityRuntime | None,
) -> Any:
    """Build the resolver ``RuntimeExecutor.bind_agent_resolver`` takes.

    Kept a plain factory function, not a class, because it closes over
    nothing but the collaborators every handle it builds needs — the live
    runtime, the application's single query service and the observability
    runtime the handle's span opens on. ``sql_query_service`` is resolved by
    the caller, once, the same way the composition root resolves it for
    every other consumer (M5): a handle never reaches into a container for
    it, and ``AgentRuntime`` holds no container to reach into.

    Args:
        runtime: Live agent runtime serving this deployment.
        sql_query_service: The application's single query service, handed to
            every ``sql`` grant view a resolved handle builds.
        observability: Runtime every handle's span opens on, or ``None``.

    Returns:
        A callable of ``(agent_name, identity) -> AgentHandle``, the shape
        :meth:`~loom.core.engine.executor.RuntimeExecutor.bind_agent_resolver`
        requires. Returned as ``Any`` because that boundary method lives in
        ``loom.core`` and is typed to stay ignorant of this pillar.
    """

    def _resolve(name: str, identity: Identity) -> _BoundAgentHandle:
        return _BoundAgentHandle(
            name=name,
            runtime=runtime,
            identity=identity,
            observability=observability,
            sql_query_service=sql_query_service,
        )

    return _resolve


if TYPE_CHECKING:  # the handle satisfies the public Protocol it stands in for (A3)

    def _bound_handle_satisfies_agent_handle(handle: _BoundAgentHandle) -> AgentHandle[Any]:
        return handle


__all__ = ["agent_marker_resolver"]
