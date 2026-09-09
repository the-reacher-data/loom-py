"""The concrete :class:`~loom.ai.abc.AgentHandle` an ``Agent()`` marker resolves to.

Built once per execution, by :func:`agent_marker_resolver`, and handed to the
executor through :meth:`~loom.core.engine.executor.RuntimeExecutor.bind_agent_resolver`
— never constructed by application code. Running the handle reuses
:meth:`AgentRuntime.run` unchanged, so the same concurrency admission and the
same policies a transport-driven run goes through apply here too; nothing in
this module opens a parallel path around them.

Only the default run mode ships in this pull request: ``run(prompt)``,
decoded into the artefact's own declared output. ``expect=``, ``run_text``,
``mcp`` and ``sql`` are the grant handles and the extra run modes of a later
pull request and raise ``NotImplementedError`` here, each naming what will
serve it.
"""

from __future__ import annotations

from typing import Any

from loom.ai.abc import AgentAnswer, McpHandle, SqlGrantHandle
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.runtime._lifecycle import AgentRuntime
from loom.core.identity import Identity
from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.observability.span import LoomSpan

_NOT_YET = (
    "ships in a later pull request of the model-as-actor work; "
    "call run(prompt) for the artefact's own declared output"
)


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
    """

    def __init__(
        self,
        *,
        name: str,
        runtime: AgentRuntime,
        identity: Identity,
        observability: ObservabilityRuntime | None,
    ) -> None:
        self._name = name
        self._runtime = runtime
        self._identity = identity
        self._observability = observability

    async def run(
        self,
        prompt: str,
        *,
        expect: type[Any] | None = None,
        conversation_id: str | None = None,
    ) -> AgentAnswer[Any]:
        """Run the agent once, decoded into its own declared output shape.

        Args:
            prompt: Prompt for this run.
            expect: Not yet implemented; passing it raises.
            conversation_id: Identifier of the conversation this run
                continues; ``None`` runs single-shot.

        Returns:
            The decoded answer, this run's own usage and its interaction id.

        Raises:
            NotImplementedError: If ``expect`` is given.
            AgentRunError: With ``UNAUTHORIZED`` when this handle's identity
                is anonymous — checked before the model is called — or with
                whatever code the run itself failed with.
        """
        if expect is not None:
            raise NotImplementedError(f"AgentHandle.run(expect=...) {_NOT_YET}")
        self._require_authenticated()
        span = self._open_span()
        try:
            result = await self._runtime.run(
                self._name,
                prompt,
                identity=self._identity,
                conversation_id=conversation_id,
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
        """Not yet implemented; raises ``NotImplementedError``."""
        raise NotImplementedError(f"AgentHandle.run_text() {_NOT_YET}")

    def mcp(self, server: str) -> McpHandle:
        """Not yet implemented; raises ``NotImplementedError``."""
        raise NotImplementedError(f"AgentHandle.mcp() {_NOT_YET}")

    def sql(self, connection: str) -> SqlGrantHandle:
        """Not yet implemented; raises ``NotImplementedError``."""
        raise NotImplementedError(f"AgentHandle.sql() {_NOT_YET}")

    def grants(self) -> tuple[str, ...]:
        """Not yet implemented; raises ``NotImplementedError``."""
        raise NotImplementedError(f"AgentHandle.grants() {_NOT_YET}")

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
    observability: ObservabilityRuntime | None,
) -> Any:
    """Build the resolver ``RuntimeExecutor.bind_agent_resolver`` takes.

    Kept a plain factory function, not a class, because it closes over
    nothing but the two collaborators every handle it builds needs — the
    live runtime and the observability runtime the handle's span opens on.

    Args:
        runtime: Live agent runtime serving this deployment.
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
        )

    return _resolve


__all__ = ["agent_marker_resolver"]
