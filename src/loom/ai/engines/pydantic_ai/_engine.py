"""The engine that serves one compiled plan through pydantic-ai.

One :class:`~pydantic_ai.Agent` is built per plan at start-up and reused by
every run; per-invocation state is exactly the caller's dependency bundle, so
nothing is rebuilt, reflected over or re-parsed per request.

Retries follow FR-028: only the ``INFRASTRUCTURE`` class is retried, and only
``policies.retries`` times, each after an exponential wait — retrying a rate
limit immediately spends the quota of the very provider that is throttling.
``OUTPUT_SCHEMA_VIOLATION`` is model behaviour, not an outage, so it is final
at this level. A stream retries only while it has emitted nothing — once a
delta has reached the caller, replaying the run would duplicate the answer, so
the failure is surfaced with its class and the caller decides.

An agent holding a capability does not retry at all, for the same reason: by
the time the provider fails, the model may already have invoked an application
operation, and nothing about a granted use case is idempotent or keyed, so a
replay would execute it again. Only a pure-language agent — which has no side
effect to duplicate — keeps its retries.

One resolved provider serves the agent, always: a failure is never re-routed to
another vendor (FR-019a).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import fields
from time import perf_counter
from types import MappingProxyType
from typing import Any

from pydantic_ai import Agent, AgentRunResult, AgentRunResultEvent
from pydantic_ai.usage import RunUsage

from loom.ai.abc import (
    AgentEvent,
    AgentResult,
    AgentUsage,
    Conversation,
    DepsFactory,
    ErrorEvent,
    FinalEvent,
    HealthStatus,
)
from loom.ai.compiler import AgentPlan
from loom.ai.engines.pydantic_ai._errors import as_run_error
from loom.ai.engines.pydantic_ai._events import translate
from loom.ai.engines.pydantic_ai._history import (
    RunConversation,
    decode_conversation,
    new_messages,
    run_kwargs,
)
from loom.ai.engines.pydantic_ai._output import decode_output
from loom.ai.errors import AgentRunError, AgentRunErrorCode, is_retriable
from loom.core.di import LoomContainer
from loom.core.identity import Identity

RETRY_BACKOFF_MS = 200
"""Base wait before a retried attempt; doubled per attempt (FR-028).

A constant rather than a policy field: ``policies`` describes what the agent
is allowed to spend, and no requirement asks the artifact to tune the wait.
"""

_HEALTHY = HealthStatus(status="ok")

# Counters :class:`~loom.ai.abc.AgentUsage` names itself, plus the engine's own
# ``details`` bag. Everything else the engine reports is copied into
# ``AgentUsage.details`` unchanged, so this set is the whole curation loom does.
_NAMED_COUNTERS: frozenset[str] = frozenset(
    {
        "input_tokens",
        "output_tokens",
        "requests",
        "cache_read_tokens",
        "cache_write_tokens",
        "tool_calls",
        "cost",
        "details",
    }
)

# Built once, at import: an engine reports health on every probe tick, and the
# answer is one of three fixed values.
_HEALTH_BY_CODE: Mapping[AgentRunErrorCode, HealthStatus] = MappingProxyType(
    {
        AgentRunErrorCode.PROVIDER_UNAVAILABLE: HealthStatus(
            status="unavailable", detail="the last run failed: the provider was unavailable"
        ),
        AgentRunErrorCode.PROVIDER_RATE_LIMITED: HealthStatus(
            status="degraded", detail="the last run failed: the provider rate limited it"
        ),
    }
)


class PydanticAIEngine:
    """One compiled agent, running on pydantic-ai.

    Args:
        plan: Compiled plan this engine serves.
        agent: Engine agent already built from the plan's spec and model.
        deps: Per-invocation dependency factory; singleton services are
            captured here, at build, and the caller's identity is supplied per
            invocation (FR-043).
        container: Application container the dependency factory resolves from.
    """

    def __init__(
        self,
        *,
        plan: AgentPlan,
        agent: Agent[Any, Any],
        deps: DepsFactory,
        container: LoomContainer,
    ) -> None:
        self._plan = plan
        self._agent = agent
        self._deps = deps
        self._container = container
        self._attempts = max(plan.policies.retries, 0) + 1
        self._last_failure: AgentRunErrorCode | None = None

    async def run(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
    ) -> AgentResult:
        """Run the agent to completion.

        Args:
            prompt: Caller prompt.
            identity: Verified caller; every capability call runs as them.
            conversation: The conversation this run continues, or ``None``
                for a single shot.

        Returns:
            The validated output, the run's usage and — when the run carried a
            conversation — the messages it added.

        Raises:
            AgentRunError: Carrying the coded, classified failure and what the
                run had already spent before it failed. A history that does
                not decode fails here before any provider call, with no usage.
        """
        decoded = decode_conversation(conversation)
        started = perf_counter()
        spend = RunUsage()
        try:
            result = await self._run_with_retries(prompt, identity, spend, decoded)
            output = decode_output(self._plan.output, result)
        except AgentRunError as error:
            error.usage = self._usage(spend, started)
            raise
        return AgentResult(
            output=output,
            usage=self._usage(spend, started),
            messages=new_messages(result, decoded),
        )

    def run_stream(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
    ) -> AbstractAsyncContextManager[AsyncIterator[AgentEvent]]:
        """Run the agent, streaming events.

        Args:
            prompt: Caller prompt.
            identity: Verified caller; every capability call runs as them.
            conversation: The conversation this run continues, or ``None``
                for a single shot.

        Returns:
            An async context manager yielding the event stream and closing it
            — and the provider connection behind it — on exit.
        """
        return self._stream(prompt, identity, conversation, output_type=None)

    def run_stream_shaped(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        output_type: type[Any],
    ) -> AbstractAsyncContextManager[AsyncIterator[AgentEvent]]:
        """Run the agent, streaming events decoded into *output_type* for this call only.

        The counterpart of :meth:`run_stream` that
        :class:`~loom.ai.runtime._handle._BoundAgentHandle` reaches for
        ``AgentHandle.run(expect=...)`` and ``AgentHandle.run_text`` (T304).
        Deliberately not a parameter of :meth:`run_stream` itself: that
        signature is pinned to exactly ``prompt``, ``identity`` and
        ``conversation`` by the shared engine contract, so a shape override is
        a separate, optional capability an engine opts into — read with
        ``getattr`` by :func:`~loom.ai.runtime._lifecycle._open_engine_stream`
        — rather than a fourth parameter every engine must carry.

        pydantic-ai validates the run's answer against *output_type* on its
        own, exactly as it validates against the plan's declared shape for an
        unshaped run; what this method skips is loom's *own* output check
        (:func:`~loom.ai.engines.pydantic_ai._output.decode_output`), because
        that check is compiled against the plan's declared schema and *this*
        answer is deliberately shaped otherwise.

        Args:
            prompt: Caller prompt.
            identity: Verified caller; every capability call runs as them.
            conversation: The conversation this run continues, or ``None``.
            output_type: Type this run's answer is decoded into, overriding
                the plan's own declared output for this call only.

        Returns:
            An async context manager yielding the event stream; its
            ``final`` event carries an already-decoded ``output_type``
            instance rather than the plan's declared shape.
        """
        return self._stream(prompt, identity, conversation, output_type=output_type)

    async def health(self) -> HealthStatus:
        """Report health from the last observed outcome, with no network I/O.

        Returns:
            ``unavailable`` after a provider outage, ``degraded`` after a rate
            limit, ``ok`` otherwise; a successful run clears the state.
        """
        if self._last_failure is None:
            return _HEALTHY
        return _HEALTH_BY_CODE.get(self._last_failure, _HEALTHY)

    # -- internals ---------------------------------------------------------

    async def _run_with_retries(
        self,
        prompt: str,
        identity: Identity,
        spend: RunUsage,
        conversation: RunConversation | None,
    ) -> AgentRunResult[Any]:
        """Call the provider, retrying only infrastructure failures.

        *spend* is handed to the engine and mutated by it, so it holds what the
        run cost even when the call raises — and it accumulates across retried
        attempts, because a retry spends the provider's tokens again. The
        decoded history, by contrast, is the same list on every attempt.
        """
        deps = self._deps.build(identity, self._container)
        for attempt in range(self._attempts):
            try:
                result = await self._agent.run(
                    prompt, deps=deps, usage=spend, **run_kwargs(conversation)
                )
            except Exception as exc:
                error = as_run_error(exc)
                self._record(error.code)
                if self._may_retry(error.code, attempt):
                    await _backoff(attempt)
                    continue
                raise error from exc
            self._record(None)
            return result
        raise AssertionError("unreachable: the loop returns or raises on every attempt")

    def _may_retry(self, code: AgentRunErrorCode, attempt: int) -> bool:
        """Report whether another attempt is allowed.

        An agent holding a capability may already have invoked an application
        operation during the failed attempt, and replaying the run would invoke
        it a second time: nothing about a granted use case is idempotent or
        keyed. So a capability-bearing agent surfaces the failure with its class
        and lets the caller decide, exactly as a stream does once a delta has
        reached the caller. A pure-language agent has no side effect to
        duplicate and keeps its retries.
        """
        if self._plan.capabilities:
            return False
        return is_retriable(code) and attempt + 1 < self._attempts

    def _record(self, code: AgentRunErrorCode | None) -> None:
        self._last_failure = code

    def _usage(self, usage: RunUsage, started: float) -> AgentUsage:
        return AgentUsage(
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            requests=usage.requests,
            duration_ms=int((perf_counter() - started) * 1000),
            cache_read_tokens=usage.cache_read_tokens,
            cache_write_tokens=usage.cache_write_tokens,
            tool_calls=usage.tool_calls,
            cost=usage.cost,
            details=_extra_counters(usage),
        )

    @asynccontextmanager
    async def _stream(
        self,
        prompt: str,
        identity: Identity,
        conversation: Conversation | None,
        *,
        output_type: type[Any] | None,
    ) -> AsyncIterator[AsyncIterator[AgentEvent]]:
        events = self._events(prompt, identity, conversation, output_type)
        try:
            yield events
        finally:
            await events.aclose()

    async def _events(
        self,
        prompt: str,
        identity: Identity,
        conversation: Conversation | None,
        output_type: type[Any] | None,
    ) -> AsyncGenerator[AgentEvent]:
        """Replay one run as loom events, ending in exactly one terminal.

        A history that does not decode is not a provider outcome: it ends the
        stream with its coded error, no usage and no health record.
        """
        try:
            decoded = decode_conversation(conversation)
        except AgentRunError as rejected:
            yield ErrorEvent(code=rejected.code, message=str(rejected))
            return
        deps = self._deps.build(identity, self._container)
        spend = RunUsage()
        started = perf_counter()
        for attempt in range(self._attempts):
            emitted = False
            attempt_run = self._one_run(prompt, deps, spend, started, decoded, output_type)
            try:
                async for event in attempt_run:
                    emitted = True
                    yield event
                self._record(None)
                return
            except Exception as exc:
                error = as_run_error(exc)
                self._record(error.code)
                if not emitted and self._may_retry(error.code, attempt):
                    await _backoff(attempt)
                    continue
                yield ErrorEvent(
                    code=error.code, message=str(error), usage=self._usage(spend, started)
                )
                return

    async def _one_run(
        self,
        prompt: str,
        deps: object,
        spend: RunUsage,
        started: float,
        conversation: RunConversation | None,
        output_type: type[Any] | None,
    ) -> AsyncIterator[AgentEvent]:
        """One attempt: engine events in, loom events out, ending in ``final``."""
        pinned: dict[str, Any] = {} if output_type is None else {"output_type": output_type}
        async with self._agent.run_stream_events(
            prompt, deps=deps, usage=spend, **run_kwargs(conversation), **pinned
        ) as stream:
            async for event in stream:
                if isinstance(event, AgentRunResultEvent):
                    yield self._final(event.result, spend, started, conversation, output_type)
                    return
                mapped = translate(event)
                if mapped is not None:
                    yield mapped

    def _final(
        self,
        result: AgentRunResult[Any],
        spend: RunUsage,
        started: float,
        conversation: RunConversation | None,
        output_type: type[Any] | None,
    ) -> FinalEvent:
        # An overridden shape skips loom's own output check on purpose
        # (T304): 'decode_output' is compiled against the plan's declared
        # schema, and pydantic-ai has already validated 'result.output'
        # against 'output_type' itself when one was requested.
        if output_type is not None:
            output = result.output
        else:
            output = decode_output(self._plan.output, result)
        return FinalEvent(
            output=output,
            usage=self._usage(spend, started),
            messages=new_messages(result, conversation),
        )


def _extra_counters(usage: RunUsage) -> dict[str, int | float]:
    """Return every counter of *usage* that :class:`AgentUsage` does not name.

    Read from the instance as well as from the declared fields: pydantic-ai
    lets a provider set counters its dataclass never declared, and a counter a
    future release adds must reach the caller without a change here.

    Args:
        usage: Accounting the engine reported for one run.

    Returns:
        The engine's own ``details`` merged with every unnamed field, under the
        engine's names and with the engine's values. Never de-duplicated
        against the named counters: deciding that a provider's own entry
        duplicates a normalised one is exactly the curation this avoids.
    """
    declared = {field.name for field in fields(usage)}
    unnamed = sorted((declared | set(vars(usage))) - _NAMED_COUNTERS)
    extras: dict[str, int | float] = {name: getattr(usage, name) for name in unnamed}
    return {**usage.details, **extras}


async def _backoff(attempt: int) -> None:
    """Wait before the next attempt, doubling the base wait per attempt."""
    await asyncio.sleep(RETRY_BACKOFF_MS * (2**attempt) / 1000)
