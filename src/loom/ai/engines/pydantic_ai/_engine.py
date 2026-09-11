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

``policies.max_usd`` is an *elastic* cap (see :class:`~loom.ai.declarative.PolicySpec`
and "Spend caps" in ``docs/ai/artifacts.md``): a run whose cost this engine
cannot fully compute is served by default (``on_unpriced_spend: serve``) with
the gap recorded in :attr:`~loom.ai.abc.AgentUsage.details`, rather than
discarded after the provider has already billed it. :meth:`PydanticAIEngine.health`
carries the same gap forward once observed.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import fields
from time import perf_counter
from types import MappingProxyType
from typing import Any, Final, cast

from pydantic_ai import Agent, AgentRunResult, AgentRunResultEvent
from pydantic_ai.messages import ModelResponse, PartStartEvent
from pydantic_ai.usage import RunUsage, UsageLimits

from loom.ai.abc import (
    AgentEvent,
    AgentResult,
    AgentUsage,
    Conversation,
    DepsFactory,
    ErrorEvent,
    FinalEvent,
    HealthStatus,
    TextDeltaEvent,
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

_UNPRICED_SPEND_HEALTH: Final[HealthStatus] = HealthStatus(
    status="degraded",
    detail=(
        "a run produced a model response this engine could not fully price; "
        "policies.max_usd may not be fully enforced"
    ),
)
"""Reported once :attr:`PydanticAIEngine._unpriced_spend_observed` is set;
never cleared by a later clean run, unlike :data:`_HEALTH_BY_CODE`. See
"Spend caps" in ``docs/ai/artifacts.md``."""


class PydanticAIEngine:
    """One compiled agent, running on pydantic-ai.

    Args:
        plan: Compiled plan this engine serves.
        agent: Engine agent already built from the plan's spec and model,
            carrying the plan's own ``output_check`` validator when it
            declares one.
        shaped_agent: Engine agent serving a per-run shape override
            (:meth:`run_stream_shaped`), built with no output validator
            registered. Defaults to *agent* when not given, which is
            correct whenever the plan declares no ``output_check``: the
            two agents would behave identically, so
            :class:`~loom.ai.engines.pydantic_ai.provider.PydanticAIEngineProvider`
            builds only one and passes it for both.
        deps: Per-invocation dependency factory; singleton services are
            captured here, at build, and the caller's identity is supplied per
            invocation (FR-043).
        container: Application container the dependency factory resolves from.
        usage_limits: The plan's spend caps, projected once at construction
            (:func:`~loom.ai.engines.pydantic_ai._limits.usage_limits`) and
            passed on every run and every streamed attempt.
    """

    def __init__(
        self,
        *,
        plan: AgentPlan,
        agent: Agent[Any, Any],
        shaped_agent: Agent[Any, Any] | None = None,
        deps: DepsFactory,
        container: LoomContainer,
        usage_limits: UsageLimits,
    ) -> None:
        self._plan = plan
        self._agent = agent
        self._shaped_agent = shaped_agent if shaped_agent is not None else agent
        self._deps = deps
        self._container = container
        self._usage_limits = usage_limits
        self._attempts = max(plan.policies.retries, 0) + 1
        self._last_failure: AgentRunErrorCode | None = None
        self._unpriced_spend_observed = False

    async def run(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        state: object | None = None,
    ) -> AgentResult:
        """Run the agent to completion.

        Args:
            prompt: Caller prompt.
            identity: Verified caller; every capability call runs as them.
            conversation: The conversation this run continues, or ``None``
                for a single shot.
            state: This run's state, already resolved against the plan's
                declared shape by :class:`~loom.ai.runtime.AgentRuntime`;
                forwarded to :attr:`_deps` unchanged.

        Returns:
            The validated output, the run's usage and the messages it added,
            bounded by ``policies.max_history_bytes`` (``None`` above it).

        Raises:
            AgentRunError: Carrying the coded, classified failure and what the
                run had already spent before it failed. A history that does
                not decode fails here before any provider call, with no usage.
                ``COST_NOT_MEASURABLE`` when ``policies.on_unpriced_spend`` is
                ``'refuse'`` and this run priced incompletely.
        """
        decoded = decode_conversation(conversation)
        started = perf_counter()
        spend = RunUsage()
        try:
            result = await self._run_with_retries(prompt, identity, spend, decoded, state)
            unpriced = self._apply_unpriced_spend_policy(result)
            output = decode_output(self._plan.output, result)
        except AgentRunError as error:
            self._record(error.code)
            error.usage = self._usage(spend, started)
            raise
        return AgentResult(
            output=output,
            usage=self._usage(spend, started, unpriced_requests=unpriced),
            messages=new_messages(
                result,
                agent=self._plan.name,
                max_history_bytes=self._plan.policies.max_history_bytes,
            ),
        )

    def run_stream(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        state: object | None = None,
    ) -> AbstractAsyncContextManager[AsyncIterator[AgentEvent]]:
        """Run the agent, streaming events.

        Args:
            prompt: Caller prompt.
            identity: Verified caller; every capability call runs as them.
            conversation: The conversation this run continues, or ``None``
                for a single shot.
            state: This run's state; see :meth:`run`'s own ``state``.

        Returns:
            An async context manager yielding the event stream and closing it
            — and the provider connection behind it — on exit.
        """
        return self._stream(prompt, identity, conversation, output_type=None, state=state)

    def run_stream_shaped(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        output_type: type[Any],
        state: object | None = None,
    ) -> AbstractAsyncContextManager[AsyncIterator[AgentEvent]]:
        """Run the agent, streaming events decoded into *output_type* for this call only.

        The counterpart of :meth:`run_stream` that
        :class:`~loom.ai.runtime._handle._BoundAgentHandle` reaches for
        ``AgentHandle.run(expect=...)`` and ``AgentHandle.run_text`` (T304).
        ``output_type`` is deliberately not a parameter of :meth:`run_stream`
        itself: that signature is pinned to exactly ``prompt``, ``identity``,
        ``conversation`` and ``state`` by the shared engine contract, so a
        shape override is a separate, optional capability an engine opts
        into — read with ``getattr`` by
        :func:`~loom.ai.runtime._lifecycle._open_engine_stream` — rather than
        a parameter every engine must carry.

        pydantic-ai validates the run's answer against *output_type* on its
        own, exactly as it validates against the plan's declared shape for an
        unshaped run; what this method skips is loom's *own* output check
        (:func:`~loom.ai.engines.pydantic_ai._output.decode_output`), because
        that check is compiled against the plan's declared schema and *this*
        answer is deliberately shaped otherwise. When the plan declares an
        ``output_check``, this call is also served by :attr:`_shaped_agent`
        rather than :attr:`_agent`: pydantic-ai itself refuses a run-level
        ``output_type`` on an agent that holds an output validator, and the
        plan's checked agent always holds one once ``output_check`` is
        declared.

        Args:
            prompt: Caller prompt.
            identity: Verified caller; every capability call runs as them.
            conversation: The conversation this run continues, or ``None``.
            output_type: Type this run's answer is decoded into, overriding
                the plan's own declared output for this call only.
            state: This run's state; see :meth:`run`'s own ``state``.

        Returns:
            An async context manager yielding the event stream; its
            ``final`` event carries an already-decoded ``output_type``
            instance rather than the plan's declared shape.
        """
        return self._stream(prompt, identity, conversation, output_type=output_type, state=state)

    async def health(self) -> HealthStatus:
        """Report health from the last observed outcome, with no network I/O.

        Returns:
            ``unavailable`` after a provider outage; ``degraded`` after a
            rate limit, or once this engine's bound model has produced a
            response — served or refused — that it could not fully price;
            ``ok`` otherwise.
        """
        by_code = (
            _HEALTH_BY_CODE.get(self._last_failure) if self._last_failure is not None else None
        )
        if by_code is not None:
            return by_code
        if self._unpriced_spend_observed:
            return _UNPRICED_SPEND_HEALTH
        return _HEALTHY

    # -- internals ---------------------------------------------------------

    async def _run_with_retries(
        self,
        prompt: str,
        identity: Identity,
        spend: RunUsage,
        conversation: RunConversation | None,
        state: object | None,
    ) -> AgentRunResult[Any]:
        """Call the provider, retrying only infrastructure failures.

        *spend* is handed to the engine and mutated by it, so it holds what the
        run cost even when the call raises — and it accumulates across retried
        attempts, because a retry spends the provider's tokens again. The
        decoded history, by contrast, is the same list on every attempt.

        *state* is already resolved by
        :class:`~loom.ai.runtime.AgentRuntime` against the plan's declared
        shape; it is cast, never re-validated, into the mapping
        :class:`~loom.ai.abc.DepsFactory` declares (FR-009).
        """
        deps = self._deps.build(
            identity, self._container, state=cast("Mapping[str, Any] | None", state)
        )
        for attempt in range(self._attempts):
            try:
                result = await self._agent.run(
                    prompt,
                    deps=deps,
                    usage=spend,
                    usage_limits=self._usage_limits,
                    **run_kwargs(conversation),
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
        """Record this attempt's outcome; the single writer of :attr:`_last_failure`.

        Every caller that raises or catches an :class:`AgentRunError` —
        :meth:`run`, :meth:`_run_with_retries` and :meth:`_events` alike —
        routes through here before the error leaves it, so ``health()`` never
        depends on which of those callers happened to fail. ``COST_NOT_MEASURABLE``
        also sticks :attr:`_unpriced_spend_observed`; the ``on_unpriced_spend:
        serve`` path sets that same attribute itself, from
        :meth:`_apply_unpriced_spend_policy`, since a served response has
        nothing to route through here — it never raises.
        """
        self._last_failure = code
        if code is AgentRunErrorCode.COST_NOT_MEASURABLE:
            self._unpriced_spend_observed = True

    def _record_attempt_failure(
        self, exc: Exception, attempt: int, emitted: bool
    ) -> tuple[AgentRunError, bool]:
        """Classify one streamed attempt's failure and record it.

        Shared by :meth:`_events`' two failure points — the attempt's own
        event stream and, separately, :meth:`_conclude` — so both route
        through :meth:`_record` the same way and neither has to repeat the
        retry decision.

        Returns:
            The classified error, and whether the caller may retry.
        """
        error = as_run_error(exc)
        self._record(error.code)
        return error, not emitted and self._may_retry(error.code, attempt)

    def _apply_unpriced_spend_policy(self, result: AgentRunResult[Any]) -> int:
        """Act on this run's responses that pydantic-ai could not price.

        A no-op whenever ``policies.max_usd`` is absent, or when every
        response in *result* priced cleanly. Counted via
        :func:`_count_unpriced_responses`, from the winning attempt's own
        messages only — see "Spend caps" in ``docs/ai/artifacts.md`` for what
        that excludes and why.

        Args:
            result: The winning attempt's result, carrying its own messages.

        Returns:
            Responses this run could not price; ``0`` when nothing is missing
            or no cap is declared. Under ``on_unpriced_spend: serve``, also
            marks this engine's :meth:`health` ``degraded`` from now on.

        Raises:
            AgentRunError: ``COST_NOT_MEASURABLE`` when
                ``policies.on_unpriced_spend`` is ``'refuse'`` and at least
                one response priced incompletely. Every caller of this method
                catches that error and calls :meth:`_record`, so the flag
                this raise implies is set there, not here.
        """
        if self._plan.policies.max_usd is None:
            return 0
        unpriced = _count_unpriced_responses(result)
        if unpriced == 0:
            return 0
        if self._plan.policies.on_unpriced_spend == "refuse":
            raise AgentRunError(
                AgentRunErrorCode.COST_NOT_MEASURABLE,
                f"policies.max_usd is declared but {unpriced} of this run's "
                "model response(s) could not be priced, so the cap could not "
                "be fully enforced; policies.on_unpriced_spend is 'refuse'",
            )
        self._unpriced_spend_observed = True
        return unpriced

    def _usage(self, usage: RunUsage, started: float, *, unpriced_requests: int = 0) -> AgentUsage:
        details = _extra_counters(usage)
        if unpriced_requests:
            details = {**details, "unpriced_requests": unpriced_requests}
        return AgentUsage(
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            requests=usage.requests,
            duration_ms=int((perf_counter() - started) * 1000),
            cache_read_tokens=usage.cache_read_tokens,
            cache_write_tokens=usage.cache_write_tokens,
            tool_calls=usage.tool_calls,
            cost=usage.cost,
            details=details,
        )

    @asynccontextmanager
    async def _stream(
        self,
        prompt: str,
        identity: Identity,
        conversation: Conversation | None,
        *,
        output_type: type[Any] | None,
        state: object | None,
    ) -> AsyncIterator[AsyncIterator[AgentEvent]]:
        events = self._events(prompt, identity, conversation, output_type, state)
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
        state: object | None,
    ) -> AsyncGenerator[AgentEvent]:
        """Replay one run as loom events, ending in exactly one terminal.

        A history that does not decode is not a provider outcome: it ends the
        stream with its coded error, no usage and no health record.

        ``on_unpriced_spend: refuse`` is evaluated only once an attempt's
        deltas are fully drained and its result is in hand: a refused stream
        can still deliver a complete answer's deltas and end on an ``error``
        terminal frame instead of a ``final`` one, so a caller that only
        watches the terminal event misses the answer it was already sent.
        """
        try:
            decoded = decode_conversation(conversation)
        except AgentRunError as rejected:
            yield ErrorEvent(code=rejected.code, message=str(rejected))
            return
        deps = self._deps.build(
            identity, self._container, state=cast("Mapping[str, Any] | None", state)
        )
        spend = RunUsage()
        started = perf_counter()
        for attempt in range(self._attempts):
            emitted = False
            outcome: list[AgentRunResult[Any]] = []
            attempt_run = self._one_run(prompt, deps, spend, decoded, output_type, outcome)
            try:
                async for event in attempt_run:
                    emitted = True
                    yield event
            except Exception as exc:
                error, retry = self._record_attempt_failure(exc, attempt, emitted)
                if retry:
                    await _backoff(attempt)
                    continue
                yield ErrorEvent(
                    code=error.code, message=str(error), usage=self._usage(spend, started)
                )
                return
            # Deliberately outside both ``try`` blocks: this checks the engine's
            # own contract with pydantic-ai, not a provider or policy failure,
            # so it must never be classified as one (FR-028 correction).
            if not outcome:
                raise AssertionError(
                    "unreachable: the engine's own event stream always ends with a "
                    "trailing AgentRunResultEvent"
                )
            try:
                final = self._conclude(outcome[0], spend, started, output_type)
            except Exception as exc:
                error, _ = self._record_attempt_failure(exc, attempt, emitted=True)
                yield ErrorEvent(
                    code=error.code, message=str(error), usage=self._usage(spend, started)
                )
                return
            self._record(None)
            yield final
            return

    async def _one_run(
        self,
        prompt: str,
        deps: object,
        spend: RunUsage,
        conversation: RunConversation | None,
        output_type: type[Any] | None,
        outcome: list[AgentRunResult[Any]],
    ) -> AsyncIterator[AgentEvent]:
        """One attempt: engine events in, loom delta events out.

        Never yields the terminal ``final`` event and never applies
        ``on_unpriced_spend`` itself: the winning result is appended to
        *outcome*, so :meth:`_events` can apply the policy only once every
        delta from this attempt has already reached the caller.

        When the plan declares ``output_check``, text deltas are buffered in
        *pending* rather than relayed live, and flushed only once this
        attempt's ``AgentRunResultEvent`` arrives: an ``output_check``
        rejection replays the model request inside this same call, each
        replay's ``PartStartEvent`` starts again at ``index=0``, and a delta
        already relayed cannot be un-sent. A plan with no check never
        buffers: *pending* stays empty and every mapped event is yielded as
        it arrives, exactly as before ``output_check`` existed.

        *output_type* also selects which built agent serves this attempt:
        :attr:`_shaped_agent`, carrying no output validator, when given —
        pydantic-ai refuses a run-level ``output_type`` on an agent holding
        one — and :attr:`_agent`, the plan's own checked agent, otherwise.
        """
        withhold = self._plan.output_check is not None
        pending: list[AgentEvent] = []
        pinned: dict[str, Any] = {} if output_type is None else {"output_type": output_type}
        agent = self._agent if output_type is None else self._shaped_agent
        async with agent.run_stream_events(
            prompt,
            deps=deps,
            usage=spend,
            usage_limits=self._usage_limits,
            **run_kwargs(conversation),
            **pinned,
        ) as stream:
            async for event in stream:
                if isinstance(event, AgentRunResultEvent):
                    outcome.append(event.result)
                    for held in pending:
                        yield held
                    return
                if withhold and isinstance(event, PartStartEvent) and event.index == 0:
                    pending.clear()
                mapped = translate(event)
                if mapped is None:
                    continue
                if withhold and isinstance(mapped, TextDeltaEvent):
                    pending.append(mapped)
                else:
                    yield mapped

    def _conclude(
        self,
        result: AgentRunResult[Any],
        spend: RunUsage,
        started: float,
        output_type: type[Any] | None,
    ) -> FinalEvent:
        """Apply ``on_unpriced_spend`` to a finished attempt and build its ``final`` event.

        Raises:
            AgentRunError: ``COST_NOT_MEASURABLE``, propagated to
                :meth:`_events` exactly as any other attempt failure — but
                never retried, because :func:`~loom.ai.errors.is_retriable`
                excludes it.
        """
        unpriced = self._apply_unpriced_spend_policy(result)
        return self._final(result, spend, started, output_type, unpriced_requests=unpriced)

    def _final(
        self,
        result: AgentRunResult[Any],
        spend: RunUsage,
        started: float,
        output_type: type[Any] | None,
        *,
        unpriced_requests: int = 0,
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
            usage=self._usage(spend, started, unpriced_requests=unpriced_requests),
            messages=new_messages(
                result,
                agent=self._plan.name,
                max_history_bytes=self._plan.policies.max_history_bytes,
            ),
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


def _count_unpriced_responses(result: AgentRunResult[Any]) -> int:
    """Count this run's own model responses whose cost pydantic-ai left unset.

    Reads ``result.new_messages()``, never ``result.all_messages()``: the
    latter also walks the ``message_history`` a multi-turn conversation
    injected, and a prior turn's response was already billed and already
    evaluated against its own run's ``max_usd``. A response with no
    ``model_name`` is a capability's own synthetic reply, never a provider
    call, so it never had a price to miss.

    Args:
        result: A completed attempt's result.

    Returns:
        The number of this run's own ``ModelResponse`` messages with no
        computed cost.
    """
    return sum(
        1
        for message in result.new_messages()
        if isinstance(message, ModelResponse)
        and message.model_name is not None
        and message.usage.cost is None
    )


async def _backoff(attempt: int) -> None:
    """Wait before the next attempt, doubling the base wait per attempt."""
    await asyncio.sleep(RETRY_BACKOFF_MS * (2**attempt) / 1000)
