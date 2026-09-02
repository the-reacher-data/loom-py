"""Span handles for the observability runtime.

:meth:`~loom.core.observability.runtime.ObservabilityRuntime.span` covers the
common case: a span that opens and closes inside one lexical scope, and can
therefore be made *current* for its whole body.

:class:`LoomSpan` covers the case that cannot. A stream opens its span in one
``asend`` of an async generator and closes it in another, so the two ends run
in different :mod:`opentelemetry.context` contexts. A span made current in one
``asend`` would have to be detached in another — which raises internally, is
swallowed by OTEL as a *"Failed to detach context"* log line, and in between
frames leaks the span into whatever the consumer is doing. ``LoomSpan`` never
attaches, so neither can happen.

Only :class:`LoomSpan` is public here. ``SpanIdentity`` and
``apply_terminal_state`` are internal to the observability package.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from time import perf_counter
from typing import Self

from opentelemetry.trace import Span, StatusCode, Tracer, use_span

from loom.core.observability.event import EventKind, LifecycleEvent, Scope


def elapsed_ms(started: float) -> float:
    """Return the milliseconds elapsed since a ``perf_counter`` reading."""
    return (perf_counter() - started) * 1000


@dataclass(frozen=True, slots=True)
class SpanIdentity:
    """Everything the START and closing events of one span have in common.

    Internal to the observability package: it exists so that
    ``ObservabilityRuntime.span`` and :class:`LoomSpan` cannot drift in the
    events they emit for the same span.
    """

    scope: Scope
    name: str
    trace_id: str | None
    correlation_id: str | None
    event_id: str | None
    meta: Mapping[str, object]

    @classmethod
    def build(
        cls,
        scope: Scope,
        name: str,
        *,
        trace_id: str | None,
        correlation_id: str | None,
        meta: Mapping[str, object],
    ) -> Self:
        """Build an identity, promoting ``id`` out of the caller's meta fields.

        ``id`` is a first-class ``LifecycleEvent`` field, so observers can
        correlate START with END without reaching into ``meta``.
        """
        fields = dict(meta)
        raw_id = fields.pop("id", None)
        return cls(
            scope=scope,
            name=name,
            trace_id=trace_id,
            correlation_id=correlation_id,
            event_id=str(raw_id) if raw_id is not None else None,
            meta=fields,
        )

    def start_event(self) -> LifecycleEvent:
        """Return the ``START`` event of this span."""
        return LifecycleEvent.start(
            scope=self.scope,
            name=self.name,
            trace_id=self.trace_id,
            correlation_id=self.correlation_id,
            id=self.event_id,
            meta=self.meta,
        )

    def end_event(self, duration_ms: float) -> LifecycleEvent:
        """Return the successful ``END`` event of this span."""
        return LifecycleEvent.end(
            scope=self.scope,
            name=self.name,
            trace_id=self.trace_id,
            correlation_id=self.correlation_id,
            id=self.event_id,
            duration_ms=duration_ms,
            meta=self.meta,
        )

    def error_event(self, exc: BaseException, duration_ms: float) -> LifecycleEvent:
        """Return the ``ERROR`` event of this span for a failure."""
        return LifecycleEvent.exception(
            scope=self.scope,
            name=self.name,
            trace_id=self.trace_id,
            correlation_id=self.correlation_id,
            id=self.event_id,
            duration_ms=duration_ms,
            error=str(exc),
            meta={**self.meta, "error_type": type(exc).__name__},
        )


def apply_terminal_state(span: Span, event: LifecycleEvent) -> None:
    """Copy the outcome carried by a closing lifecycle event onto its span.

    Internal to the observability package.

    Args:
        span: Span being closed.
        event: ``END`` or ``ERROR`` event describing the outcome.
    """
    if event.duration_ms is not None:
        span.set_attribute("duration_ms", event.duration_ms)
    if event.kind is EventKind.ERROR:
        span.set_status(StatusCode.ERROR, event.error or "")
        return
    span.set_status(StatusCode.OK)


class LoomSpan:
    """An open span whose start and end do not share a lexical scope.

    The span is **never made current**: it is neither attached to nor detached
    from the ambient OTEL context, so closing it from a different context than
    the one that opened it is safe. Its parent is captured once, at open time,
    from the context that opened it — which is the correct parent.

    Use :meth:`as_current` for an inner region that *is* lexically safe.

    Prefer :meth:`~loom.core.observability.runtime.ObservabilityRuntime.span`
    whenever the span opens and closes in one scope.

    Example::

        handle = runtime.open_span(Scope.AGENT, "agent_run", agent="analyst")
        try:
            async for frame in frames:
                yield frame
        except Exception as exc:
            handle.fail(exc)
            raise
        else:
            handle.end()
    """

    def __init__(
        self,
        *,
        span: Span,
        identity: SpanIdentity,
        emit: Callable[[LifecycleEvent], None],
        on_closed: Callable[[], None],
    ) -> None:
        self._span = span
        self._identity = identity
        self._emit = emit
        self._on_closed = on_closed
        self._started = perf_counter()
        self._closed = False

    @classmethod
    def open(
        cls,
        *,
        tracer: Tracer,
        identity: SpanIdentity,
        emit: Callable[[LifecycleEvent], None],
        on_closed: Callable[[], None],
    ) -> Self:
        """Start the OTEL span and emit its ``START`` event.

        The event is emitted with the new span current — briefly, and within
        this one call — so that a log line written by an observer carries the
        span it belongs to rather than the parent's.

        Args:
            tracer: Tracer that starts the span.
            identity: Identity shared by every event of this span.
            emit: Sink for the lifecycle events of this span.
            on_closed: Called once the span has ended, so its owner can drain
                the exporter.

        Returns:
            The open handle. The caller owns closing it.
        """
        start_event = identity.start_event()
        span = tracer.start_span(
            start_event.otel_span_name(),
            attributes=start_event.otel_attributes(),
        )
        handle = cls(span=span, identity=identity, emit=emit, on_closed=on_closed)
        with use_span(
            span, end_on_exit=False, record_exception=False, set_status_on_exception=False
        ):
            emit(start_event)
        return handle

    def as_current(self) -> AbstractContextManager[Span]:
        """Make this span current for a region that starts and ends together.

        The span is not ended on exit, and neither exception recording nor
        status setting is delegated to the context manager: a failure is
        recorded exactly once, by :meth:`fail`.

        Returns:
            Context manager yielding the underlying span.
        """
        return use_span(
            self._span,
            end_on_exit=False,
            record_exception=False,
            set_status_on_exception=False,
        )

    def end(self) -> None:
        """Close the span as a success and emit its ``END`` event.

        Calling this after the span is already closed does nothing.
        """
        self._close(self._identity.end_event(elapsed_ms(self._started)))

    def fail(self, exc: BaseException) -> None:
        """Close the span as a failure, recording *exc*, and emit ``ERROR``.

        Calling this after the span is already closed does nothing.

        Args:
            exc: Failure that ended the work this span covers.
        """
        if self._closed:
            return
        self._span.record_exception(exc)
        self._close(self._identity.error_event(exc, elapsed_ms(self._started)))

    def _close(self, event: LifecycleEvent) -> None:
        if self._closed:
            return
        self._closed = True
        apply_terminal_state(self._span, event)
        with use_span(
            self._span, end_on_exit=False, record_exception=False, set_status_on_exception=False
        ):
            self._emit(event)
        self._span.end()
        self._on_closed()


__all__ = ["LoomSpan"]
