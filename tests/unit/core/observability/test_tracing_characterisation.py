"""Characterisation corpus for the OTEL tracing defects — pins CURRENT behaviour.

Every assertion here describes what ``OtelLifecycleObserver`` does **today**,
including what it does wrong. The defects are real and known; they are pinned
so that the follow-up PR that fixes them has to invert each assertion
explicitly, which is the evidence that the fix landed.

Do not "fix" these expectations without fixing the production code in the same
change.

Spans are exported through a real ``TracerProvider`` with a
``SimpleSpanProcessor`` and an ``InMemorySpanExporter``, and every assertion is
about exported span structure — trace ids, parent span ids, span counts. A
``ProxyTracerProvider`` (what ``endpoint=""`` yields in production) produces
non-recording spans that can never fail such assertions, which is exactly how
these defects shipped.
"""

from __future__ import annotations

from typing import Any

import pytest
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from loom.core.config.observability import OtelConfig
from loom.core.observability.event import LifecycleEvent, Scope
from loom.core.observability.observer import otel as _otel_module
from loom.core.observability.observer.otel import OtelLifecycleObserver
from loom.core.observability.topology import span_parent_key

_TRACE_ID_HEX = "4b3f9a1c2d8e0f7b6a5c3e1d9f2b4a0c"
_OTHER_TRACE_ID_HEX = "0f1e2d3c4b5a69788796a5b4c3d2e1f0"

# ``_trace_parent_context`` fabricates this span id for the remote parent it
# invents from the business trace id (observer/otel.py, E5).
_FABRICATED_PARENT_SPAN_ID = 1


@pytest.fixture
def recorded_observer(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[OtelLifecycleObserver, InMemorySpanExporter]:
    """Build a real-SDK observer whose spans land in an in-memory exporter."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("loom.characterisation")

    def _fake_build_tracer(config: OtelConfig) -> tuple[Any, Any]:
        del config
        return tracer, provider

    monkeypatch.setattr(_otel_module, "_build_tracer", _fake_build_tracer)
    observer = OtelLifecycleObserver(OtelConfig(endpoint="", service_name="loom-characterisation"))
    return observer, exporter


def _span_by_name(spans: tuple[ReadableSpan, ...], name: str) -> ReadableSpan:
    matches = [span for span in spans if span.name == name]
    assert len(matches) == 1, f"expected exactly one {name!r} span, got {len(matches)}"
    return matches[0]


def _span_id(span: ReadableSpan) -> int:
    context = span.get_span_context()
    assert context is not None
    return context.span_id


def _trace_id(span: ReadableSpan) -> int:
    context = span.get_span_context()
    assert context is not None
    return context.trace_id


def _open_and_close(observer: OtelLifecycleObserver, **event_kwargs: Any) -> None:
    observer.on_event(LifecycleEvent.start(**event_kwargs))
    observer.on_event(LifecycleEvent.end(**event_kwargs))


class TestE2ParentingNeverHappens:
    """E2 — the registry key written and the key read never match.

    ``_span_key`` stores ``f"{scope}:{name}:{trace_id}"`` while
    ``span_parent_key`` looks up ``f"{parent_scope}::{trace_id}"`` with an
    empty name segment, so a child scope never finds its parent span.
    """

    def test_lookup_key_cannot_match_any_stored_key(self) -> None:
        parent_event = LifecycleEvent.start(
            scope=Scope.POLL_CYCLE, name="orders", trace_id=_TRACE_ID_HEX
        )
        child_event = LifecycleEvent.start(
            scope=Scope.NODE, name="transform", trace_id=_TRACE_ID_HEX
        )

        stored = _otel_module._span_key(parent_event)
        looked_up = span_parent_key(child_event.scope, child_event.trace_id)

        # BROKEN TODAY. A later PR makes these equal; then this assertion inverts.
        assert stored == f"poll_cycle:orders:{_TRACE_ID_HEX}"
        assert looked_up == f"poll_cycle::{_TRACE_ID_HEX}"
        assert stored != looked_up

    def test_child_scope_is_not_parented_to_its_open_parent_scope(
        self, recorded_observer: tuple[OtelLifecycleObserver, InMemorySpanExporter]
    ) -> None:
        observer, exporter = recorded_observer

        observer.on_event(
            LifecycleEvent.start(scope=Scope.POLL_CYCLE, name="orders", trace_id=_TRACE_ID_HEX)
        )
        _open_and_close(observer, scope=Scope.NODE, name="transform", trace_id=_TRACE_ID_HEX)
        observer.on_event(
            LifecycleEvent.end(scope=Scope.POLL_CYCLE, name="orders", trace_id=_TRACE_ID_HEX)
        )

        spans = exporter.get_finished_spans()
        assert len(spans) == 2
        poll_cycle = _span_by_name(spans, "poll_cycle:orders")
        node = _span_by_name(spans, "node:transform")

        # BROKEN TODAY: the node span should be a child of the poll_cycle span.
        # Instead both hang off the same fabricated remote parent (see E5), so
        # the collector shows a flat pair, not a tree. A later PR inverts this
        # to ``node.parent.span_id == _span_id(poll_cycle)``.
        assert node.parent is not None
        assert node.parent.span_id != _span_id(poll_cycle)
        assert node.parent.span_id == _FABRICATED_PARENT_SPAN_ID
        assert poll_cycle.parent is not None
        assert poll_cycle.parent.span_id == _FABRICATED_PARENT_SPAN_ID

    def test_without_a_trace_id_parent_and_child_become_two_unrelated_traces(
        self, recorded_observer: tuple[OtelLifecycleObserver, InMemorySpanExporter]
    ) -> None:
        """This is what the Bytewax runner actually emits: POLL_CYCLE with no trace id."""
        observer, exporter = recorded_observer

        observer.on_event(LifecycleEvent.start(scope=Scope.POLL_CYCLE, name="orders"))
        _open_and_close(observer, scope=Scope.NODE, name="transform")
        observer.on_event(LifecycleEvent.end(scope=Scope.POLL_CYCLE, name="orders"))

        spans = exporter.get_finished_spans()
        assert len(spans) == 2
        poll_cycle = _span_by_name(spans, "poll_cycle:orders")
        node = _span_by_name(spans, "node:transform")

        # BROKEN TODAY: two roots in two different traces — a poll cycle and
        # the node it ran cannot be correlated at all. A later PR inverts this
        # to one trace with the node parented to the poll cycle.
        assert poll_cycle.parent is None
        assert node.parent is None
        assert _trace_id(poll_cycle) != _trace_id(node)


class TestE7SpanKeyCollision:
    """E7 — the registry key carries no per-run identity.

    Two concurrent runs of the same ``(scope, name, trace_id)`` share one key in
    the process-wide ``_SpanRegistry``, so the second START overwrites the
    first, and the first END closes the survivor. One span is exported instead
    of two, and the lost one is never exported at all.
    """

    def test_concurrent_runs_of_the_same_scope_and_name_overwrite_each_other(
        self, recorded_observer: tuple[OtelLifecycleObserver, InMemorySpanExporter]
    ) -> None:
        observer, exporter = recorded_observer

        observer.on_event(
            LifecycleEvent.start(
                scope=Scope.POLL_CYCLE, name="orders", trace_id=_TRACE_ID_HEX, id="run-a"
            )
        )
        observer.on_event(
            LifecycleEvent.start(
                scope=Scope.POLL_CYCLE, name="orders", trace_id=_TRACE_ID_HEX, id="run-b"
            )
        )
        observer.on_event(
            LifecycleEvent.end(
                scope=Scope.POLL_CYCLE, name="orders", trace_id=_TRACE_ID_HEX, id="run-a"
            )
        )
        observer.on_event(
            LifecycleEvent.end(
                scope=Scope.POLL_CYCLE, name="orders", trace_id=_TRACE_ID_HEX, id="run-b"
            )
        )

        spans = exporter.get_finished_spans()

        # BROKEN TODAY: two runs went in, one span came out — and it is run-b's
        # span, closed by run-a's END event. Run-a's span is leaked, never
        # ended, never exported. A later PR keys spans per run and inverts this
        # to two exported spans, one per run id.
        assert len(spans) == 1
        assert spans[0].attributes is not None
        assert spans[0].attributes["id"] == "run-b"
        exported_ids = {span.attributes["id"] for span in spans if span.attributes is not None}
        assert "run-a" not in exported_ids


class TestE5DanglingParent:
    """E5 — the parent span id is fabricated, so it points at nothing.

    ``_trace_parent_context`` builds a remote ``SpanContext`` from the business
    trace id (a uuid4 hex) with a hardcoded ``span_id=1``. No such span exists
    anywhere, so the collector shows every root-level Loom span as a child of a
    span it will never receive.
    """

    def test_exported_span_points_at_a_parent_that_was_never_exported(
        self, recorded_observer: tuple[OtelLifecycleObserver, InMemorySpanExporter]
    ) -> None:
        observer, exporter = recorded_observer

        _open_and_close(observer, scope=Scope.NODE, name="transform", trace_id=_TRACE_ID_HEX)
        _open_and_close(observer, scope=Scope.NODE, name="enrich", trace_id=_OTHER_TRACE_ID_HEX)

        spans = exporter.get_finished_spans()
        assert len(spans) == 2
        exported_span_ids = {_span_id(span) for span in spans}

        # BROKEN TODAY: every span claims a parent that does not exist. A later
        # PR makes the business trace id the span's own trace id instead of
        # inventing a parent, and inverts this to ``span.parent is None``.
        for span in spans:
            assert span.parent is not None
            assert span.parent.span_id == _FABRICATED_PARENT_SPAN_ID
            assert span.parent.span_id not in exported_span_ids
            assert span.parent.is_remote is True

        transform = _span_by_name(spans, "node:transform")
        assert _trace_id(transform) == int(_TRACE_ID_HEX, 16)
