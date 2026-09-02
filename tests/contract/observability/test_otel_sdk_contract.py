"""Contract tests for OpenTelemetry SDK behaviour that Loom relies on.

These pin assumptions about a third-party SDK, not about Loom code. They exist
because the assumptions are undocumented: if a future SDK release breaks them,
the symptom in production is silent — correct-looking traces carrying random
trace ids — so the breakage must surface here instead.
"""

from __future__ import annotations

from importlib.metadata import version

from opentelemetry.context import Context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.id_generator import IdGenerator

_FIXED_TRACE_ID = 0xABC
_FIXED_SPAN_ID = 0xDEF


class _FixedIdGenerator(IdGenerator):
    """Id generator returning constant ids so span provenance is unambiguous."""

    def generate_span_id(self) -> int:
        return _FIXED_SPAN_ID

    def generate_trace_id(self) -> int:
        return _FIXED_TRACE_ID


def _provider_with_memory_exporter() -> tuple[TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


def _root_trace_id(exporter: InMemorySpanExporter) -> int:
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    span_context = spans[0].get_span_context()
    assert span_context is not None
    return span_context.trace_id


class TestTracerProviderIdGenerator:
    """Pins how ``TracerProvider.id_generator`` behaves when assigned post-construction.

    Loom maps a business trace id onto the OTEL trace id by installing its own
    id generator on an already-constructed provider. ``id_generator`` is a
    public, mutable attribute, but the SDK documents neither that it may be
    replaced after construction nor when it is read.
    """

    def test_assignment_before_get_tracer_drives_the_next_root_span(self) -> None:
        provider, exporter = _provider_with_memory_exporter()

        provider.id_generator = _FixedIdGenerator()
        tracer = provider.get_tracer("loom.contract")

        # An empty Context() forces a root span: a parent-derived trace id
        # would mask a cached generator.
        tracer.start_span("root", context=Context()).end()

        assert _root_trace_id(exporter) == _FIXED_TRACE_ID, (
            f"opentelemetry-sdk {version('opentelemetry-sdk')} no longer honours an "
            "id_generator assigned onto a constructed TracerProvider before get_tracer(). "
            f"The root span got {_root_trace_id(exporter):032x} instead of "
            f"{_FIXED_TRACE_ID:032x}. Loom depends on this to make the business trace id "
            "the OTEL trace id; if the SDK stops reading the attribute, trace ids silently "
            "revert to random values and cross-service correlation breaks with no error."
        )

    def test_assignment_after_get_tracer_does_not_affect_existing_tracers(self) -> None:
        """``get_tracer`` snapshots the generator — the window for swapping it is narrow.

        This is the trap the previous test guards: any caller that installs a
        custom generator must do so before taking a tracer off the provider.
        """
        provider, exporter = _provider_with_memory_exporter()

        tracer = provider.get_tracer("loom.contract")
        provider.id_generator = _FixedIdGenerator()

        tracer.start_span("root", context=Context()).end()

        assert _root_trace_id(exporter) != _FIXED_TRACE_ID, (
            f"opentelemetry-sdk {version('opentelemetry-sdk')} now reads "
            "TracerProvider.id_generator dynamically instead of snapshotting it in "
            "get_tracer(). That is a safe relaxation, not a regression: this test can be "
            "dropped once the supported SDK floor guarantees the new behaviour."
        )
