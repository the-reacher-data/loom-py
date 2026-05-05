"""OpenTelemetry lifecycle observer — emits spans and optional log correlation."""

from __future__ import annotations

import logging
import threading
from importlib.util import find_spec
from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import StatusCode, set_span_in_context

from loom.core.config.observability import OtelConfig
from loom.core.observability.event import EventKind, LifecycleEvent

_logger = logging.getLogger(__name__)

# Scopes that represent root units of work — their spans are force-flushed on END
# so short-lived runners (ETL jobs, test suites) don't lose spans at process exit.
_ROOT_SCOPES: frozenset[str] = frozenset(
    {"use_case", "job", "poll_cycle", "pipeline", "maintenance"}
)

if find_spec("opentelemetry.exporter.otlp.proto.grpc.trace_exporter") is not None:
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter as _GrpcExporter,
    )

    _grpc_exporter_cls: type[Any] | None = _GrpcExporter
else:
    _grpc_exporter_cls = None

if find_spec("opentelemetry.exporter.otlp.proto.http.trace_exporter") is not None:
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
        OTLPSpanExporter as _HttpExporter,
    )

    _http_exporter_cls: type[Any] | None = _HttpExporter
else:
    _http_exporter_cls = None


class OtelLifecycleObserver:
    """Lifecycle observer that emits OpenTelemetry spans for every ``span()`` call.

    Each ``START`` event opens a span keyed by ``scope:name:trace_id``. The
    matching ``END`` or ``ERROR`` event closes it. Concurrent spans with
    different ``trace_id`` values are tracked independently.

    When ``export_logs=True``, a structlog processor is installed that adds
    the active OTEL trace and span IDs to every log entry, correlating logs
    with traces in the same collector.

    Args:
        config: OTLP exporter configuration.
        export_logs: Install the OTEL log-correlation structlog processor.
    """

    def __init__(self, config: OtelConfig, *, export_logs: bool = False) -> None:
        config.validate()
        self._tracer, self._provider = _build_tracer(config)
        self._spans = _SpanRegistry()
        if export_logs:
            _install_log_correlation()

    def on_event(self, event: LifecycleEvent) -> None:
        """Handle one lifecycle event.

        Args:
            event: Lifecycle event from the runtime.
        """
        match event.kind:
            case EventKind.START:
                self._open_span(event)
            case EventKind.END:
                self._close_span(event, ok=True)
            case EventKind.ERROR:
                self._close_span(event, ok=False)
            case _:
                pass

    def _open_span(self, event: LifecycleEvent) -> None:
        attrs: dict[str, Any] = {"loom.scope": event.scope}
        if event.trace_id:
            attrs["loom.trace_id"] = event.trace_id
        if event.correlation_id:
            attrs["loom.correlation_id"] = event.correlation_id
        attrs.update({f"loom.meta.{k}": str(v) for k, v in event.meta.items()})

        parent_span = self._spans.get(_parent_key(event.scope, event.trace_id))
        parent_ctx = set_span_in_context(parent_span) if parent_span is not None else None

        span = self._tracer.start_span(
            f"loom.{event.scope}.{event.name}",
            context=parent_ctx,
            attributes=attrs,
        )
        self._spans.put(_span_key(event), span)

    def _close_span(self, event: LifecycleEvent, *, ok: bool) -> None:
        span = self._spans.pop(_span_key(event))
        if span is None:
            return
        if event.duration_ms is not None:
            span.set_attribute("loom.duration_ms", event.duration_ms)
        if ok:
            span.set_status(StatusCode.OK)
        else:
            span.set_status(StatusCode.ERROR, event.error or "")
        span.end()
        if (
            event.scope in _ROOT_SCOPES
            and self._provider is not None
            and hasattr(self._provider, "force_flush")
        ):
            self._provider.force_flush()


class _SpanRegistry:
    """Thread-safe in-memory registry mapping span keys to active spans."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._spans: dict[str, Any] = {}

    def put(self, key: str, span: Any) -> None:
        with self._lock:
            self._spans[key] = span

    def get(self, key: str) -> Any | None:
        with self._lock:
            return self._spans.get(key)

    def pop(self, key: str) -> Any | None:
        with self._lock:
            return self._spans.pop(key, None)


def _span_key(event: LifecycleEvent) -> str:
    return f"{event.scope}:{event.name}:{event.trace_id or ''}"


def _parent_key(scope: str, trace_id: str | None) -> str:
    """Return the key of the parent scope for ``scope``, if any."""
    _PARENT: dict[str, str] = {
        "node": "poll_cycle",
        "batch_collect": "poll_cycle",
        "batch_write": "poll_cycle",
        "transport": "use_case",
    }
    parent_scope = _PARENT.get(scope)
    if parent_scope is None:
        return ""
    return f"{parent_scope}::{trace_id or ''}"


def _build_tracer(config: OtelConfig) -> tuple[Any, Any | None]:
    if not config.endpoint.strip():
        return trace.get_tracer(config.tracer_name, config.tracer_version or None), None

    exporter = _build_exporter(config)
    resource_attrs = {SERVICE_NAME: config.service_name, **config.resource_attributes}
    provider = TracerProvider(resource=Resource.create(resource_attrs))
    provider.add_span_processor(BatchSpanProcessor(exporter, **config.span_processor_kwargs))
    tracer = provider.get_tracer(config.tracer_name, config.tracer_version or None)
    return tracer, provider


def _build_exporter(config: OtelConfig) -> Any:
    kwargs: dict[str, Any] = {"endpoint": config.endpoint, **config.exporter_kwargs}
    if config.headers:
        kwargs["headers"] = dict(config.headers)
    if config.protocol == "grpc":
        if _grpc_exporter_cls is None:
            raise ValueError(
                "OTel protocol='grpc' requires 'opentelemetry-exporter-otlp-proto-grpc'."
            )
        kwargs["insecure"] = config.insecure
        return _grpc_exporter_cls(**kwargs)
    if _http_exporter_cls is None:
        raise ValueError(
            "OTel protocol='http/protobuf' requires 'opentelemetry-exporter-otlp-proto-http'."
        )
    return _http_exporter_cls(**kwargs)


def _install_log_correlation() -> None:
    """Install a structlog processor that adds OTEL trace/span IDs to log entries."""
    try:
        from collections.abc import MutableMapping

        import structlog
        from opentelemetry import trace as otel_trace

        def _otel_correlation_processor(
            logger: Any,
            method: str,
            event_dict: MutableMapping[str, Any],
        ) -> MutableMapping[str, Any]:
            span = otel_trace.get_current_span()
            if span.is_recording():
                ctx = span.get_span_context()
                event_dict.setdefault("otel_trace_id", format(ctx.trace_id, "032x"))
                event_dict.setdefault("otel_span_id", format(ctx.span_id, "016x"))
            return event_dict

        existing = structlog.get_config().get("processors", [])
        if _otel_correlation_processor not in existing:
            structlog.configure(processors=[_otel_correlation_processor, *existing])
    except Exception:
        _logger.warning(
            "otel_log_correlation_failed",
            exc_info=True,
        )


__all__ = ["OtelLifecycleObserver"]
