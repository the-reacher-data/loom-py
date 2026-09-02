"""OpenTelemetry lifecycle observer — emits spans and optional log correlation.

Only ``opentelemetry-api`` is imported at module scope. The SDK and the OTLP
exporters are extras-only, so every import that touches them is deferred to the
call that needs them and guarded with an actionable error.
"""

from __future__ import annotations

import atexit
import logging
import os
import threading
from collections.abc import Callable, MutableMapping
from typing import TYPE_CHECKING, Any, cast

from opentelemetry import _logs as otel_logs
from opentelemetry import trace
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    StatusCode,
    TraceFlags,
    TraceState,
    set_span_in_context,
)

from loom.core.config.observability import OtelConfig
from loom.core.observability.event import EventKind, LifecycleEvent
from loom.core.observability.topology import ROOT_SCOPES, span_parent_key

if TYPE_CHECKING:  # SDK types are extras-only: never import them at runtime here.
    from opentelemetry.sdk._logs import LoggingHandler
    from opentelemetry.sdk._logs.export import LogRecordExporter

_ERR_MISSING_GRPC = "OTel protocol='grpc' requires 'opentelemetry-exporter-otlp-proto-grpc'."
_ERR_MISSING_HTTP = (
    "OTel protocol='http/protobuf' requires 'opentelemetry-exporter-otlp-proto-http'."
)
_ERR_MISSING_SDK = (
    "OTEL span and log export requires 'opentelemetry-sdk'. "
    "Install it with: pip install 'loom-py[etl-otel]'"
)
_V1_LOGS_SUFFIX = "/v1/logs"


def _load_span_exporter_cls(protocol: str) -> type[Any]:
    """Import the OTLP span exporter class for a protocol, on demand.

    The exporters are extras-only, so the import is deferred until an
    exporter is actually requested.

    Args:
        protocol: Configured OTLP protocol.

    Returns:
        The OTLP span exporter class for the protocol.

    Raises:
        ValueError: If the matching exporter package is not installed.
    """
    if protocol == "grpc":
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        except ImportError as exc:
            raise ValueError(_ERR_MISSING_GRPC) from exc
        return OTLPSpanExporter
    try:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter as HttpSpanExporter,
        )
    except ImportError as exc:
        raise ValueError(_ERR_MISSING_HTTP) from exc
    return HttpSpanExporter


def _load_log_exporter_cls(protocol: str) -> type[Any]:
    """Import the OTLP log exporter class for a protocol, on demand.

    Args:
        protocol: Resolved OTLP logs protocol.

    Returns:
        The OTLP log exporter class for the protocol.

    Raises:
        ValueError: If the protocol is unsupported or its exporter package
            is not installed.
    """
    if protocol == "grpc":
        try:
            from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
        except ImportError as exc:
            raise ValueError(_ERR_MISSING_GRPC) from exc
        return OTLPLogExporter
    if protocol != "http/protobuf":
        raise ValueError(_ERR_MISSING_HTTP)
    try:
        from opentelemetry.exporter.otlp.proto.http._log_exporter import (
            OTLPLogExporter as HttpLogExporter,
        )
    except ImportError as exc:
        raise ValueError(_ERR_MISSING_HTTP) from exc
    return HttpLogExporter


class OtelLifecycleObserver:
    """Lifecycle observer that emits OpenTelemetry spans for every ``span()`` call.

    Each ``START`` event opens a span keyed by ``scope:name:trace_id``. The
    matching ``END`` or ``ERROR`` event closes it. Concurrent spans with
    different ``trace_id`` values are tracked independently.

    When the runtime enables log export, it installs a structlog processor
    that adds the active OTEL trace and span IDs to every log entry.

    Args:
        config: OTLP exporter configuration.
    """

    def __init__(self, config: OtelConfig) -> None:
        config.validate()
        self._tracer, self._provider = _build_tracer(config)
        self._spans = _SpanRegistry()

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

    def _open_span(self, event: LifecycleEvent) -> None:
        attrs = event.otel_attributes()

        parent_span = self._spans.get(span_parent_key(event.scope, event.trace_id))
        parent_ctx = (
            set_span_in_context(parent_span)
            if parent_span is not None
            else _trace_parent_context(event.trace_id)
        )

        span = self._tracer.start_span(
            event.otel_span_name(),
            context=parent_ctx,
            attributes=attrs,
        )
        self._spans.put(_span_key(event), span)

    def _close_span(self, event: LifecycleEvent, *, ok: bool) -> None:
        span = self._spans.pop(_span_key(event))
        if span is None:
            return
        if event.duration_ms is not None:
            span.set_attribute("duration_ms", event.duration_ms)
        if ok:
            span.set_status(StatusCode.OK)
        else:
            span.set_status(StatusCode.ERROR, event.error or "")
        span.end()
        if (
            event.scope in ROOT_SCOPES
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


def _build_tracer(config: OtelConfig) -> tuple[Any, Any | None]:
    if not config.endpoint.strip():
        return trace.get_tracer(config.tracer_name, config.tracer_version or None), None

    exporter = _build_exporter(config)
    try:
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError as exc:
        raise ImportError(_ERR_MISSING_SDK) from exc

    resource_attrs = {SERVICE_NAME: config.service_name, **config.resource_attributes}
    provider = TracerProvider(resource=Resource.create(resource_attrs))
    provider.add_span_processor(BatchSpanProcessor(exporter, **config.span_processor_kwargs))
    tracer = provider.get_tracer(config.tracer_name, config.tracer_version or None)
    return tracer, provider


def _build_exporter(config: OtelConfig) -> Any:
    kwargs: dict[str, Any] = {"endpoint": config.endpoint, **config.exporter_kwargs}
    if config.headers:
        kwargs["headers"] = dict(config.headers)
    exporter_cls = _load_span_exporter_cls(config.protocol)
    if config.protocol == "grpc":
        kwargs["insecure"] = config.insecure
    return exporter_cls(**kwargs)


def _trace_parent_context(trace_id: str | None) -> Any | None:
    if trace_id is None:
        return None
    try:
        trace_id_int = int(trace_id, 16)
    except ValueError:
        return None
    if trace_id_int == 0:
        return None
    parent = NonRecordingSpan(
        SpanContext(
            trace_id=trace_id_int,
            span_id=1,
            is_remote=True,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
            trace_state=TraceState(),
        )
    )
    return set_span_in_context(parent)


def build_log_correlation_processor() -> Callable[
    [Any, str, MutableMapping[str, Any]],
    MutableMapping[str, Any],
]:
    """Build a structlog processor that adds the active OTEL span IDs.

    Returns:
        A processor function compatible with ``structlog``.
    """

    def _processor(
        logger: Any,
        method: str,
        event_dict: MutableMapping[str, Any],
    ) -> MutableMapping[str, Any]:
        span = trace.get_current_span()
        if span.is_recording():
            ctx = span.get_span_context()
            event_dict.setdefault("otel_trace_id", format(ctx.trace_id, "032x"))
            event_dict.setdefault("otel_span_id", format(ctx.span_id, "016x"))
        return event_dict

    return _processor


_LOG_EXPORT_STATE: tuple[Any, LoggingHandler] | None = None


def install_otel_log_export(
    config: OtelConfig,
    *,
    exporter: LogRecordExporter | None = None,
) -> LoggingHandler | None:
    """Install a stdlib log handler that exports OTEL logs.

    The helper is idempotent for the current process. Subsequent calls return
    the already-installed handler.

    Args:
        config: OTLP exporter configuration.
        exporter: Optional pre-built log record exporter. When omitted, one is
            built from ``config``.

    Returns:
        The installed logging handler.

    Raises:
        ImportError: If ``opentelemetry-sdk`` is not installed.
        ValueError: If the configured protocol has no installed OTLP exporter.
    """
    global _LOG_EXPORT_STATE
    config.validate()
    if _LOG_EXPORT_STATE is not None:
        return _LOG_EXPORT_STATE[1]

    try:
        from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    except ImportError as exc:
        raise ImportError(_ERR_MISSING_SDK) from exc

    log_exporter = exporter or _build_log_exporter(config)
    provider = LoggerProvider(
        resource=Resource.create(
            {
                SERVICE_NAME: config.service_name,
                **config.resource_attributes,
            }
        )
    )
    provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
    otel_logs.set_logger_provider(provider)

    handler = LoggingHandler(level=logging.NOTSET, logger_provider=provider)
    logging.getLogger().addHandler(handler)
    atexit.register(provider.shutdown)
    _LOG_EXPORT_STATE = (provider, handler)
    return handler


def _build_log_exporter(config: OtelConfig) -> LogRecordExporter:
    kwargs: dict[str, Any] = {**config.exporter_kwargs}
    endpoint = _resolve_log_endpoint(config)
    if endpoint is not None:
        kwargs["endpoint"] = endpoint
    if config.headers:
        kwargs["headers"] = dict(config.headers)

    protocol = _resolve_log_protocol(config)
    exporter_cls = _load_log_exporter_cls(protocol)
    if protocol == "grpc":
        kwargs["insecure"] = config.insecure
    return cast("LogRecordExporter", exporter_cls(**kwargs))


def _resolve_log_endpoint(config: OtelConfig) -> str | None:
    env_endpoint = os.getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "").strip()
    if env_endpoint:
        return env_endpoint

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", config.endpoint).strip()
    if not endpoint:
        return None
    if endpoint.endswith("/v1/traces"):
        return endpoint[: -len("/v1/traces")] + _V1_LOGS_SUFFIX
    if endpoint.endswith(_V1_LOGS_SUFFIX):
        return endpoint
    return endpoint.rstrip("/") + _V1_LOGS_SUFFIX


def _resolve_log_protocol(config: OtelConfig) -> str:
    env_protocol = os.getenv("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL", "").strip()
    if env_protocol:
        return env_protocol
    return os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL", config.protocol).strip()


__all__ = [
    "OtelLifecycleObserver",
    "build_log_correlation_processor",
    "install_otel_log_export",
]
