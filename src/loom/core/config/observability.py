"""Shared OpenTelemetry configuration objects used by Loom runtimes."""

from __future__ import annotations

import warnings
from typing import Any
from urllib.parse import urlparse

from loom.core.model import LoomFrozenStruct

_LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1"}


def _is_local_endpoint(endpoint: str) -> bool:
    return (urlparse(endpoint).hostname or "") in _LOCAL_HOSTS


class OtelConfig(LoomFrozenStruct, frozen=True):
    """OpenTelemetry SDK/exporter configuration.

    Args:
        service_name: Resource attribute ``service.name``.
        tracer_name: Tracer instrumentation name.
        tracer_version: Optional tracer instrumentation version.
        protocol: OTLP protocol (``http/protobuf`` or ``grpc``).
        endpoint: OTLP endpoint URI. When empty, uses global OTel runtime defaults.
        insecure: Exporter transport mode when supported by protocol/exporter.
        headers: Exporter request headers (vendor auth/tags).
        resource_attributes: Additional OTel resource attributes.
        span_attributes: Static span attributes added to all spans emitted by this observer.
        exporter_kwargs: Extra keyword args passed through to OTLP exporter.
        span_processor_kwargs: Extra keyword args passed through to BatchSpanProcessor.
    """

    service_name: str = "loom"
    tracer_name: str = "loom"
    tracer_version: str = ""
    protocol: str = "http/protobuf"
    endpoint: str = ""
    insecure: bool = False
    headers: dict[str, str] = {}
    resource_attributes: dict[str, str] = {}
    span_attributes: dict[str, str] = {}
    exporter_kwargs: dict[str, Any] = {}
    span_processor_kwargs: dict[str, Any] = {}

    def validate(self) -> None:
        """Validate the configured transport protocol.

        Raises:
            ValueError: If protocol is unsupported.
        """
        if self.protocol not in {"http/protobuf", "grpc"}:
            raise ValueError(
                "observability.otel_config.protocol must be either 'http/protobuf' or 'grpc'."
            )
        if self.insecure and self.endpoint and not _is_local_endpoint(self.endpoint):
            warnings.warn(
                f"OtelConfig.insecure=True on non-local endpoint {self.endpoint!r}."
                " Set insecure=False for production deployments.",
                UserWarning,
                stacklevel=2,
            )


__all__ = ["OtelConfig"]
