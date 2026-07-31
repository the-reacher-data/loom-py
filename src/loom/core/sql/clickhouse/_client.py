"""Driver boundary for the ClickHouse SQL backend.

Centralizes every touch point with ``clickhouse-connect``: the guarded import
with the install hint, the structural client contract shared by the executor
and the registry, the adapter over the driver ``AsyncClient`` and the
sanitization of driver error messages (no host, DSN or stack trace).
"""

from __future__ import annotations

import re
import warnings
from collections.abc import Mapping, Sequence
from collections.abc import Set as AbstractSet
from types import ModuleType
from typing import Any, Protocol, cast

_INSTALL_HINT = (
    "The ClickHouse SQL backend requires 'clickhouse-connect'. "
    "Install it with: pip install 'loom-kernel[clickhouse]'"
)

try:
    from clickhouse_connect.driver import httputil  # type: ignore[import-untyped]
    from clickhouse_connect.driver.exceptions import (  # type: ignore[import-untyped]
        DatabaseError,
        OperationalError,
    )
except ImportError as exc:  # pragma: no cover - only without the optional extra
    raise ImportError(_INSTALL_HINT) from exc

__all__ = [
    "AsyncClickHouseClient",
    "ClickHouseClientFactory",
    "ClickHouseColumnType",
    "ClickHouseQueryResult",
    "DatabaseError",
    "OperationalError",
    "create_driver_client",
    "pool_manager",
    "sanitize_backend_error",
]

_CODE_LINE_RE = re.compile(r"Code:\s*\d+[^\n]*")
_URL_RE = re.compile(r"https?://\S+")


class ClickHouseColumnType(Protocol):
    """Driver column type exposing its native ClickHouse name."""

    @property
    def name(self) -> str:
        """Native type name (e.g. ``"DateTime64(3, 'UTC')"``)."""
        ...


class ClickHouseQueryResult(Protocol):
    """Surface of the driver query result consumed by the executor."""

    @property
    def result_rows(self) -> Sequence[Sequence[Any]]:
        """Materialized result rows."""
        ...

    @property
    def column_names(self) -> Sequence[str]:
        """Column names in result order."""
        ...

    @property
    def column_types(self) -> Sequence[ClickHouseColumnType]:
        """Column types in result order."""
        ...


class AsyncClickHouseClient(Protocol):
    """Async client surface required by the executor and the registry."""

    @property
    def valid_transport_settings(self) -> AbstractSet[str]:
        """Per-query settings the driver accepts as HTTP transport settings."""
        ...

    async def query(
        self,
        query: str,
        *,
        parameters: Mapping[str, Any] | None = None,
        settings: Mapping[str, Any] | None = None,
    ) -> ClickHouseQueryResult:
        """Run *query* with server-side *parameters* and per-query *settings*."""
        ...

    async def close(self) -> None:
        """Release the underlying connection resources."""
        ...


class ClickHouseClientFactory(Protocol):
    """Factory invoked with the exact ``get_async_client`` keyword arguments."""

    async def __call__(self, **kwargs: Any) -> AsyncClickHouseClient:
        """Create a client for one connection from driver keyword arguments."""
        ...


class _DriverAsyncClientAdapter:
    """Adapts the driver ``AsyncClient`` to :class:`AsyncClickHouseClient`.

    The driver exposes ``valid_transport_settings`` only on the wrapped sync
    client; the adapter surfaces it next to the async ``query``/``close``.
    """

    def __init__(self, async_client: Any) -> None:
        self._async_client = async_client

    @property
    def valid_transport_settings(self) -> AbstractSet[str]:
        """Per-query transport settings reported by the wrapped sync client."""
        return frozenset(self._async_client.client.valid_transport_settings)

    async def query(
        self,
        query: str,
        *,
        parameters: Mapping[str, Any] | None = None,
        settings: Mapping[str, Any] | None = None,
    ) -> ClickHouseQueryResult:
        """Delegate to the driver client (untyped boundary, hence the cast)."""
        result = await self._async_client.query(
            query=query, parameters=parameters, settings=settings
        )
        return cast(ClickHouseQueryResult, result)

    async def close(self) -> None:
        """Close the driver client and shut down its thread pool."""
        await self._async_client.close()


async def create_driver_client(**kwargs: Any) -> AsyncClickHouseClient:
    """Create an adapted driver ``AsyncClient`` from ``get_async_client`` kwargs.

    Args:
        **kwargs: Keyword arguments forwarded verbatim to
            ``clickhouse_connect.get_async_client``.

    Returns:
        The created client adapted to :class:`AsyncClickHouseClient`.

    Raises:
        ImportError: When ``clickhouse-connect`` is not installed, with the
            ``loom-kernel[clickhouse]`` install hint.
    """
    module = _import_driver()
    with warnings.catch_warnings():
        # The 0.15.x AsyncClient is a thread-pool wrapper and warns about the
        # upcoming native client; migration to 1.0 is tracked debt (spec §5).
        warnings.simplefilter("ignore", FutureWarning)
        async_client = await module.get_async_client(**kwargs)
    return _DriverAsyncClientAdapter(async_client)


def pool_manager(maxsize: int) -> Any:
    """Build a urllib3 pool manager sized for one connection (``pool_size``)."""
    return httputil.get_pool_manager(maxsize=maxsize)


def sanitize_backend_error(message: str) -> str:
    """Reduce a driver error message to a single sanitized line.

    Keeps the ``Code: NNN`` line when present (first line otherwise) and
    redacts any URL so host or DSN details never leak to callers.

    Args:
        message: Raw driver exception message.

    Returns:
        One line carrying the ClickHouse error code, safe to expose.
    """
    match = _CODE_LINE_RE.search(message)
    if match is not None:
        line = match.group(0)
    else:
        lines = message.splitlines()
        line = lines[0] if lines else "unknown backend error"
    return _URL_RE.sub("<redacted>", line).strip()


def _import_driver() -> ModuleType:
    # Local import on purpose: a missing optional extra must fail here, at
    # client creation time, with an actionable hint — even when this module
    # was imported while the extra was still available.
    try:
        import clickhouse_connect  # type: ignore[import-untyped]
    except ImportError as exc:
        raise ImportError(_INSTALL_HINT) from exc
    return cast(ModuleType, clickhouse_connect)
