"""The scrape server (``prometheus`` extra): the one module starting prometheus-client's server."""

from __future__ import annotations

from prometheus_client import start_http_server

__all__ = ["start"]


def start(port: int, addr: str) -> None:
    """Start the standalone ``/metrics`` server on *addr*:*port*.

    Raises:
        OSError: When the port is already in use.
    """
    start_http_server(port, addr=addr)
