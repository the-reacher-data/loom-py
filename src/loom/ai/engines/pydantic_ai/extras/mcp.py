"""The MCP client (``mcp`` extra): pydantic-ai's toolset and fastmcp's stdio transport."""

from __future__ import annotations

from fastmcp.client.transports import StdioTransport
from pydantic_ai.mcp import MCPToolset

__all__ = ["MCPToolset", "StdioTransport"]
