"""FastAPI-specific REST layer for Loom.

Provides the FastAPI binding for compiled REST interfaces:
- :class:`~loom.rest.fastapi.response.MsgspecJSONResponse` — zero-copy JSON response.
- :func:`~loom.rest.fastapi.router_runtime.bind_interfaces` — binds compiled routes to FastAPI.
- :func:`~loom.rest.fastapi.app.create_fastapi_app` — composition root.
"""

from loom.rest.fastapi.app import create_fastapi_app
from loom.rest.fastapi.auto import create_app
from loom.rest.fastapi.response import MsgspecJSONResponse
from loom.rest.fastapi.router_runtime import bind_interfaces

__all__ = [
    "MsgspecJSONResponse",
    "bind_interfaces",
    "create_app",
    "create_fastapi_app",
]
