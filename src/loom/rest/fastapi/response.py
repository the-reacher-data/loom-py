"""MsgspecJSONResponse — zero-copy JSON response for FastAPI.

Uses ``msgspec.json.encode`` to serialise response content directly to bytes,
bypassing the stdlib ``json`` module and Pydantic's serialisation pipeline.
No double-serialisation occurs: the bytes are written to the response body
exactly once.

Supports any type that ``msgspec.json.encode`` accepts: plain Python types
(``dict``, ``list``, ``str``, ``int``, etc.), ``msgspec.Struct`` instances,
and ``dataclasses``.

Usage::

    from loom.rest.fastapi.response import MsgspecJSONResponse

    @app.get("/items/{item_id}")
    async def get_item(item_id: int) -> MsgspecJSONResponse:
        item = await repo.get(item_id)
        return MsgspecJSONResponse(content=item)
"""

from __future__ import annotations

import msgspec
from starlette.responses import Response


class MsgspecJSONResponse(Response):
    """FastAPI ``Response`` subclass that encodes content with ``msgspec.json``.

    Drop-in replacement for ``fastapi.responses.JSONResponse`` with two
    advantages:

    - Native ``msgspec.Struct`` serialisation — no ``dict`` conversion needed.
    - Single encoding pass — content is written directly to bytes with no
      intermediate JSON string or Pydantic round-trip.

    Args:
        content: Any object supported by ``msgspec.json.encode``.
        status_code: HTTP status code.  Defaults to ``200``.
        headers: Additional response headers.
        media_type: Defaults to ``"application/json"``.
        background: Optional Starlette background task.

    Example::

        return MsgspecJSONResponse(content=my_struct, status_code=201)
    """

    media_type = "application/json"

    def render(self, content: object) -> bytes:
        """Encode ``content`` to JSON bytes using ``msgspec.json.encode``.

        Args:
            content: Object to serialise.

        Returns:
            UTF-8 encoded JSON bytes.
        """
        return msgspec.json.encode(content)
