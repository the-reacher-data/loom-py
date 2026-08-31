"""Single-pass JSON response for the agent endpoints.

:class:`~loom.rest.fastapi.response.MsgspecJSONResponse` raises on any type
``msgspec`` cannot encode natively, and an agent's answer is decoded from the
model's output into whatever shape the artifact declared — including backend
types that arrive through a capability. A bodyless 500 at the last moment of a
paid run is the failure mode this module removes, exactly as
``loom.rest.fastapi.sql`` had to for query results.
"""

from __future__ import annotations

import base64
from ipaddress import IPv4Address, IPv6Address
from typing import Any

import msgspec

from loom.rest.fastapi.response import MsgspecJSONResponse


def encode_exotic(obj: Any) -> str:
    """Encode values ``msgspec`` does not handle natively.

    ``msgspec`` already covers datetime, date, UUID and Decimal; this hook adds
    IPv4/IPv6 addresses, bytes as base64, and a documented ``str()`` fallback so
    an unexpected type degrades into a readable value instead of a bodyless 500.

    Args:
        obj: Value the encoder could not serialise on its own.

    Returns:
        The textual form written to the response body.
    """
    if isinstance(obj, (IPv4Address, IPv6Address)):
        return str(obj)
    if isinstance(obj, bytes):
        return base64.b64encode(obj).decode("ascii")
    return str(obj)


ENCODER = msgspec.json.Encoder(enc_hook=encode_exotic)
"""Module-level encoder shared by the JSON and the SSE surfaces: built once."""


class AgentJSONResponse(MsgspecJSONResponse):
    """Agent response encoded once by the module-level agent encoder.

    Example::

        return AgentJSONResponse(content=result)
    """

    def render(self, content: object) -> bytes:
        """Encode *content* to JSON bytes in a single pass.

        Args:
            content: Value to serialise, typically an
                :class:`~loom.ai.abc.AgentResult`.

        Returns:
            The UTF-8 encoded JSON body.
        """
        return ENCODER.encode(content)
