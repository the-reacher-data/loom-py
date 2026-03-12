"""Base response contract type.

Users may subclass :class:`Response` for custom API output DTOs without
needing to expose transport implementation details.
"""

from __future__ import annotations

from loom.core.model.struct import LoomStruct


class Response(LoomStruct, frozen=True, kw_only=True, rename="camel"):  # type: ignore[misc]
    """Base for API response DTOs.

    Subclasses use Python ``snake_case`` attributes while JSON output uses
    ``camelCase`` by default.
    """
