"""Base response contract type.

Users may subclass :class:`Response` for custom API output DTOs without
needing to expose transport implementation details.
"""

from __future__ import annotations

import msgspec


class Response(msgspec.Struct, frozen=True, kw_only=True, omit_defaults=True, rename="camel"):
    """Base for API response DTOs.

    Subclasses use Python ``snake_case`` attributes while JSON output uses
    ``camelCase`` by default.
    """
