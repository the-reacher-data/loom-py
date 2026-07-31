"""Native stateless JWT authentication for the Loom REST layer.

Provides :class:`JwtAuthConfig` (fail-fast validated settings bound from the
``app.rest.auth.jwt`` config section) and :class:`JwtAuthMiddleware` (pure
ASGI bearer-token verification with claims exposed on
``scope["state"]["jwt_claims"]``).

Install the optional dependency with::

    pip install "loom-kernel[jwt]"
"""

from loom.rest.auth.config import JwtAuthConfig
from loom.rest.auth.middleware import JwtAuthMiddleware

__all__ = [
    "JwtAuthConfig",
    "JwtAuthMiddleware",
]
