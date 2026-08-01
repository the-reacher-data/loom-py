"""Stateless JWT bearer authentication mechanism.

Verifies the token and projects its verified claims onto an
:class:`~loom.core.identity.identity.Identity`.  Everything downstream — role
resolution, business policies — consumes that identity and never learns a JWT
was involved.

Install the optional dependency with::

    pip install "loom-kernel[jwt]"
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from types import ModuleType
from typing import Any

from loom.core.identity import Identity
from loom.rest.auth.abc import RequestCredentials
from loom.rest.auth.config import JwtAuthConfig

_logger = logging.getLogger(__name__)

MECHANISM_NAME = "jwt"
"""Label recorded on every identity this mechanism issues."""

_AUTHORIZATION_HEADER = "authorization"
_BEARER_SCHEME = "bearer"
_SUBJECT_CLAIM = "sub"

# Registered claims (RFC 7519 §4.1) describe the token, not the caller, so they
# never reach the identity attributes.
_PROTOCOL_CLAIMS = frozenset({"iss", "sub", "aud", "exp", "nbf", "iat", "jti"})

_PYJWT_HINT = (
    "JWT authentication requires the optional dependency 'pyjwt'. "
    "Install it with: pip install 'loom-kernel[jwt]'"
)


class JwtAuthenticator:
    """Authenticates callers from a stateless JWT bearer token.

    Verification is fully stateless: no server-side session storage and no
    remote JWKS fetch.  Signature, ``exp`` and ``sub`` are always required
    (a token without a subject carries no identity to bind an authorization
    decision to, nor to audit afterwards); ``aud``/``iss`` are validated only
    when configured.

    Args:
        config: Validated JWT settings.

    Raises:
        ImportError: If the optional ``pyjwt`` dependency is not installed.

    Example::

        authenticator = JwtAuthenticator(
            JwtAuthConfig(secret="...", algorithms=("HS256",), roles_claim="loom_sql_roles")
        )
    """

    def __init__(self, config: JwtAuthConfig) -> None:
        self._config = config
        self._jwt = _load_pyjwt()
        self._key = config.verification_key
        self._algorithms = list(config.algorithms)
        self._roles_claim = config.roles_claim
        self._decode_options: dict[str, Any] = {
            "require": ["exp", _SUBJECT_CLAIM],
            "verify_aud": config.audience is not None,
        }

    @property
    def name(self) -> str:
        """Return the mechanism label recorded on issued identities."""
        return MECHANISM_NAME

    @property
    def provides_roles(self) -> bool:
        """Whether a verified claim binds roles to the caller identity."""
        return self._roles_claim is not None

    async def authenticate(self, credentials: RequestCredentials) -> Identity | None:
        """Verify the bearer token and project its claims onto an identity.

        Args:
            credentials: Headers and path of the request.

        Returns:
            The verified identity, or ``None`` when the header is absent, uses
            another scheme, or the token fails verification.
        """
        token = _bearer_token(credentials.header(_AUTHORIZATION_HEADER))
        if token is None:
            return None
        claims = self._decode(token)
        if claims is None:
            return None
        return self._to_identity(claims)

    def _decode(self, token: str) -> Mapping[str, Any] | None:
        try:
            claims: dict[str, Any] = self._jwt.decode(
                token,
                self._key,
                algorithms=self._algorithms,
                audience=self._config.audience,
                issuer=self._config.issuer,
                leeway=self._config.leeway_seconds,
                options=self._decode_options,
            )
        except self._jwt.PyJWTError as exc:
            # DEBUG only, and never the token itself: no oracle in responses/logs.
            _logger.debug("JWT verification failed: %s: %s", type(exc).__name__, exc)
            return None
        return claims

    def _to_identity(self, claims: Mapping[str, Any]) -> Identity | None:
        subject = claims.get(_SUBJECT_CLAIM)
        if not isinstance(subject, str) or not subject:
            return None
        return Identity(
            subject=subject,
            roles=_roles_from_claim(claims.get(self._roles_claim)) if self._roles_claim else (),
            attributes=_attributes(claims, self._roles_claim),
            mechanism=MECHANISM_NAME,
        )


def _load_pyjwt() -> ModuleType:
    """Import and return :mod:`jwt`, failing fast with an actionable hint.

    The import is local on purpose: ``pyjwt`` is an optional extra, and
    resolving it at authenticator construction turns a missing dependency into
    a startup error instead of a broken API at first request.

    Returns:
        The imported ``jwt`` module.

    Raises:
        ImportError: If ``pyjwt`` is not installed.
    """
    try:
        import jwt
    except ImportError as exc:
        raise ImportError(_PYJWT_HINT) from exc
    return jwt


def _bearer_token(header: str | None) -> str | None:
    """Return the ``Bearer`` token carried by an ``Authorization`` header."""
    if header is None:
        return None
    scheme, _, token = header.strip().partition(" ")
    token = token.strip()
    if scheme.lower() != _BEARER_SCHEME or not token:
        return None
    return token


def _roles_from_claim(value: Any) -> tuple[str, ...]:
    """Read the roles claim as ``str`` or ``list[str]``, refusing anything else.

    Values are never coerced and a malformed claim yields no role at all: a
    list holding one valid role and one number is a broken token, not a
    partially authorized caller.
    """
    if isinstance(value, str):
        return (value,) if value else ()
    if not isinstance(value, (list, tuple)) or not value:
        return ()
    if not all(isinstance(item, str) and item for item in value):
        return ()
    return tuple(dict.fromkeys(value))


def _attributes(claims: Mapping[str, Any], roles_claim: str | None) -> dict[str, str]:
    """Project the caller-describing claims onto identity attributes.

    Only string-valued custom claims cross: structured claims would smuggle
    unverifiable shapes into a domain value object, and the registered claims
    describe the token rather than the caller.
    """
    excluded = _PROTOCOL_CLAIMS | ({roles_claim} if roles_claim else frozenset())
    return {
        name: value
        for name, value in claims.items()
        if name not in excluded and isinstance(value, str)
    }
