"""Minting a credential for a verified :class:`Identity`.

The port lives beside the identity it consumes, not beside the transport that
implements it: the caller is a login use case, so putting the abstraction in
``loom.rest`` would make the application layer import infrastructure.  What is
domain here is *"this service needs to mint a credential for this identity"*;
that a JWT comes out is the detail, and it belongs to the implementation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from loom.core.identity.identity import Identity


@dataclass(frozen=True, slots=True)
class IssuedToken:
    """A minted credential together with what the issuer already knew about it.

    Returned instead of a bare string so a login endpoint never has to decode
    the token it just signed: the expiry the HTTP response advertises and the
    identifier the audit trail records are both produced by the signing step.

    Attributes:
        token: The encoded credential, ready to travel as a bearer token.
        expires_at: Instant the credential stops being valid, timezone-aware.
        jti: Unique identifier of this minting, for correlation and audit.
    """

    token: str
    expires_at: datetime
    jti: str


class TokenIssuer(Protocol):
    """Mints a credential that an :class:`Authenticator` can later verify.

    Implementations guarantee the round trip: whatever the identity carries —
    subject, roles and attributes — a matching authenticator recovers intact.
    Everything the credential says comes from the identity, so no caller can
    smuggle a claim past it.

    Example::

        issued = issuer.issue(identity)
        response = {"access_token": issued.token, "expires_at": issued.expires_at}
    """

    def issue(self, identity: Identity, *, ttl: timedelta | None = None) -> IssuedToken:
        """Mint a credential for *identity*.

        Args:
            identity: Verified caller the credential speaks for.
            ttl: Lifetime override. ``None`` uses the configured one, which is
                also the ceiling: a longer lifetime is refused.

        Returns:
            The minted credential and its metadata.

        Raises:
            ValueError: If the identity cannot be represented — anonymous, or
                carrying an attribute that would be unreadable once encoded —
                or if *ttl* exceeds the configured lifetime.
            RuntimeError: If the credential cannot be produced. Implementations
                keep the cause out of the traceback: it can carry key material.
        """
        ...
