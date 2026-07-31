"""Identity-bound role resolution for the SQL endpoint (spec §4).

The effective roles of a query are derived from the VERIFIED JWT claims the
authentication middleware left in ``scope["state"]["jwt_claims"]``, never from
the request body. The body may only narrow the resulting set. Resolution is
fail-closed: absent, empty or wrongly typed claims deny the request, and
``default_role`` is never a fallback once a connection binds roles to identity.

Internal module: the resolved values are consumed by
:mod:`loom.rest.fastapi.sql` and are not part of the public API.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, NoReturn

from loom.core.sql.abc import RoleNotAllowedError, RolesNotBoundError

_logger = logging.getLogger(__name__)

_STATE_KEY = "state"
_CLAIMS_KEY = "jwt_claims"
_SUBJECT_CLAIM = "sub"


@dataclass(frozen=True)
class RequestIdentity:
    """Authenticated caller of one SQL request.

    Attributes:
        subject: Value of the ``sub`` claim, or ``None`` without verified
            claims (only reachable when the connection binds no claim).
        roles: Roles this request may use, already narrowed by the body.
    """

    subject: str | None
    roles: tuple[str, ...]


def resolve_identity(
    scope: Mapping[str, Any],
    *,
    connection: str,
    roles_claim: str | None,
    allowed_roles: frozenset[str],
    requested_roles: Sequence[str] | None,
) -> RequestIdentity:
    """Resolve the caller identity and the roles their query may use.

    Args:
        scope: ASGI scope of the request, carrying the verified claims.
        connection: Name of the SQL connection being queried.
        roles_claim: Claim binding roles to the identity, or ``None`` when the
            connection declares no binding (single-role endpoint).
        allowed_roles: Connection allowlist, the ceiling of the intersection.
        requested_roles: Roles asked for in the body; they may only narrow.

    Returns:
        The caller subject and the effective roles for this single request.

    Raises:
        RolesNotBoundError: When no allowed role can be derived from the
            verified claims.
        RoleNotAllowedError: When the body asks for a role the identity does
            not hold.
    """
    claims = _verified_claims(scope)
    subject = _subject(claims)
    if roles_claim is None:
        # No binding declared: the connection allowlist (empty by config rule
        # for a mounted endpoint) stays the only barrier in the service.
        return RequestIdentity(subject=subject, roles=tuple(requested_roles or ()))
    authorized = _authorized_roles(
        claims,
        connection=connection,
        roles_claim=roles_claim,
        allowed_roles=allowed_roles,
        subject=subject,
    )
    return RequestIdentity(
        subject=subject,
        roles=_narrow(authorized, requested_roles, connection),
    )


def _verified_claims(scope: Mapping[str, Any]) -> Mapping[str, Any] | None:
    state = scope.get(_STATE_KEY)
    if not isinstance(state, Mapping):
        return None
    claims = state.get(_CLAIMS_KEY)
    return claims if isinstance(claims, Mapping) else None


def _subject(claims: Mapping[str, Any] | None) -> str | None:
    if claims is None:
        return None
    subject = claims.get(_SUBJECT_CLAIM)
    return subject if isinstance(subject, str) else None


def _authorized_roles(
    claims: Mapping[str, Any] | None,
    *,
    connection: str,
    roles_claim: str,
    allowed_roles: frozenset[str],
    subject: str | None,
) -> tuple[str, ...]:
    """Intersect the claimed roles with the allowlist, fail-closed."""
    if claims is None:
        _deny(connection, subject, "the request carries no verified claims")
    claimed = _claimed_roles(claims.get(roles_claim))
    if claimed is None:
        _deny(connection, subject, f"claim {roles_claim!r} is absent, empty or not string-typed")
    authorized = tuple(role for role in claimed if role in allowed_roles)
    if not authorized:
        _deny(connection, subject, f"no role in claim {roles_claim!r} is allowlisted")
    return authorized


def _claimed_roles(value: Any) -> tuple[str, ...] | None:
    """Read the claim as ``str`` or ``list[str]``; anything else is a denial.

    Values are never coerced: a number, a mapping or a list holding anything
    but non-empty strings returns ``None`` so the caller denies the request.
    """
    if isinstance(value, str):
        return (value,) if value else None
    if not isinstance(value, (list, tuple)) or not value:
        return None
    if not all(isinstance(item, str) and item for item in value):
        return None
    return tuple(dict.fromkeys(value))


def _narrow(
    authorized: tuple[str, ...],
    requested: Sequence[str] | None,
    connection: str,
) -> tuple[str, ...]:
    """Apply the body narrowing: a subset of *authorized*, never a widening."""
    if not requested:
        return authorized
    for role in requested:
        if role not in authorized:
            raise RoleNotAllowedError(role, connection=connection)
    return tuple(dict.fromkeys(requested))


def _deny(connection: str, subject: str | None, reason: str) -> NoReturn:
    """Log the audit trail of a denial and refuse the request.

    The response message stays generic on purpose (no oracle about which part
    of the token failed); the precise reason is only recorded server-side.
    """
    _logger.warning(
        "SQL role authorization denied: connection=%s subject=%s reason=%s",
        connection,
        subject,
        reason,
    )
    raise RolesNotBoundError(connection)
