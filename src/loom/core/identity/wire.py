"""Serialisation of the caller identity for job envelopes.

A job crosses a process boundary, where no context variable follows it.  The
identity therefore travels inside the envelope, as an explicit part of the wire
contract, encoded with plain JSON types so any broker serializer can carry it.

Decoding treats its input as untrusted: anything that cannot be read as an
identity becomes ``None``, and a use case declaring ``Caller()`` then fails
closed instead of running as a partially decoded caller.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from loom.core.identity.identity import Identity

_SUBJECT = "subject"
_ROLES = "roles"
_ATTRIBUTES = "attributes"
_MECHANISM = "mechanism"


def encode_identity(identity: Identity | None) -> dict[str, Any] | None:
    """Encode *identity* for transport inside a job envelope.

    Args:
        identity: Caller to propagate, or ``None``.

    Returns:
        A JSON-serialisable mapping, or ``None`` when there is no
        authenticated caller to propagate.

    Example::

        envelope = {"payload": payload, "identity": encode_identity(caller)}
    """
    if identity is None or not identity.is_authenticated:
        return None
    return {
        _SUBJECT: identity.subject,
        _ROLES: list(identity.roles),
        _ATTRIBUTES: dict(identity.attributes),
        _MECHANISM: identity.mechanism,
    }


def decode_identity(payload: Any) -> Identity | None:
    """Rebuild the caller from a job envelope, refusing anything unreadable.

    Args:
        payload: Value found under the envelope's identity key.  Any type is
            accepted because the envelope may predate this contract or come
            from a tampered broker message.

    Returns:
        The decoded identity, or ``None`` when the envelope carries none or
        carries something that is not one.
    """
    if not isinstance(payload, Mapping):
        return None
    subject = payload.get(_SUBJECT)
    if not isinstance(subject, str) or not subject:
        return None
    mechanism = payload.get(_MECHANISM)
    return Identity(
        subject=subject,
        roles=_decode_roles(payload.get(_ROLES)),
        attributes=_decode_attributes(payload.get(_ATTRIBUTES)),
        mechanism=mechanism if isinstance(mechanism, str) else "",
    )


def _decode_roles(value: Any) -> tuple[str, ...]:
    """Keep the non-empty string roles, dropping anything else."""
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(item for item in value if isinstance(item, str) and item)


def _decode_attributes(value: Any) -> dict[str, str]:
    """Keep the string-valued attributes, dropping anything else."""
    if not isinstance(value, Mapping):
        return {}
    return {
        name: item
        for name, item in value.items()
        if isinstance(name, str) and isinstance(item, str)
    }
