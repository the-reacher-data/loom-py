"""The identity wire contract: what survives a broker hop, and what is refused.

A job envelope crosses a process boundary, so the codec treats its input as
untrusted: anything it cannot read as an identity decodes to ``None``, and a
use case declaring ``Caller()`` then fails closed instead of running as a
half-decoded caller.
"""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.identity import Identity
from loom.core.identity.wire import decode_identity, encode_identity

_SUBJECT = "user-1"
_ROLES = ("role_a", "role_b")
_ATTRIBUTES = {"email": "ada@example.com"}
_MECHANISM = "jwt"


def _identity() -> Identity:
    return Identity(
        subject=_SUBJECT,
        roles=_ROLES,
        attributes=_ATTRIBUTES,
        mechanism=_MECHANISM,
    )


def _roundtrip(identity: Identity) -> Identity | None:
    return decode_identity(encode_identity(identity))


# ---------------------------------------------------------------------------
# Round trip
# ---------------------------------------------------------------------------


def test_the_identity_survives_the_round_trip() -> None:
    """Every field the authorization rules read must cross the wire intact."""
    assert _roundtrip(_identity()) == _identity()


def test_the_encoded_form_is_plain_json_types() -> None:
    """The envelope travels through a JSON broker serializer, not pickle."""
    encoded = encode_identity(_identity())
    assert encoded == {
        "subject": _SUBJECT,
        "roles": list(_ROLES),
        "attributes": dict(_ATTRIBUTES),
        "mechanism": _MECHANISM,
    }


def test_an_anonymous_identity_encodes_to_none() -> None:
    """Anonymity is the absence of a caller: nothing to put on the wire."""
    assert encode_identity(Identity(subject="")) is None


def test_no_identity_encodes_to_none() -> None:
    """A dispatch without a caller must not invent an envelope field."""
    assert encode_identity(None) is None


# ---------------------------------------------------------------------------
# Decoding untrusted input
# ---------------------------------------------------------------------------


def test_an_absent_envelope_decodes_to_no_identity() -> None:
    """An envelope predating the contract carries no identity, and says so."""
    assert decode_identity(None) is None


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"subject": ""},
        {"subject": 7},
        {"roles": ["role_a"]},
        "not-a-mapping",
        [],
    ],
    ids=["empty", "blank-subject", "non-string-subject", "no-subject", "string", "list"],
)
def test_an_unreadable_envelope_decodes_to_no_identity(payload: Any) -> None:
    """Fail-closed: a half-understood envelope is not a caller."""
    assert decode_identity(payload) is None


def test_non_string_roles_are_dropped() -> None:
    """A tampered envelope cannot smuggle a non-string role into an allowlist check."""
    identity = decode_identity({"subject": _SUBJECT, "roles": ["role_a", 7, None]})
    assert identity is not None
    assert identity.roles == ("role_a",)


def test_non_string_attributes_are_dropped() -> None:
    """Attributes are string-valued by contract, on the wire as much as in memory."""
    identity = decode_identity({"subject": _SUBJECT, "attributes": {"a": "1", "b": 2}})
    assert identity is not None
    assert dict(identity.attributes) == {"a": "1"}


def test_a_missing_mechanism_decodes_to_an_empty_label() -> None:
    """The mechanism is an audit label, not an authorization input: never mandatory."""
    identity = decode_identity({"subject": _SUBJECT})
    assert identity is not None
    assert identity.mechanism == ""
