"""Model binding value type shared by the compiler and the engine.

:class:`InferenceTarget` lives in its own module rather than in
``loom.ai.config`` because it is a value type of the pillar: the compiler
embeds it in the plan and the engine consumes it, so it is not a
config-parsing detail.

Secret containment (data-model invariant 4, FR-018): ``credentials_ref`` and
``options`` never leave the process in clear text.  The mechanism is
fail-closed — on construction both values are rewrapped into types msgspec
refuses to encode, so ``msgspec.json.encode`` (and ``msgspec.to_builtins``,
``msgspec.msgpack.encode``) raise ``TypeError`` instead of leaking them, and
``repr``/``str`` redact them.  Rejecting the encode was chosen over silent
redaction because a plan that reaches a wire encoder with a secret reference
aboard is a bug worth surfacing, not smoothing over.  Decoding through the
config loader is unaffected: ``msgspec.convert`` builds the struct from plain
values and ``__post_init__`` wraps them afterwards.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

from msgspec import field, structs

from loom.core.model import LoomFrozenStruct


class _RedactedRef(str):
    """Secret reference that redacts itself and refuses msgspec encoding.

    A ``str`` subclass behaves as the reference everywhere in Python, while
    msgspec rejects encoding non-exact ``str`` types, which is exactly the
    fail-closed behaviour invariant 4 requires.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return "<redacted>"


class _RedactedOptions(Mapping[str, Any]):
    """Options mapping that redacts itself and refuses msgspec encoding.

    msgspec encodes ``dict`` but not arbitrary mappings, so wrapping the
    decoded ``dict`` makes any direct encode of the struct raise instead of
    leaking vendor settings.
    """

    __slots__ = ("_raw",)

    def __init__(self, raw: Mapping[str, Any]) -> None:
        self._raw: dict[str, Any] = dict(raw)

    def __getitem__(self, key: str) -> Any:
        return self._raw[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._raw)

    def __len__(self) -> int:
        return len(self._raw)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, _RedactedOptions):
            return self._raw == other._raw
        if isinstance(other, Mapping):
            return self._raw == dict(other)
        return NotImplemented

    def __repr__(self) -> str:
        return "<redacted>"


class InferenceTarget(LoomFrozenStruct, frozen=True, kw_only=True):
    """One resolved model binding for a model role (``ai.models.<role>``).

    ``repr``/``str`` show ``provider``, ``model``, ``region`` and ``endpoint``
    but never the values of ``credentials_ref`` or ``options`` — the plan
    carries this struct, so an unredacted repr in a start-up traceback is the
    concrete leak path.  Encoding the struct with msgspec raises when either
    secret-bearing field is set (see the module docstring for the rationale).

    Attributes:
        provider: Provider identifier (``bedrock``, ``openai``, ...).
        model: Vendor model id.
        region: Region for regional providers such as Bedrock.
        endpoint: Gateway or compatible endpoint URL.
        credentials_ref: Reference resolved by the existing secrets resolver.
            Never a literal secret (FR-018).
        options: Vendor-specific settings.  Confined here; never reaches the
            artifact.
    """

    provider: str
    model: str
    region: str | None = None
    endpoint: str | None = None
    credentials_ref: str | None = None
    options: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.credentials_ref is not None:
            structs.force_setattr(self, "credentials_ref", _RedactedRef(self.credentials_ref))
        if self.options:
            structs.force_setattr(self, "options", _RedactedOptions(self.options))

    def __repr__(self) -> str:
        credentials = "<redacted>" if self.credentials_ref is not None else None
        options = "<redacted>" if self.options else "{}"
        return (
            f"InferenceTarget(provider={self.provider!r},"
            f" model={self.model!r},"
            f" region={self.region!r},"
            f" endpoint={self.endpoint!r},"
            f" credentials_ref={credentials},"
            f" options={options})"
        )

    __str__ = __repr__
